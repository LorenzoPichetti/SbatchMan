from __future__ import annotations

import shutil
from pathlib import Path
from typing import Optional

import paramiko
import platformdirs
import tomllib
import tomli_w
from rich.console import Console

console = Console()

# ---------------------------------------------------------------------------
# Config paths
# ---------------------------------------------------------------------------

CONFIG_DIR  = Path(platformdirs.user_config_dir("sbatchman", "sbatchman"))
CONFIG_FILE = CONFIG_DIR / "remotes-config.toml"

DEFAULT_CONFIG: str = """\
# sbatchman config
#
# transfer_backend: "rsync" (default) or "sftp".
#   rsync is always preferred; sftp is used as a fallback when rsync is not
#   available on PATH, or when explicitly set here / via CLI.
#
# Exclude lists are merged in this order (lowest → highest priority):
#
#   common_excludes          – applied to BOTH fetch and sync
#   fetch_excludes           – applied to fetch only  (global or per-cluster)
#   sync_excludes            – applied to sync only   (global or per-cluster)
#   cluster.excludes         – applied to all ops on that cluster
#   cluster.fetch_excludes   – fetch only on that cluster
#   cluster.sync_excludes    – sync only on that cluster
#   dir_pair.excludes        – applied to that specific directory pair
#
# Each [[clusters]] block describes one remote cluster.
# SSH credentials (host / port / user / key_path) are shared by both
# fetch and sync operations.
#
# [[clusters.fetch_dirs]]  – remote → local  (pull)
# [[clusters.sync_dirs]]   – local  → remote (push)
#
# Example
# -------
# [global]
# local_base        = "~/SbatchMan"
# transfer_backend  = "rsync"
# common_excludes   = [".git", ".venv", "__pycache__", "build"]
# fetch_excludes    = ["src", "configs"]   # never pull source back down
# sync_excludes     = ["results", "data"]  # never push large outputs up
#
# [[clusters]]
# name     = "leonardo"
# host     = "login.leonardo.cineca.it"
# port     = 22
# user     = "alice"
# key_path = "~/.ssh/id_ed25519"
# excludes         = ["scratch"]           # all ops on this cluster
# fetch_excludes   = ["logs"]              # fetch only on this cluster
# sync_excludes    = ["datasets"]          # sync only on this cluster
#
#   [[clusters.fetch_dirs]]
#   remote = "~/app1"
#   local  = "~/results/app1"
#   excludes = ["tmp"]   # merged with all of the above
#
#   [[clusters.sync_dirs]]
#   alias   = "myproject"
#   local   = "~/projects/myproject"
#   remote  = "~/myproject"
#   excludes = ["data", "results"]

[global]
local_base        = "~/SbatchMan"
transfer_backend  = "rsync"

# Excluded from BOTH fetch and sync
common_excludes   = [
  ".git",
  ".venv",
  ".vscode",
  "__pycache__",
  "build",
  "target",
  "bin",
]

# Excluded from fetch only (remote → local pulls)
fetch_excludes    = []

# Excluded from sync only (local → remote pushes)
sync_excludes     = [
  "SbatchMan",
  "results",
  "plots",
]
"""


def ensure_config() -> Path:
    """Create the config directory/file if missing; return the config path."""
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    if not CONFIG_FILE.exists():
        CONFIG_FILE.write_text(DEFAULT_CONFIG)
        console.print(
            f"[yellow]Created default config at[/yellow] {CONFIG_FILE}\n"
            "[yellow]Please edit it before running fetch/sync.[/yellow]"
        )
    return CONFIG_FILE


def load_config() -> dict:
    ensure_config()
    with open(CONFIG_FILE, "rb") as fh:
        return tomllib.load(fh)


def save_config(cfg: dict) -> None:
    from copy import deepcopy
    cfg = deepcopy(cfg)
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    with open(CONFIG_FILE, "wb") as fh:
        tomli_w.dump(cfg, fh)


# ---------------------------------------------------------------------------
# Exclude-list helpers
# ---------------------------------------------------------------------------

def resolve_excludes(
    cfg: dict,
    cluster_def: dict,
    dir_pair: Optional[dict] = None,
    operation: str = "common",
) -> list[str]:
    """
    Return the merged exclude list for a transfer operation.

    Merge order (lowest → highest priority, duplicates removed, order preserved):

    1. global ``common_excludes``          – both fetch and sync
    2. global ``{operation}_excludes``     – fetch or sync only
    3. cluster ``excludes``                – all ops on this cluster
    4. cluster ``{operation}_excludes``    – fetch or sync only on this cluster
    5. dir_pair ``excludes``               – this directory pair only

    Parameters
    ----------
    cfg:
        Full parsed config dict.
    cluster_def:
        The ``[[clusters]]`` block for the current cluster.
    dir_pair:
        The ``[[clusters.fetch_dirs]]`` or ``[[clusters.sync_dirs]]`` block,
        if any.
    operation:
        ``"fetch"`` or ``"sync"``.  Any other value skips the
        operation-specific layers (useful for testing).
    """
    seen: set[str] = set()
    result: list[str] = []

    def _add(items: list[str]) -> None:
        for item in items:
            if item not in seen:
                seen.add(item)
                result.append(item)

    global_cfg = cfg.get("global", {})

    # 1. Shared base
    _add(global_cfg.get("common_excludes", []))

    # 2. Global operation-specific
    if operation in ("fetch", "sync"):
        _add(global_cfg.get(f"{operation}_excludes", []))

    # 3. Cluster-level shared
    _add(cluster_def.get("excludes", []))

    # 4. Cluster-level operation-specific
    if operation in ("fetch", "sync"):
        _add(cluster_def.get(f"{operation}_excludes", []))

    # 5. Directory-pair level
    if dir_pair:
        _add(dir_pair.get("excludes", []))

    return result


# ---------------------------------------------------------------------------
# Backend resolution
# ---------------------------------------------------------------------------

VALID_BACKENDS = ("rsync", "sftp")


def resolve_backend(
    cfg: dict,
    cluster_def: dict,
    cli_override: Optional[str] = None,
) -> str:
    """
    Return the effective transfer backend for a cluster.

    Priority (highest → lowest):
      1. CLI --backend flag
      2. Per-cluster transfer_backend
      3. global.transfer_backend
      4. "rsync" (hard default)

    If the resolved backend is "rsync" but rsync is not on PATH, falls back
    to "sftp" with a warning.
    """
    backend = (
        cli_override
        or cluster_def.get("transfer_backend")
        or cfg.get("global", {}).get("transfer_backend")
        or "rsync"
    ).lower()

    if backend not in VALID_BACKENDS:
        console.print(
            f"[yellow]Unknown transfer_backend '{backend}', using 'rsync'.[/yellow]"
        )
        backend = "rsync"

    if backend == "rsync" and not shutil.which("rsync"):
        console.print(
            "[yellow]rsync not found on PATH – falling back to sftp.[/yellow]"
        )
        backend = "sftp"

    return backend


# ---------------------------------------------------------------------------
# SSH connection helpers
# ---------------------------------------------------------------------------

def _interactive_handler(host: str, user: str):
    import getpass

    def handler(title: str, instructions: str, prompt_list: list) -> list[str]:
        if title:
            console.print(f"[bold]{title}[/bold]")
        if instructions:
            console.print(instructions)
        answers: list[str] = []
        for prompt_text, echo in prompt_list:
            if echo:
                answers.append(input(prompt_text))
            else:
                answers.append(getpass.getpass(prompt_text))
        return answers

    return handler


def _key_candidates(key_path: Optional[str]) -> list[Path]:
    candidates: list[Path] = []

    if key_path:
        p = Path(key_path).expanduser()
        if p.exists():
            candidates.append(p)
        else:
            console.print(f"[yellow]Key not found: {p}[/yellow]")

    if not candidates:
        defaults = [
            "~/.ssh/id_ed25519",
            "~/.ssh/id_ecdsa",
            "~/.ssh/id_rsa",
            "~/.ssh/id_dsa",
        ]
        for d in defaults:
            p = Path(d).expanduser()
            if p.exists():
                candidates.append(p)

    return candidates


def _load_pkey(path: Path, passphrase: str) -> paramiko.PKey:
    for cls in (
        paramiko.Ed25519Key,
        paramiko.ECDSAKey,
        paramiko.RSAKey,
        paramiko.DSSKey,
    ):
        try:
            return cls.from_private_key_file(str(path), password=passphrase)
        except paramiko.SSHException:
            continue
    raise paramiko.SSHException(f"Could not load private key: {path}")


def build_ssh_client(
    host: str,
    port: int,
    user: str,
    key_path: Optional[str],
) -> paramiko.SSHClient:
    """
    Connect using the best available auth method:
      1. Public-key (explicit path, then ~/.ssh/id_* defaults)
      2. Keyboard-interactive (OTP / 2FA / challenge-response)
      3. Plain password fallback

    Raises ``paramiko.AuthenticationException`` if all methods fail.
    """
    import getpass

    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    base_kwargs: dict = dict(hostname=host, port=port, username=user)

    # 1. Key-based auth
    for key_file in _key_candidates(key_path):
        try:
            client.connect(
                **base_kwargs,
                key_filename=str(key_file),
                look_for_keys=False,
                allow_agent=True,
            )
            console.print(f"[dim]Authenticated with key: {key_file}[/dim]")
            return client
        except paramiko.PasswordRequiredException:
            passphrase = getpass.getpass(f"Passphrase for {key_file}: ")
            try:
                pkey = _load_pkey(key_file, passphrase)
                client.connect(
                    **base_kwargs,
                    pkey=pkey,
                    look_for_keys=False,
                    allow_agent=False,
                )
                console.print(f"[dim]Authenticated with key: {key_file}[/dim]")
                return client
            except paramiko.AuthenticationException:
                console.print(
                    f"[yellow]Passphrase incorrect for {key_file}, skipping.[/yellow]"
                )
        except paramiko.AuthenticationException:
            pass
        except Exception as exc:
            console.print(f"[yellow]Key {key_file} error: {exc}[/yellow]")

    # 2. Keyboard-interactive
    console.print(
        f"[yellow]No key accepted by {host}. "
        "Trying keyboard-interactive auth…[/yellow]"
    )
    try:
        client.connect(
            **base_kwargs,
            look_for_keys=False,
            allow_agent=False,
            auth_strategy=None,
        )
        return client
    except paramiko.BadAuthenticationType:
        pass
    except paramiko.AuthenticationException:
        pass

    try:
        transport = paramiko.Transport((host, port))
        transport.connect()
        transport.auth_interactive(user, _interactive_handler(host, user))
        if transport.is_authenticated():
            client._transport = transport  # noqa: SLF001
            return client
    except paramiko.AuthenticationException:
        pass
    except Exception as exc:
        console.print(f"[yellow]Keyboard-interactive error: {exc}[/yellow]")

    # 3. Password fallback
    console.print(
        f"[yellow]Keyboard-interactive failed. "
        f"Falling back to password auth for {user}@{host}.[/yellow]"
    )
    password = getpass.getpass(f"Password for {user}@{host}: ")
    client.connect(
        **base_kwargs,
        password=password,
        look_for_keys=False,
        allow_agent=False,
    )
    return client