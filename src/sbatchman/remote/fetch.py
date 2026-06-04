from __future__ import annotations

import os
import stat
import subprocess
from pathlib import Path
from typing import Optional

import paramiko
from rich.progress import Progress, SpinnerColumn, TextColumn

from sbatchman.remote.ssh import (
    CONFIG_DIR,
    CONFIG_FILE,
    console,
    ensure_config,
    load_config,
    build_ssh_client,
    resolve_backend,
    resolve_excludes,
)

__all__ = [
    "CONFIG_DIR",
    "CONFIG_FILE",
    "ensure_config",
    "load_config",
    "fetch_remotes",
]


# ---------------------------------------------------------------------------
# rsync backend – fetch (remote → local)
# ---------------------------------------------------------------------------

def _rsync_fetch(
    local_dir: Path,
    user: str,
    host: str,
    port: int,
    remote_path: str,
    excludes: list[str],
    dry_run: bool = False,
) -> subprocess.CompletedProcess:
    """
    Pull *user@host:remote_path/* into *local_dir* using rsync.

    Flags:
      -z   compress
      -v   verbose
      -r   recursive
      -h   human-readable sizes
      --update  skip files newer on destination
    """
    if not remote_path.startswith("/") and not remote_path.startswith("~/"):
        remote_path = f"~/{remote_path}"

    # Ensure trailing slash on source so rsync merges into local_dir
    source = f"{user}@{host}:{remote_path}/"
    exclude_flags = [f"--exclude={e}" for e in excludes]

    cmd: list[str] = [
        "rsync",
        "-zvrh",
        "--update",
        "-e", f"ssh -p {port}",
        *exclude_flags,
        source,
        str(local_dir) + "/",
    ]

    if dry_run:
        cmd.insert(1, "--dry-run")

    local_dir.mkdir(parents=True, exist_ok=True)

    return subprocess.run(
        cmd,
        check=True,
        text=True,
        capture_output=False,  # stream rsync output directly to terminal
    )


# ---------------------------------------------------------------------------
# SFTP backend – fetch (remote → local)
# ---------------------------------------------------------------------------

def _sftp_is_dir(sftp: paramiko.SFTPClient, path: str) -> bool:
    try:
        return stat.S_ISDIR(sftp.stat(path).st_mode)
    except FileNotFoundError:
        return False


def _sftp_fetch_tree(
    sftp: paramiko.SFTPClient,
    remote_path: str,
    local_path: Path,
    excludes: set[str],
    progress: Progress,
    task_id,
) -> tuple[int, int]:
    """
    Recursively copy *remote_path* → *local_path* via SFTP, additively.

    Skips entries whose name matches *excludes*.
    Skips files whose local mtime is >= remote mtime.
    Returns (files_updated, files_skipped).
    """
    updated = skipped = 0
    local_path.mkdir(parents=True, exist_ok=True)

    for entry in sftp.listdir_attr(remote_path):
        if entry.filename in excludes:
            skipped += 1
            continue

        r_child = f"{remote_path}/{entry.filename}"
        l_child = local_path / entry.filename

        if stat.S_ISDIR(entry.st_mode):
            u, s = _sftp_fetch_tree(
                sftp, r_child, l_child, excludes, progress, task_id
            )
            updated += u
            skipped += s
        else:
            remote_mtime = entry.st_mtime or 0
            if l_child.exists() and l_child.stat().st_mtime >= remote_mtime:
                skipped += 1
                continue
            sftp.get(r_child, str(l_child))
            os.utime(l_child, (remote_mtime, remote_mtime))
            updated += 1
            progress.advance(task_id)

    return updated, skipped


# ---------------------------------------------------------------------------
# Per-cluster fetch dispatcher
# ---------------------------------------------------------------------------

def _fetch_cluster(
    cdef: dict,
    cfg: dict,
    backend: str,
    alias_filter: Optional[set[str]],
    dry_run: bool,
) -> None:
    name     = cdef.get("name", cdef.get("host", "unknown"))
    host     = cdef["host"]
    port     = int(cdef.get("port", 22))
    user     = cdef["user"]
    key_path = cdef.get("key_path")
    # Support legacy "dirs" key alongside "fetch_dirs"
    dir_pairs: list[dict] = cdef.get("fetch_dirs", cdef.get("dirs", []))

    if not dir_pairs:
        console.print(f"[yellow]  {name}: no fetch_dirs entries, skipping.[/yellow]")
        return

    if alias_filter is not None:
        dir_pairs = [p for p in dir_pairs if p.get("alias") in alias_filter]
        if not dir_pairs:
            console.print(
                f"[yellow]  {name}: no matching aliases, skipping.[/yellow]"
            )
            return

    console.rule(f"[bold cyan]{name}[/bold cyan]")

    # rsync does not need a persistent SSH connection
    if backend == "rsync":
        for pair in dir_pairs:
            alias  = pair.get("alias", "?")
            r_dir  = pair.get("remote", "").strip()
            l_dir  = pair.get("local",  "").strip()

            if not r_dir or not l_dir:
                console.print(
                    f"[yellow]  {name}/{alias}: incomplete fetch_dir pair, skipping.[/yellow]"
                )
                continue

            excludes = resolve_excludes(cfg, cdef, pair, operation="fetch")
            l_target = Path(l_dir).expanduser()

            label = (
                f"[cyan]{alias}[/cyan]  {user}@{host}:{r_dir} → {l_target}  "
                f"[dim]({backend})[/dim]"
            )
            if dry_run:
                label = "[dim](dry-run)[/dim] " + label
            console.print(f"  ↓ {label}")

            try:
                _rsync_fetch(
                    local_dir=l_target,
                    user=user,
                    host=host,
                    port=port,
                    remote_path=r_dir,
                    excludes=excludes,
                    dry_run=dry_run,
                )
                console.print(f"  [green]✓ {alias} done[/green]")
            except subprocess.CalledProcessError as exc:
                console.print(
                    f"  [red]✗ {alias} failed (rsync exit {exc.returncode})[/red]"
                )
            except Exception as exc:
                console.print(f"  [red]✗ {alias}: {exc}[/red]")

        return

    # sftp path — needs a persistent SSH connection
    ssh: Optional[paramiko.SSHClient]  = None
    sftp: Optional[paramiko.SFTPClient] = None

    try:
        ssh  = build_ssh_client(host, port, user, key_path)
        sftp = ssh.open_sftp()
    except Exception as exc:
        console.print(f"[red]  ✗ {name}: cannot connect – {exc}[/red]")
        return

    try:
        for pair in dir_pairs:
            alias  = pair.get("alias", "?")
            r_dir  = pair.get("remote", "").strip()
            l_dir  = pair.get("local",  "").strip()

            if not r_dir or not l_dir:
                console.print(
                    f"[yellow]  {name}/{alias}: incomplete fetch_dir pair, skipping.[/yellow]"
                )
                continue

            # Expand ~ on the remote side via the shell
            if r_dir.startswith("~/") and ssh is not None:
                _, stdout, _ = ssh.exec_command(f"echo {r_dir}")
                r_dir = stdout.read().decode().strip() or r_dir

            if not _sftp_is_dir(sftp, r_dir):
                console.print(
                    f"[yellow]  {name}/{alias}: remote path not found: {r_dir}[/yellow]"
                )
                continue

            excludes = set(resolve_excludes(cfg, cdef, pair, operation="fetch"))
            l_target = Path(l_dir).expanduser()

            label = (
                f"[cyan]{alias}[/cyan]  {user}@{host}:{r_dir} → {l_target}  "
                f"[dim]({backend})[/dim]"
            )
            if dry_run:
                label = "[dim](dry-run)[/dim] " + label
            console.print(f"  ↓ {label}")

            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                transient=False,
            ) as progress:
                task_id = progress.add_task(
                    f"[cyan]Downloading {alias}…", total=None
                )
                try:
                    u, s = _sftp_fetch_tree(
                        sftp=sftp,
                        remote_path=r_dir,
                        local_path=l_target,
                        excludes=excludes,
                        progress=progress,
                        task_id=task_id,
                    )
                    progress.update(
                        task_id,
                        description=(
                            f"[green]✓ {alias}[/green] — "
                            f"{u} updated, {s} skipped"
                        ),
                        completed=1,
                        total=1,
                    )
                except Exception as exc:
                    progress.update(
                        task_id,
                        description=f"[red]✗ {alias}: {exc}[/red]",
                        completed=1,
                        total=1,
                    )
    finally:
        if sftp is not None:
            sftp.close()
        if ssh is not None:
            ssh.close()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_remotes(
    clusters: Optional[list[str]] = None,
    aliases: Optional[list[str]] = None,
    backend: Optional[str] = None,
    dry_run: bool = False,
) -> None:
    """
    Pull remote directories into their configured local destinations.

    Parameters
    ----------
    clusters:
        Names of clusters to target.  ``None`` → all clusters.
    aliases:
        Limit to fetch_dirs whose ``alias`` field matches one of these values.
        ``None`` → all fetch_dirs on the selected clusters.
    backend:
        ``"rsync"`` or ``"sftp"``.  Overrides config when supplied (CLI flag).
        Falls back through per-cluster → global → "rsync" hard default.
        If rsync is not on PATH, sftp is used regardless.
    dry_run:
        When True and backend is rsync, passes ``--dry-run`` to rsync.
        For sftp the option is noted but no transfer is skipped (sftp has no
        native dry-run; use rsync for that).

    Excludes applied (in order, lowest → highest priority):
        global.common_excludes → global.fetch_excludes →
        cluster.excludes → cluster.fetch_excludes →
        fetch_dir.excludes
    """
    cfg = load_config()
    cluster_configs: list[dict] = cfg.get("clusters", [])

    if not cluster_configs:
        console.print(
            f"[red]No clusters defined in[/red] {CONFIG_FILE}\n"
            "Edit the file and add at least one [[clusters]] block."
        )
        return

    if clusters:
        requested = set(clusters)
        cluster_configs = [c for c in cluster_configs if c.get("name") in requested]
        if not cluster_configs:
            console.print(
                f"[red]None of the requested clusters ({', '.join(requested)}) "
                "were found in the config.[/red]"
            )
            return

    alias_filter: Optional[set[str]] = set(aliases) if aliases else None

    for cdef in cluster_configs:
        effective_backend = resolve_backend(cfg, cdef, cli_override=backend)
        _fetch_cluster(
            cdef=cdef,
            cfg=cfg,
            backend=effective_backend,
            alias_filter=alias_filter,
            dry_run=dry_run,
        )