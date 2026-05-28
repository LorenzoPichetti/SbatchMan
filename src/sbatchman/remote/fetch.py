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
    dry_run: bool,
    progress: Progress,
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

    task = progress.add_task(f"[cyan]Connecting to {name}…", total=None)

    # rsync does not need a persistent SSH connection
    if backend == "rsync":
        progress.update(task, description=f"[cyan]Fetching from {name} (rsync)…")
        total_updated = total_skipped = 0

        for pair in dir_pairs:
            r_dir = pair.get("remote", "").strip()
            l_dir = pair.get("local",  "").strip()
            if not r_dir or not l_dir:
                console.print(
                    f"[yellow]  {name}: skipping incomplete dir pair {pair}[/yellow]"
                )
                continue

            excludes = resolve_excludes(cfg, cdef, pair, operation="fetch")
            l_target = Path(l_dir).expanduser()

            progress.update(
                task,
                description=f"[cyan]rsync fetch {name}:{r_dir} → {l_target}…",
            )

            if dry_run:
                console.print(
                    f"  [dim](dry-run)[/dim] rsync {user}@{host}:{r_dir}/ → {l_target}/"
                )

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
                # rsync streams its own output; we count pairs not files
                total_updated += 1
            except subprocess.CalledProcessError as exc:
                console.print(
                    f"  [red]✗ {name}/{r_dir}: rsync exited {exc.returncode}[/red]"
                )

        progress.update(
            task,
            description=f"[green]✓ {name}[/green] — {total_updated} pair(s) synced",
            completed=1,
            total=1,
        )
        return

    # sftp path
    try:
        ssh  = build_ssh_client(host, port, user, key_path)
        sftp = ssh.open_sftp()
    except Exception as exc:
        progress.update(task, description=f"[red]✗ {name}: {exc}[/red]")
        return

    total_updated = total_skipped = 0

    for pair in dir_pairs:
        r_dir = pair.get("remote", "").strip()
        l_dir = pair.get("local",  "").strip()
        if not r_dir or not l_dir:
            console.print(
                f"[yellow]  {name}: skipping incomplete dir pair {pair}[/yellow]"
            )
            continue

        # Expand ~ on the remote side via the shell
        if r_dir.startswith("~/"):
            _, stdout, _ = ssh.exec_command(f"echo {r_dir}")
            r_dir = stdout.read().decode().strip() or r_dir

        if not _sftp_is_dir(sftp, r_dir):
            console.print(
                f"[yellow]  {name}: remote path not found: {r_dir}[/yellow]"
            )
            continue

        excludes = set(resolve_excludes(cfg, cdef, pair, operation="fetch"))
        l_target = Path(l_dir).expanduser()

        progress.update(
            task,
            description=f"[cyan]sftp fetch {name}:{r_dir} → {l_target}…",
        )

        u, s = _sftp_fetch_tree(sftp, r_dir, l_target, excludes, progress, task)
        total_updated += u
        total_skipped += s

    sftp.close()
    ssh.close()

    progress.update(
        task,
        description=(
            f"[green]✓ {name}[/green] — "
            f"{total_updated} updated, {total_skipped} skipped"
        ),
        completed=1,
        total=1,
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_remotes(
    clusters: Optional[list[str]] = None,
    backend: Optional[str] = None,
    dry_run: bool = False,
) -> None:
    """
    Pull remote directories into their configured local destinations.

    Parameters
    ----------
    clusters:
        If given, only those cluster names are processed; otherwise all
        clusters defined in the config file are used.
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

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        transient=False,
    ) as progress:
        for cdef in cluster_configs:
            effective_backend = resolve_backend(cfg, cdef, cli_override=backend)
            _fetch_cluster(cdef, cfg, effective_backend, dry_run, progress)