from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Optional

import paramiko
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

from sbatchman.remote.ssh import (
    CONFIG_FILE,
    console,
    load_config,
    build_ssh_client,
    resolve_backend,
    resolve_excludes,
)

__all__ = ["sync_remotes"]


# ---------------------------------------------------------------------------
# rsync backend – sync (local → remote)
# ---------------------------------------------------------------------------

def _rsync_sync(
    local_dir: Path,
    user: str,
    host: str,
    port: int,
    remote_path: str,
    excludes: list[str],
    dry_run: bool = False,
) -> subprocess.CompletedProcess:
    """
    Push *local_dir* to *user@host:remote_path* using rsync.

    Flags:
      -z   compress
      -v   verbose
      -r   recursive
      -h   human-readable sizes
      --update  skip files newer on destination
    """
    if not remote_path.startswith("/") and not remote_path.startswith("~/"):
        remote_path = f"~/{remote_path}"

    dest = f"{user}@{host}:{remote_path}"
    exclude_flags = [f"--exclude={e}" for e in excludes]

    cmd: list[str] = [
        "rsync",
        "-zvrh",
        "--update",
        "-e", f"ssh -p {port}",
        *exclude_flags,
        "./",
        dest,
    ]

    if dry_run:
        cmd.insert(1, "--dry-run")

    return subprocess.run(
        cmd,
        cwd=str(local_dir),
        check=True,
        text=True,
        capture_output=False,  # let rsync stream directly to terminal
    )


# ---------------------------------------------------------------------------
# SFTP backend – sync (local → remote)
# ---------------------------------------------------------------------------

def _sftp_remote_mtime(sftp: paramiko.SFTPClient, path: str) -> float:
    """Return remote mtime, or -1 if the file does not exist."""
    try:
        return sftp.stat(path).st_mtime or 0.0
    except FileNotFoundError:
        return -1.0


def _sftp_ensure_remote_dir(sftp: paramiko.SFTPClient, path: str) -> None:
    """Create *path* (and parents) on the remote if they do not exist."""
    parts = path.replace("\\", "/").split("/")
    current = ""
    for part in parts:
        if not part:
            current = "/"
            continue
        current = f"{current}/{part}" if current and current != "/" else f"/{part}" if current == "/" else part
        try:
            sftp.stat(current)
        except FileNotFoundError:
            try:
                sftp.mkdir(current)
            except OSError:
                pass  # may already exist due to a race or relative-path quirk


def _sftp_sync_tree(
    sftp: paramiko.SFTPClient,
    local_path: Path,
    remote_path: str,
    excludes: set[str],
    progress: Progress,
    task_id,
) -> tuple[int, int]:
    """
    Recursively upload *local_path* → *remote_path* via SFTP, additively.

    Skips entries whose name matches *excludes*.
    Skips files whose remote mtime is >= local mtime.
    Returns (files_updated, files_skipped).
    """
    updated = skipped = 0
    _sftp_ensure_remote_dir(sftp, remote_path)

    for child in local_path.iterdir():
        if child.name in excludes:
            skipped += 1
            continue

        r_child = f"{remote_path}/{child.name}"

        if child.is_dir():
            u, s = _sftp_sync_tree(
                sftp, child, r_child, excludes, progress, task_id
            )
            updated += u
            skipped += s
        else:
            local_mtime = child.stat().st_mtime
            remote_mtime = _sftp_remote_mtime(sftp, r_child)
            if remote_mtime >= local_mtime:
                skipped += 1
                continue
            sftp.put(str(child), r_child)
            # Mirror local mtime on the remote so subsequent runs skip unchanged files
            try:
                sftp.utime(r_child, (local_mtime, local_mtime))
            except Exception:
                pass  # some servers do not support utime; skip silently
            updated += 1
            progress.advance(task_id)

    return updated, skipped


# ---------------------------------------------------------------------------
# Per-cluster sync dispatcher
# ---------------------------------------------------------------------------

def _sync_cluster(
    cdef: dict,
    cfg: dict,
    backend: str,
    alias_filter: Optional[set[str]],
    extra_excludes: Optional[list[str]],
    dry_run: bool,
) -> None:
    name     = cdef.get("name", cdef.get("host", "unknown"))
    host     = cdef["host"]
    port     = int(cdef.get("port", 22))
    user     = cdef["user"]
    key_path = cdef.get("key_path")
    sync_pairs: list[dict] = cdef.get("sync_dirs", [])

    if not sync_pairs:
        console.print(f"[yellow]  {name}: no sync_dirs entries, skipping.[/yellow]")
        return

    if alias_filter is not None:
        sync_pairs = [p for p in sync_pairs if p.get("alias") in alias_filter]
        if not sync_pairs:
            console.print(
                f"[yellow]  {name}: no matching aliases, skipping.[/yellow]"
            )
            return

    console.rule(f"[bold cyan]{name}[/bold cyan]")

    # For sftp we need a persistent connection; rsync handles its own SSH.
    sftp: Optional[paramiko.SFTPClient] = None
    ssh: Optional[paramiko.SSHClient]  = None

    if backend == "sftp":
        try:
            ssh  = build_ssh_client(host, port, user, key_path)
            sftp = ssh.open_sftp()
        except Exception as exc:
            console.print(f"[red]  ✗ {name}: cannot connect – {exc}[/red]")
            return

    try:
        for pair in sync_pairs:
            alias     = pair.get("alias", "?")
            local_dir = pair.get("local", "").strip()
            remote    = pair.get("remote", "").strip()

            if not local_dir or not remote:
                console.print(
                    f"[yellow]  {name}/{alias}: incomplete sync_dir pair, skipping.[/yellow]"
                )
                continue

            local_path = Path(local_dir).expanduser().resolve()
            if not local_path.is_dir():
                console.print(
                    f"[yellow]  {name}/{alias}: local path not found: {local_path}[/yellow]"
                )
                continue

            # Build final exclude list: global + cluster + pair + CLI extras
            pair_with_extra = dict(pair)
            if extra_excludes:
                pair_with_extra["excludes"] = list(
                    pair.get("excludes", [])
                ) + extra_excludes
            excludes = resolve_excludes(cfg, cdef, pair_with_extra)

            label = (
                f"[cyan]{alias}[/cyan]  {local_path} → {user}@{host}:{remote}  "
                f"[dim]({backend})[/dim]"
            )
            if dry_run:
                label = "[dim](dry-run)[/dim] " + label
            console.print(f"  ↑ {label}")

            if backend == "rsync":
                try:
                    _rsync_sync(
                        local_dir=local_path,
                        user=user,
                        host=host,
                        port=port,
                        remote_path=remote,
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

            else:  # sftp
                # Expand ~ on the remote side via the shell
                effective_remote = remote
                if effective_remote.startswith("~/") and ssh is not None:
                    _, stdout, _ = ssh.exec_command(f"echo {effective_remote}")
                    effective_remote = stdout.read().decode().strip() or effective_remote

                with Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    transient=False,
                ) as progress:
                    task_id = progress.add_task(
                        f"[cyan]Uploading {alias}…", total=None
                    )
                    try:
                        u, s = _sftp_sync_tree(
                            sftp=sftp,
                            local_path=local_path,
                            remote_path=effective_remote,
                            excludes=set(excludes),
                            progress=progress,
                            task_id=task_id,
                        )
                        progress.update(
                            task_id,
                            description=(
                                f"[green]✓ {alias}[/green] — "
                                f"{u} uploaded, {s} skipped"
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

def sync_remotes(
    clusters: Optional[list[str]] = None,
    aliases: Optional[list[str]] = None,
    backend: Optional[str] = None,
    extra_excludes: Optional[list[str]] = None,
    dry_run: bool = False,
) -> None:
    """
    Push local sync_dirs to their configured remote destinations.

    Parameters
    ----------
    clusters:
        Names of clusters to target.  ``None`` → all clusters.
    aliases:
        Limit to sync_dirs whose ``alias`` field matches one of these values.
        ``None`` → all sync_dirs on the selected clusters.
    backend:
        ``"rsync"`` or ``"sftp"``.  Overrides config when supplied (CLI flag).
    extra_excludes:
        Additional names to exclude, appended after the merged config excludes.
    dry_run:
        Pass ``--dry-run`` to rsync (rsync backend only).
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
        _sync_cluster(
            cdef=cdef,
            cfg=cfg,
            backend=effective_backend,
            alias_filter=alias_filter,
            extra_excludes=extra_excludes,
            dry_run=dry_run,
        )