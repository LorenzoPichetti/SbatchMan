# `sbatchman fetch` — pull files from remote clusters

The `fetch` command copies files **from a remote cluster to your local machine**.
It reads every `[[clusters.fetch_dirs]]` block in the config and, for each pair,
mirrors the remote directory into its local destination, skipping files that are
already up-to-date.

---

## Quick start

```bash
# Pull all clusters, all fetch_dirs
sbatchman fetch

# Pull only one cluster
sbatchman fetch -c cluster-a

# Dry-run to see what would be transferred (rsync only)
sbatchman fetch --dry-run

# Use SFTP instead of rsync (e.g. no rsync on the login node)
sbatchman fetch -b sftp
```

---

## How it works

1. sbatchman loads `~/.config/sbatchman/remotes-config.toml`.
2. For each matching cluster it reads the `[[clusters.fetch_dirs]]` blocks.
3. Each block describes one `remote → local` pair.
4. Files are copied **only when the remote version is newer** than the local one
   (`--update` / mtime comparison), so repeated runs are fast.
5. Progress is streamed to the terminal in real time.

### Backends

| Backend | Requires | Notes |
|---------|----------|-------|
| `rsync` | `rsync` on your local PATH | Default. Streams output; supports `--dry-run`. |
| `sftp`  | SSH access only | Fallback when rsync is unavailable. No native dry-run. |

If `rsync` is selected but not found on PATH, sbatchman falls back to `sftp`
automatically.

---

## Config reference

```toml
[global]
transfer_backend = "rsync"   # default backend for all clusters

# Excluded from BOTH fetch and sync
common_excludes = [".git", ".venv", "__pycache__", "build"]

# Excluded from fetch only
fetch_excludes  = ["results", "data", "datasets", "plots"]

[[clusters]]
name     = "cluster-a"
host     = "login.cluster-a.com"
port     = 22
user     = "alice"
key_path = "~/.ssh/id_ed25519"

# Excluded from all operations on this cluster
excludes       = ["scratch"]

# Excluded from fetch only on this cluster
fetch_excludes = ["logs"]

  [[clusters.fetch_dirs]]
  remote   = "~/myproject"          # path on the cluster
  local    = "~/results/myproject"  # where to put it locally
  excludes = ["tmp"]                # excluded for this pair only
```

> **Tip:** `remote` paths starting with `~/` are resolved on the remote shell,
> so they respect the user's actual home directory even on non-standard systems.

---

## Exclude precedence

Excludes are merged in this order (later entries win on duplicates):

```
global.common_excludes
  → global.fetch_excludes
    → cluster.excludes
      → cluster.fetch_excludes
        → fetch_dir.excludes
```

This lets you keep a shared blacklist (`common_excludes`) for things you never
want to transfer in either direction, while `fetch_excludes` lets you prevent
large output directories from being pulled back down.

---

## CLI reference

```
sbatchman fetch [OPTIONS]

Options:
  -c, --clusters TEXT   Cluster name to pull from. Repeatable. Default: all.
  -b, --backend TEXT    rsync (default) or sftp.
  -n, --dry-run         Show what would be transferred (rsync only).
  --help                Show this message and exit.
```

### Examples

```bash
# Pull two specific clusters
sbatchman fetch -c cluster-a -c cluster-b

# Dry-run on a single cluster with sftp
sbatchman fetch -c cluster-a -b sftp --dry-run
```