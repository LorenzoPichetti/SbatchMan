# Sync — push files to remote clusters

The `sync` command copies files **from your local machine to a remote cluster**.
It reads every `[[clusters.sync_dirs]]` block in the config and, for each pair,
uploads the local directory to the remote destination, skipping files that are
already up-to-date on the cluster.

!!! tip
    Use the `sbatchman remotes-config` TUI to manage you configurations.

---

## CLI Usage

```bash
# Push all clusters, all sync_dirs
sbatchman sync

# Push only one cluster
sbatchman sync -c cluster-a

# Push a specific alias on all clusters
sbatchman sync -a myproject

# Dry-run to see what would be uploaded (rsync only)
sbatchman sync --dry-run

# Exclude extra directories on top of the config
sbatchman sync -e checkpoints -e wandb

# Use SFTP instead of rsync
sbatchman sync -b sftp
```

---

## How it works

1. sbatchman loads `~/.config/sbatchman/remotes-config.toml`.
2. For each matching cluster it reads the `[[clusters.sync_dirs]]` blocks.
3. Each block describes one `local → remote` pair, identified by an `alias`.
4. Files are uploaded **only when the local version is newer** than the remote
   one (`--update` / mtime comparison), so repeated runs are fast.
5. Remote directories are created automatically if they do not exist.
6. Progress is streamed to the terminal in real time.

| Backend | Requires | Notes |
|---------|----------|-------|
| `rsync` | `rsync` on your local PATH | Default. Streams output; supports `--dry-run`. |
| `sftp`  | SSH access only | Fallback when rsync is unavailable. No native dry-run. |

If `rsync` is selected but not found on PATH, sbatchman falls back to `sftp`
automatically.

---

## Config reference

You can manually edit the config file or use the TUI (`sbatchman remotes-config`).

```toml
[global]
transfer_backend = "rsync"   # default backend for all clusters

# Excluded from BOTH fetch and sync
common_excludes = [".git", ".venv", "__pycache__", "build"]

# Excluded from sync only
sync_excludes   = ["results", "data", "datasets"]

[[clusters]]
name     = "cluster-a"
host     = "login.cluster-a.cineca.it"
port     = 22
user     = "alice"
key_path = "~/.ssh/id_ed25519"

# Excluded from all operations on this cluster
excludes      = ["scratch"]

# Excluded from sync only on this cluster
sync_excludes = ["checkpoints"]

[[clusters.sync_dirs]]
alias    = "myproject"              # short name used with -a / --aliases
local    = "~/projects/myproject"   # local source directory
remote   = "~/myproject"            # destination on the cluster
excludes = ["data", "results"]      # excluded for this pair only
```

> **Tip:** An `alias` is required for each `sync_dirs` entry. It lets you push
> a single project to multiple clusters at once, or target just one project on
> a specific cluster.

---

## Exclude precedence

Excludes are merged in this order (later entries win on duplicates):

```
global.common_excludes
  → global.sync_excludes
    → cluster.excludes
      → cluster.sync_excludes
        → sync_dir.excludes
          → --exclude CLI flags
```

This lets you keep a shared blacklist (`common_excludes`) for things you never
want to transfer in either direction, while `sync_excludes` prevents you from
accidentally pushing large data or result directories up to the cluster.

---

## CLI reference

```
sbatchman sync [OPTIONS]

Options:
  -c, --clusters TEXT    Cluster name to push to. Repeatable. Default: all.
  -a, --aliases TEXT     sync_dir alias to push. Repeatable. Default: all.
  -b, --backend TEXT     rsync (default) or sftp.
  -e, --exclude TEXT     Extra name to exclude. Repeatable.
  -n, --dry-run          Show what would be uploaded (rsync only).
  --help                 Show this message and exit.
```

### Examples

```bash
# Push two aliases to one cluster
sbatchman sync -c cluster-a -a myproject -a experiments

# Push everything, exclude two extra dirs at runtime
sbatchman sync -e wandb -e checkpoints

# Dry-run push to all clusters
sbatchman sync --dry-run
```