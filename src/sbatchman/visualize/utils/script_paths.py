"""Global import paths available to Python scripts in the visualize UI."""
import json
import os
import sys
from pathlib import Path

import platformdirs

CONFIG_DIR = Path(platformdirs.user_config_dir("sbatchman", "sbatchman"))
SCRIPT_PATHS_FILE = CONFIG_DIR / "visualize-script-paths.json"
_MANAGED_SCRIPT_PATHS = set()


def _clean_paths(paths):
    if not isinstance(paths, list):
        raise ValueError("Script paths must be provided as a list.")
    cleaned = []
    seen = set()
    for raw in paths:
        if not isinstance(raw, str):
            continue
        raw = raw.strip()
        if not raw:
            continue
        expanded = os.path.expandvars(os.path.expanduser(raw))
        normalized = str(Path(expanded).resolve())
        if normalized not in seen:
            cleaned.append(normalized)
            seen.add(normalized)
    return cleaned


def get_script_paths():
    if not SCRIPT_PATHS_FILE.exists():
        return []
    try:
        config = json.loads(SCRIPT_PATHS_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"Could not read script paths config {SCRIPT_PATHS_FILE}: {exc}") from exc
    if not isinstance(config, dict):
        raise ValueError(f"Script paths config {SCRIPT_PATHS_FILE} must contain a JSON object.")
    return _clean_paths(config.get("paths", []))


def set_script_paths(paths):
    cleaned = _clean_paths(paths)
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    SCRIPT_PATHS_FILE.write_text(
        json.dumps({"version": 1, "paths": cleaned}, indent=2) + "\n",
        encoding="utf-8",
    )
    return cleaned


def merge_script_paths(paths):
    """Append paths not already in the user's global script-path config."""
    return set_script_paths(get_script_paths() + _clean_paths(paths))


def add_script_paths_to_sys_path():
    """Make configured directories importable by user-authored scripts."""
    global _MANAGED_SCRIPT_PATHS
    paths = get_script_paths()
    wanted = set(paths)
    for path in _MANAGED_SCRIPT_PATHS - wanted:
        try:
            sys.path.remove(path)
        except ValueError:
            pass
    existing = {str(Path(p or ".").resolve()) for p in sys.path}
    for path in paths:
        if path not in existing:
            sys.path.append(path)
            existing.add(path)
            _MANAGED_SCRIPT_PATHS.add(path)
    _MANAGED_SCRIPT_PATHS.intersection_update(wanted)
