"""
Headless figure export CLI, for CI or a paper's build script:

    python -m utils.cli export plots.json --out figures/ --format png pdf
    python -m utils.cli export plots.json --db results.sqlite --style paper_double_column

Regenerates every tab in a workspace JSON (the same file produced by the
app's "Export workspace" button, or a plots.json auto-load file) without
opening a browser. See export.py for the rendering side of this.
"""
import argparse
import json
import sys
from pathlib import Path


def _load_workspace(path: Path) -> dict:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        sys.exit(f"error: workspace file not found: {path}")
    except json.JSONDecodeError as e:
        sys.exit(f"error: {path} is not valid JSON ({e})")
    if "tabs" not in data:
        sys.exit(f"error: {path} doesn't look like a workspace file (no 'tabs' key)")
    return data


def _load_style(preset_or_path: str | None) -> dict:
    """Accepts a preset name (see styles.PRESETS) or a path to a style JSON
    file (as produced by the app's Style tab -> Export style)."""
    if not preset_or_path:
        return {}
    from .styles import PRESETS
    if preset_or_path in PRESETS:
        return dict(PRESETS[preset_or_path])
    p = Path(preset_or_path)
    if p.exists():
        return json.loads(p.read_text(encoding="utf-8"))
    sys.exit(f"error: '{preset_or_path}' is neither a known style preset "
              f"({', '.join(PRESETS)}) nor an existing file")


def cmd_export(args):
    from . import db
    from .export import export_workspace

    workspace = _load_workspace(Path(args.workspace))
    style = _load_style(args.style)

    dbs = args.db or []
    if not dbs:
        # No --db given: look for sibling .sqlite files next to the workspace,
        # since plots.json is normally kept alongside the database it plots.
        dbs = sorted(Path(args.workspace).parent.glob("*.sqlite")) + \
              sorted(Path(args.workspace).parent.glob("*.db"))
        if dbs:
            print(f"No --db given; auto-discovered: {', '.join(str(d) for d in dbs)}", file=sys.stderr)
    if not dbs:
        sys.exit("error: no database given (--db) and none found next to the workspace file")
    db.load_databases(dbs)

    if args.plugins:
        from . import plots
        plots.load_plugins(args.plugins)

    result = export_workspace(workspace, out_dir=args.out, formats=args.format, style=style)

    for f in result["written"]:
        print(f"wrote {f}")
    for e in result["errors"]:
        label = f"{e['tab']}" + (f" [{e['format']}]" if "format" in e else "")
        print(f"FAILED {label}: {e['error']}", file=sys.stderr)

    if result["errors"] and not result["written"]:
        sys.exit(1)  # total failure: non-zero exit for CI
    elif result["errors"]:
        sys.exit(2)  # partial failure: distinct code so CI can choose to tolerate it


def build_parser():
    p = argparse.ArgumentParser(prog="python -m utils.cli", description="Headless plot export.")
    sub = p.add_subparsers(dest="command", required=True)

    exp = sub.add_parser("export", help="Render every tab of a workspace JSON to image files.")
    exp.add_argument("workspace", help="Path to a workspace JSON (plots.json or an exported workspace file).")
    exp.add_argument("--db", action="append", metavar="PATH",
                     help="SQLite database to load (repeatable). Auto-discovered next to the workspace if omitted.")
    exp.add_argument("--out", default="figures", metavar="DIR", help="Output directory (default: figures/).")
    exp.add_argument("--format", nargs="+", default=["png"], choices=["png", "svg", "pdf"],
                     help="One or more output formats (default: png).")
    exp.add_argument("--style", metavar="PRESET_OR_PATH",
                     help="A style preset name (screen, paper_single_column, paper_double_column, slides) "
                          "or a path to an exported style JSON. Per-tab styles in the workspace still override this.")
    exp.add_argument("--plugins", nargs="+", metavar="DIR", help="Plugin directories to load before rendering.")
    exp.set_defaults(func=cmd_export)
    return p


def main(argv=None):
    args = build_parser().parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
