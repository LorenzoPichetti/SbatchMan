"""Thin HTTP layer. All logic lives in utils/; this module only routes
requests, serializes JSON, and turns exceptions into error responses."""
import json
import traceback
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import urlparse

from sbatchman.visualize.utils import db, plots, render, styles
from sbatchman.visualize.utils.core import SERVER_LOG, get_initial_workspace, load_initial_workspace, log

MODULE_DIR = Path(__file__).resolve().parent
HTML_PATH = MODULE_DIR / "webapp.html"
DOCS_PATH = MODULE_DIR / "docs.html"
STATIC_DIR = MODULE_DIR / "static"

# db_name -> {"parser": Path, "output_path": Path}, so re-parse can actually
# regenerate the SQLite file rather than just re-read whatever's on disk.
REPARSE_SOURCES: dict = {}
REMOTE_SYSTEMS: dict = {}

CONTENT_TYPES = {".js": "text/javascript", ".css": "text/css", ".map": "application/json"}


def hook_reparse(db_name: str) -> dict:
    """Replace with real ETL. Falls back to just re-reading the file if no
    parser/output_path was registered for this database at startup."""
    src = REPARSE_SOURCES.get(db_name)
    try:
        if src:
            from sbatchman.parser import parse_jobs_and_generate_sqlite_db
            parse_jobs_and_generate_sqlite_db(parser=src["parser"], output_path=src["output_path"])
        counts = db.table_counts(db_name)
        summary = ", ".join(f"{t}={n}" for t, n in counts.items())
        return {"ok": True, "message": f"Re-parsed '{db_name}': {summary}"}
    except Exception as e:
        return {"ok": False, "message": f"Re-parse failed for '{db_name}': {e}"}


def hook_fetch_remote(system: str, paths: list) -> dict:
    """Replace with real scp/rsync/S3 logic."""
    if system not in REMOTE_SYSTEMS:
        return {"ok": False, "message": f"Unknown remote system: {system}", "loaded_dbs": []}
    try:
        from sbatchman.remote.fetch import fetch_remotes
        fetch_remotes([system], paths)
        return {"ok": True, "message": f"Fetched {paths} from {system}!",
                "loaded_dbs": [f"{p} @ {system}" for p in paths]}
    except Exception as e:
        log(f"Error during fetch: {e}", "error")
        return {"ok": False, "message": str(e), "loaded_dbs": []}


def load_remote_systems():
    global REMOTE_SYSTEMS
    try:
        from sbatchman.remote.ssh import load_config
        cfg = load_config()
        systems = {}
        for cluster in cfg.get("clusters", []):
            systems[cluster["name"]] = [d["alias"] for d in cluster.get("fetch_dirs", [])]
        REMOTE_SYSTEMS = systems
    except Exception as e:
        log(f"No remote systems config: {e}", "warn")
        REMOTE_SYSTEMS = {}


def db_status() -> dict:
    return {"databases": db.all_schemas(), "single_db": db.is_single_db(), "default_database": db.only_db_name()}


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass

    # -- response helpers ---------------------------------------------
    def send_json(self, data, status=200):
        body = json.dumps(data).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def send_html(self, html):
        body = html.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def send_static(self, rel_path):
        f = (STATIC_DIR / rel_path).resolve()
        if not str(f).startswith(str(STATIC_DIR.resolve())) or not f.is_file():
            self.send_json({"error": "Not found"}, 404)
            return
        body = f.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", CONTENT_TYPES.get(f.suffix, "application/octet-stream"))
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(body)

    def read_json_body(self):
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length) if length else b""
        return json.loads(raw) if raw else {}

    def guarded_json(self, fn, *args):
        """Run fn(*args), send its dict result as JSON, or turn an exception
        into a 400 with a traceback and matching log entry — every POST
        handler below follows this same shape."""
        try:
            result = fn(*args)
            self.send_json(result)
        except Exception as e:
            entry = log(str(e), "error")
            self.send_json({"error": str(e), "traceback": traceback.format_exc(), "log_entries": [entry]}, 400)

    # -- GET -------------------------------------------------------------
    def do_GET(self):
        path = urlparse(self.path).path
        if path in ("/", "/index.html"):
            try:
                self.send_html(HTML_PATH.read_text(encoding="utf-8"))
            except Exception as e:
                self.send_json({"error": f"Could not load {HTML_PATH}: {e}"}, 500)
        elif path in ("/docs", "/docs.html"):
            try:
                self.send_html(DOCS_PATH.read_text(encoding="utf-8"))
            except Exception as e:
                self.send_json({"error": f"Could not load {DOCS_PATH}: {e}"}, 500)
        elif path.startswith("/static/"):
            self.send_static(path[len("/static/"):])
        elif path == "/api/databases":
            self.send_json(db_status())
        elif path == "/api/plot_types":
            self.send_json({"plot_types": plots.plot_type_catalog()})
        elif path == "/api/remote_systems":
            self.send_json({"systems": REMOTE_SYSTEMS})
        elif path == "/api/logs":
            self.send_json({"logs": SERVER_LOG[-200:]})
        elif path == "/api/initial_workspace":
            self.send_json({"workspace": get_initial_workspace()})
        elif path == "/api/style_presets":
            self.send_json({"presets": styles.PRESETS, "defaults": styles.DEFAULT})
        else:
            self.send_json({"error": "Not found"}, 404)

    # -- POST --------------------------------------------------------------
    def do_POST(self):
        path = urlparse(self.path).path
        try:
            payload = self.read_json_body()
        except Exception:
            self.send_json({"error": "Invalid JSON"}, 400)
            return

        if path == "/api/preview":
            self.guarded_json(render.run_preview, payload)
        elif path == "/api/plot":
            self.guarded_json(render.render_plot, payload)
        elif path == "/api/plot_grid":
            self.guarded_json(render.render_grid, payload)
        elif path == "/api/export":
            self.guarded_json(self._export, payload)
        elif path == "/api/reparse":
            self.guarded_json(self._reparse, payload)
        elif path == "/api/fetch_remote":
            self.guarded_json(self._fetch_remote, payload)
        elif path == "/api/reload_plugins":
            self.guarded_json(self._reload_plugins, payload)
        elif path == "/api/log":
            entry = log(payload.get("message", ""), payload.get("level", "info"))
            self.send_json({"log_entry": entry})
        else:
            self.send_json({"error": "Not found"}, 404)

    # -- POST bodies (return plain dicts; guarded_json handles errors) ---
    def _export(self, payload):
        from utils.export import export_workspace
        result = export_workspace(payload["workspace"], payload.get("out_dir", "."),
                                  formats=payload.get("formats", ["png"]))
        return {"ok": True, "files": result}

    def _reparse(self, payload):
        db_name = payload.get("database")
        if not db_name or db_name not in db.DB_REGISTRY:
            raise ValueError(f"Unknown database: {db_name}")
        result = hook_reparse(db_name)
        entry = log(result["message"], "info" if result["ok"] else "error")
        return {**result, "log_entry": entry, **db_status()}

    def _fetch_remote(self, payload):
        system, paths = payload.get("system"), payload.get("paths", [])
        if not system:
            raise ValueError("No system specified.")
        result = hook_fetch_remote(system, paths)
        entry = log(result["message"], "info" if result["ok"] else "error")
        return {**result, "log_entry": entry, **db_status()}

    def _reload_plugins(self, payload):
        dirs = payload.get("dirs", [])
        if dirs:
            plots.load_plugins(dirs)
        entry = log(f"Plugins reloaded: {list(plots.PLUGIN_PLOTS.keys()) or 'none'}")
        return {"reloaded": list(plots.PLUGIN_PLOTS.keys()), "plot_types": plots.plot_type_catalog(),
                "log_entry": entry}


def launch_visualize_web_server(parser, presets, port=8765, plugins=None):
    from sbatchman.config.project_config import get_project_root
    from sbatchman.parser import parse_jobs_and_generate_sqlite_db

    db_path = get_project_root() / "data.sqlite"
    parse_jobs_and_generate_sqlite_db(parser=parser, output_path=db_path)
    db.load_databases([db_path])
    if not db.DB_REGISTRY:
        log("No valid databases loaded. Exiting.", "error")
        raise SystemExit(1)

    REPARSE_SOURCES[Path(db_path).stem] = {"parser": parser, "output_path": db_path}
    load_remote_systems()
    if plugins:
        plots.load_plugins(plugins)
    load_initial_workspace()

    if not HTML_PATH.exists():
        log(f"Missing front-end file: {HTML_PATH}", "error")
        raise SystemExit(1)

    server = HTTPServer(("127.0.0.1", port), Handler)
    url = f"http://127.0.0.1:{port}"
    print(f"\n  Plot Builder\n  {'-'*25}\n  URL   : {url}\n  Docs  : {url}/docs")
    print(f"  DBs   : {', '.join(db.DB_REGISTRY.keys())}")
    print(f"  Plots : {', '.join(plots.BUILTIN_PLOTS.keys())}")
    if plots.PLUGIN_PLOTS:
        print(f"  Plugins: {', '.join(plots.PLUGIN_PLOTS.keys())}")
    if get_initial_workspace():
        print(f"  Workspace: loaded from ./plots.json")
    print("\n  Press Ctrl+C to stop\n")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[stopped]")
        server.server_close()
