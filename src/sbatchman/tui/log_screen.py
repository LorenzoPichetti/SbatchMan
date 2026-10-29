from textual.app import ComposeResult
from textual.widgets import Footer, Log, Markdown
from textual.containers import Vertical
from textual.binding import Binding
from textual.screen import Screen
from textual.events import MouseDown
from textual.message import Message

from sbatchman.core.job import Job

class LogScreen(Screen):
  """A screen to display logs of a selected job."""
  BINDINGS = [
    Binding("q", "app.pop_screen", "Back to jobs"),
    Binding("n", "next_page", "Next page"),
    Binding("p", "prev_page", "Previous page"),
    Binding("tab", "toggle_focus", "Switch log"),
    Binding("c", "copy_path", "Copy log path"),
  ]

  PAGE_SIZE = 50       # Number of lines per page
  MAX_LINE_LEN = 600  # Max chars per line

  class LogsChanged(Message):
    def __init__(self, stdout, stderr, stdout_signature, stderr_signature):
      super().__init__()
      self.stdout = stdout
      self.stderr = stderr
      self.stdout_signature = stdout_signature
      self.stderr_signature = stderr_signature

  def __init__(self, job: Job, log_dir=None, **kwargs):
    super().__init__(**kwargs)
    self.job = job
    # Campaign jobs are loaded from an isolated project. Their Job objects
    # retain relative exp_dir values, but the project context used to load
    # them is gone by the time this screen's background log reader runs.
    self.log_dir = log_dir
    self.stdout_lines = []
    self.stderr_lines = []
    self.stdout_page = 0
    self.stderr_page = 0
    self.focused_log = "stdout"
    self._log_signatures = None
    self._log_worker = None

  def compose(self) -> ComposeResult:
    yield Vertical(
      Markdown("**STDOUT**", id="stdout_title"), Log(id="stdout_log", highlight=True),
      Markdown("**STDERR**", id="stderr_title"), Log(id="stderr_log", highlight=True),
      id="log_view"
    )
    yield Footer()

  def on_mount(self) -> None:
    self.stdout_page = 0
    self.stderr_page = 0
    self.focused_log = "stdout"
    self.query_one("#stdout_log", Log).border_title = "STDOUT (active)"

    self.display_page()
    # The first read uses the same background path as subsequent refreshes.
    self._log_signatures = None
    self.set_interval(1.0, self._check_logs)
    self._check_logs()

  @staticmethod
  def _file_signature(path):
    try:
      stat = path.stat()
      return (stat.st_mtime_ns, stat.st_size, stat.st_ino)
    except OSError:
      return None

  def _log_paths(self):
    if self.log_dir is not None:
      return self.log_dir / "stdout.log", self.log_dir / "stderr.log"
    return self.job.get_stdout_path(), self.job.get_stderr_path()

  def _check_logs(self) -> None:
    # Filesystem access can block (for example on a network mount), so keep
    # both the checks and reads off the UI thread. Never queue overlapping scans.
    if self._log_worker is None or not self._log_worker.is_running:
      self._log_worker = self.run_worker(self._read_changed_logs, thread=True, name="read-job-logs")

  def _read_changed_logs(self) -> None:
    paths = self._log_paths()
    signatures = tuple(self._file_signature(path) for path in paths)
    if self._log_signatures is not None and signatures == self._log_signatures:
      return

    contents = []
    for path in paths:
      try:
        contents.append(path.read_text())
      except (OSError, UnicodeError):
        contents.append(None)
    lines = []
    for content, label in zip(contents, ("stdout", "stderr")):
      split = content.splitlines() if content else [f"No {label} log file found."]
      lines.append([line if len(line) < self.MAX_LINE_LEN else line[:self.MAX_LINE_LEN] + " ...truncated line..." for line in split])
    self.post_message(self.LogsChanged(lines[0], lines[1], *signatures))

  # Textual includes the enclosing class in handler names for nested message
  # classes, so LogsChanged dispatches to on_log_screen_logs_changed.
  def on_log_screen_logs_changed(self, message: "LogScreen.LogsChanged") -> None:
    self._log_signatures = (message.stdout_signature, message.stderr_signature)
    self.stdout_lines = message.stdout
    self.stderr_lines = message.stderr
    self.stdout_page = min(self.stdout_page, max(0, (len(self.stdout_lines) - 1) // self.PAGE_SIZE))
    self.stderr_page = min(self.stderr_page, max(0, (len(self.stderr_lines) - 1) // self.PAGE_SIZE))
    self.display_page()

  def display_page(self):
    stdout_log = self.query_one("#stdout_log", Log)
    stderr_log = self.query_one("#stderr_log", Log)
    stdout_title = self.query_one("#stdout_title", Markdown)
    stderr_title = self.query_one("#stderr_title", Markdown)

    # Clear logs
    stdout_log.clear()
    stderr_log.clear()

    # Calculate page slices
    s_start = self.stdout_page * self.PAGE_SIZE
    s_end = s_start + self.PAGE_SIZE
    e_start = self.stderr_page * self.PAGE_SIZE
    e_end = e_start + self.PAGE_SIZE

    # Total page counts
    stdout_total_pages = max(1, (len(self.stdout_lines) - 1) // self.PAGE_SIZE + 1)
    stderr_total_pages = max(1, (len(self.stderr_lines) - 1) // self.PAGE_SIZE + 1)

    # Update Markdown headers with page info and active status
    if self.focused_log == "stdout":
      stdout_title.update(f"**STDOUT** [Page {self.stdout_page + 1}/{stdout_total_pages}] (active)")
      stderr_title.update(f"**STDERR** [Page {self.stderr_page + 1}/{stderr_total_pages}]")
    else:
      stdout_title.update(f"**STDOUT** [Page {self.stdout_page + 1}/{stdout_total_pages}]")
      stderr_title.update(f"**STDERR** [Page {self.stderr_page + 1}/{stderr_total_pages}] (active)")

    # Write log content
    stdout_log.write("\n".join(self.stdout_lines[s_start:s_end]))
    stderr_log.write("\n".join(self.stderr_lines[e_start:e_end]))

  def action_next_page(self):
    if self.focused_log == "stdout":
      if (self.stdout_page + 1) * self.PAGE_SIZE < len(self.stdout_lines):
        self.stdout_page += 1
    else:
      if (self.stderr_page + 1) * self.PAGE_SIZE < len(self.stderr_lines):
        self.stderr_page += 1
    self.display_page()

  def action_prev_page(self):
    if self.focused_log == "stdout":
      if self.stdout_page > 0:
        self.stdout_page -= 1
    else:
      if self.stderr_page > 0:
        self.stderr_page -= 1
    self.display_page()

  def action_toggle_focus(self):
    self.focused_log = "stderr" if self.focused_log == "stdout" else "stdout"
    self.display_page()

  def action_copy_path(self) -> None:
    """Copy the full path of the currently focused log file."""
    path = (self._log_paths()[0 if self.focused_log == "stdout" else 1]).resolve()
    self.app.copy_to_clipboard(str(path))
    self.notify(f"Copied {self.focused_log.upper()} path: {path}")

  def on_mouse_down(self, event: MouseDown) -> None:
    # FIXME doesn't work...
    if event.widget and event.widget.id:
      target_id = event.widget.id
      if target_id == "stdout_log" and self.focused_log != "stdout":
        self.focused_log = "stdout"
        self.display_page()
      elif target_id == "stderr_log" and self.focused_log != "stderr":
        self.focused_log = "stderr"
        self.display_page()
