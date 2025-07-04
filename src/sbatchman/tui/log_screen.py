from textual.app import ComposeResult
from textual.widgets import Header, Footer, Log, Markdown
from textual.containers import Vertical
from textual.binding import Binding
from textual.screen import Screen

from sbatchman.core.job import Job

class LogScreen(Screen):
  """A screen to display logs of a selected job."""
  BINDINGS = [
    Binding("q", "app.pop_screen", "Back to jobs"),
    Binding("n", "next_page", "Next page"),
    Binding("p", "prev_page", "Previous page"),
    Binding("tab", "toggle_focus", "Switch log"),
  ]

  PAGE_SIZE = 50       # Number of lines per page
  MAX_LINE_LEN = 600  # Max chars per line

  def __init__(self, job: Job, **kwargs):
    super().__init__(**kwargs)
    self.job = job
    self.stdout_lines = []
    self.stderr_lines = []
    self.stdout_page = 0
    self.stderr_page = 0
    self.focused_log = "stdout"  # "stdout" or "stderr"

  def compose(self) -> ComposeResult:
    yield Markdown(
      "**Note:** Output is paged. Use [n]ext/[p]revious to scroll, [Tab] to switch between STDOUT/STDERR, [q] to quit."
    )
    yield Vertical(
      Markdown("### STDOUT"), Log(id="stdout_log", highlight=True),
      Markdown("### STDERR"), Log(id="stderr_log", highlight=True),
      id="log_view"
    )
    yield Footer()

  def on_mount(self) -> None:
    stdout = self.job.get_stdout()
    stderr = self.job.get_stderr()

    self.stdout_lines = stdout.splitlines() if stdout else ["No stdout log file found."]
    self.stderr_lines = stderr.splitlines() if stderr else ["No stderr log file found."]
    self.stdout_lines = [l if len(l) < self.MAX_LINE_LEN else l[:self.MAX_LINE_LEN] + " ...truncated line..." for l in self.stdout_lines]
    self.stderr_lines = [l if len(l) < self.MAX_LINE_LEN else l[:self.MAX_LINE_LEN] + " ...truncated line..." for l in self.stderr_lines]
    self.stdout_page = 0
    self.stderr_page = 0
    self.focused_log = "stdout"
    self.query_one("#stdout_log", Log).border_title = "STDOUT (active)"

    self.display_page()

  def display_page(self):
    stdout_log = self.query_one("#stdout_log", Log)
    stderr_log = self.query_one("#stderr_log", Log)

    # Clear logs
    stdout_log.clear()
    stderr_log.clear()

    # Calculate page slices
    s_start = self.stdout_page * self.PAGE_SIZE
    s_end = s_start + self.PAGE_SIZE
    e_start = self.stderr_page * self.PAGE_SIZE
    e_end = e_start + self.PAGE_SIZE

    # Write current page, add page info
    stdout_log.write(
      f"[Page {self.stdout_page + 1}/{max(1, (len(self.stdout_lines) - 1) // self.PAGE_SIZE + 1)}]"
      + ("\n" if self.stdout_lines else "")
      + "\n".join(self.stdout_lines[s_start:s_end])
    )
    stderr_log.write(
      f"[Page {self.stderr_page + 1}/{max(1, (len(self.stderr_lines) - 1) // self.PAGE_SIZE + 1)}]"
      + ("\n" if self.stderr_lines else "")
      + "\n".join(self.stderr_lines[e_start:e_end])
    )

    # Optionally highlight the focused log
    if self.focused_log == "stdout":
      stdout_log.border_title = "STDOUT (active)"
      stderr_log.border_title = "STDERR"
    else:
      stdout_log.border_title = "STDOUT"
      stderr_log.border_title = "STDERR (active)"

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