from textual.app import ComposeResult
from textual.widgets import Footer, Log, Markdown
from textual.containers import Vertical
from textual.binding import Binding
from textual.screen import Screen
from textual.events import MouseDown

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
    self.focused_log = "stdout"

  def compose(self) -> ComposeResult:
    yield Vertical(
      Markdown("**STDOUT**", id="stdout_title"), Log(id="stdout_log", highlight=True),
      Markdown("**STDERR**", id="stderr_title"), Log(id="stderr_log", highlight=True),
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