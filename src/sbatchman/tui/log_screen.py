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
  ]

  def __init__(self, job: Job, **kwargs):
    super().__init__(**kwargs)
    self.job = job

  def compose(self) -> ComposeResult:
    yield Header()
    yield Vertical(
      Markdown("### STDOUT"), Log(id="stdout_log", highlight=True),
      Markdown("### STDERR"), Log(id="stderr_log", highlight=True),
      id="log_view"
    )
    yield Footer()

  def on_mount(self) -> None:
    """Load and display the logs."""
    stdout_log = self.query_one("#stdout_log", Log)
    stderr_log = self.query_one("#stderr_log", Log)
    
    stdout = self.job.get_stdout()
    stderr = self.job.get_stderr()

    if stdout:
      stdout_log.write(stdout)
    else:
      stdout_log.write(f"No stdout log file found.")

    if stderr:
      stderr_log.write(stderr)
    else:
      stderr_log.write(f"No stderr log file found.")
