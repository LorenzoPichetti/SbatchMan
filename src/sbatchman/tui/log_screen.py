from textual.app import App, ComposeResult
from textual.widgets import Header, Footer, Log, Markdown
from textual.containers import Vertical
from textual.binding import Binding
from textual.screen import Screen
from pathlib import Path


class LogScreen(Screen):
  """A screen to display logs of a selected job."""
  BINDINGS = [
    Binding("q", "app.pop_screen", "Back to jobs"),
  ]

  def __init__(self, exp_dir: Path, **kwargs):
    super().__init__(**kwargs)
    self.exp_dir = exp_dir

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
    
    stdout_path = self.exp_dir / "stdout.log"
    stderr_path = self.exp_dir / "stderr.log"

    if stdout_path.exists():
      with open(stdout_path, "r") as f:
        stdout_log.write(f.read())
    else:
      stdout_log.write(f"No stdout log file found.")

    if stderr_path.exists():
      with open(stderr_path, "r") as f:
        stderr_log.write(f.read())
    else:
      stderr_log.write(f"No stderr log file found.")

