# src/exp_kit/tui.py
from textual.app import App, ComposeResult
from textual.widgets import Header, Footer, DataTable, Log, TabbedContent, TabPane, Markdown
from textual.containers import Vertical
from textual.binding import Binding
from textual.screen import Screen
from textual.coordinate import Coordinate
from textual.widgets.data_table import RowDoesNotExist
from pathlib import Path
import json
from typing import Optional
from datetime import datetime

from .config import get_experiments_dir

from .schedulers.slurm import SlurmScheduler
from .schedulers.pbs import PbsScheduler
from .schedulers.local import LocalScheduler

SCHEDULER_INSTANCE_MAP = {
  "SlurmScheduler": SlurmScheduler(),
  "PbsScheduler": PbsScheduler(),
  "LocalScheduler": LocalScheduler(),
}

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

class JobsScreen(Screen):
  """The main screen with job tables."""
  BINDINGS = [
    Binding("q", "app.quit", "Quit"),
    Binding("r", "refresh_jobs", "Refresh"),
    Binding("enter", "select_cursor", "View Logs", priority=True)
  ]

  CSS = """
  DataTable {
    height: 1fr;
  }
  """

  def __init__(self, experiments_dir: Optional[Path] = None, **kwargs):
    super().__init__(**kwargs)
    self.experiments_root = experiments_dir or get_experiments_dir()
    self.all_jobs = []

  def compose(self) -> ComposeResult:
    yield Header()
    with TabbedContent(id="tabs"):
      with TabPane("Queued", id="queued-tab"):
        yield DataTable(id="queued-table")
      with TabPane("Running", id="running-tab"):
        yield DataTable(id="running-table")
      with TabPane("Finished/Failed", id="finished-tab"):
        yield DataTable(id="finished-table")
    yield Footer()

  def on_mount(self) -> None:
    for table_id in ["#queued-table", "#running-table", "#finished-table"]:
      table = self.query_one(table_id, DataTable)
      table.cursor_type = "row"
      table.add_column("Time", width=20, key="timestamp")
      table.add_column("Name", width=20)
      table.add_column("Job ID", width=12)
      table.add_column("Status", width=18)
      table.add_column("Command")
    
    self.load_and_update_jobs()
    self.timer = self.set_interval(5, self.load_and_update_jobs)

  def load_and_update_jobs(self) -> None:
    experiments = []
    if self.experiments_root.exists():
      for config_dir in self.experiments_root.iterdir():
        if not config_dir.is_dir(): continue
        for exp_dir in config_dir.iterdir():
          metadata_path = exp_dir / "metadata.json"
          if exp_dir.is_dir() and metadata_path.exists():
            with open(metadata_path, "r") as f:
              try:
                data = json.load(f)
                data['exp_dir'] = str(exp_dir)
                experiments.append(data)
              except json.JSONDecodeError:
                self.log(f"Error decoding JSON from {metadata_path}")
    
    self.all_jobs = experiments
    self.update_tables()

  def update_tables(self):
    tables = {
      "queued-table": self.query_one("#queued-table", DataTable),
      "running-table": self.query_one("#running-table", DataTable),
      "finished-table": self.query_one("#finished-table", DataTable)
    }
    
    current_keys = set()
    for job in self.all_jobs:
      key = job.get('exp_dir')
      if not key:
        continue
      
      current_keys.add(key)
      
      timestamp_str = job.get('timestamp', '')
      formatted_timestamp = timestamp_str
      if timestamp_str:
        try:
          dt_object = datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")
          formatted_timestamp = dt_object.strftime('%Y-%m-%d %H:%M:%S')
        except (ValueError, TypeError):
          formatted_timestamp = timestamp_str

      row_data = (
        formatted_timestamp,
        job.get('name', 'N/A'),
        job.get('job_id', 'N/A'),
        job.get('status', 'UNKNOWN'),
        job.get('command') or '',
      )

      if job.get('status') in ["QUEUED", "SUBMITTING"]:
        target_table = tables["queued-table"]
      elif job.get('status') == "RUNNING":
        target_table = tables["running-table"]
      else:
        target_table = tables["finished-table"]

      # When the job changes state, we need to remove it from the old table
      for table_name, table in tables.items():
        if table is not target_table:
          try:
            table.remove_row(key)
          except RowDoesNotExist:
            pass
      
      # Update or add the row to the correct table
      try:
        row_index = target_table.get_row_index(key)
        for i, cell in enumerate(row_data):
          target_table.update_cell_at(Coordinate(row_index, i), cell)
      except RowDoesNotExist:
        target_table.add_row(*row_data, key=key)

    # Remove rows for jobs that don't exist anymore
    for table in tables.values():
      for row_key in list(table.rows.keys()):
        if row_key not in current_keys:
          try:
            table.remove_row(row_key)
          except RowDoesNotExist:
            pass
      table.sort("timestamp", reverse=True)

  async def action_refresh_jobs(self) -> None:
    self.load_and_update_jobs()
  
  def action_select_cursor(self) -> None:
    active_tab_id = self.query_one(TabbedContent).active
    if not active_tab_id:
      return
    active_table = self.query_one(f"#{active_tab_id.replace('tab', 'table')}", DataTable)
    if active_table.row_count > 0:
      coord = active_table.cursor_coordinate
      try:
        exp_dir_str = active_table.coordinate_to_cell_key(coord).row_key.value or ''
        self.app.push_screen(LogScreen(exp_dir= self.experiments_root / exp_dir_str))
      except RowDoesNotExist:
        pass

class ExperimentTUI(App):
  TITLE = "SBatchMan Status"
  
  def __init__(self, experiments_dir: Optional[Path] = None, **kwargs):
    super().__init__(**kwargs)
    self.animation_level = "none"
    self.experiments_root = experiments_dir or get_experiments_dir()

  def on_mount(self) -> None:
    self.push_screen(JobsScreen(experiments_dir=self.experiments_root))

def run_tui(experiments_dir: Optional[Path] = None):
  app = ExperimentTUI(experiments_dir=experiments_dir)
  app.run()