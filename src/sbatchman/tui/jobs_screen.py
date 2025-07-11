import yaml
from sbatchman import Job, jobs_list
from textual import on
from textual.app import ComposeResult
from textual.widgets import Header, Footer, DataTable, TabbedContent, TabPane, Input, Markdown
from textual.binding import Binding
from textual.screen import Screen
from textual.coordinate import Coordinate
from textual.widgets.data_table import RowDoesNotExist
from pathlib import Path
from typing import List, Optional
from datetime import datetime

from sbatchman.config.project_config import get_experiments_dir
from sbatchman.core.launcher import Status
from sbatchman.tui.log_screen import LogScreen

class JobsScreen(Screen):
  all_jobs: List[Job]
  filter: None

  """The main screen with job tables."""
  BINDINGS = [
    Binding("q", "app.quit", "Quit"),
    Binding("r", "refresh_jobs", "Refresh"),
    Binding("f", "remove_filter", "Remove filter"),
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
    self.filter = None
    self.filtered_finished_jobs: Optional[List[Job]] = None

  def compose(self) -> ComposeResult:
    yield Header()
    with TabbedContent(id="tabs"):
      with TabPane("Queued", id="queued-tab"):
        yield DataTable(id="queued-table")
      with TabPane("Running", id="running-tab"):
        yield DataTable(id="running-table")
      with TabPane("Finished/Failed", id="finished-tab"):
        yield DataTable(id="finished-table")
        yield Input(placeholder="Filter example: status=FAILED, config=my_config, time>2024-01-01", id="filter-input")
        # yield Markdown("Debug", id='dbg')
    yield Footer()

  def on_mount(self) -> None:
    for table_id in ["#queued-table", "#running-table", "#finished-table"]:
      table = self.query_one(table_id, DataTable)
      table.cursor_type = "row"
      table.add_column("Time", key="timestamp")
      table.add_column("Config")
      table.add_column("Tag")
      table.add_column("Job ID")
      table.add_column("Status")
      table.add_column("Command")
    
    self.load_and_update_jobs()
    self.timer = self.set_interval(30, self.load_and_update_jobs)

  def action_remove_filter(self) -> None:
    self.filter = None
    self.update_tables()

  def apply_filter(self) -> None:
    query = self.query_one("#filter-input", Input).value.strip()
    # self.query_one("#dbg", Markdown).update(query)
    self.filter = query
    self.update_tables()

  def load_and_update_jobs(self) -> None:
    self.all_jobs = jobs_list()
    self.update_tables()

  def update_tables(self):
    tables = {
      "queued-table": self.query_one("#queued-table", DataTable),
      "running-table": self.query_one("#running-table", DataTable),
      "finished-table": self.query_one("#finished-table", DataTable)
    }
    
    current_keys = set()
    job_list = self.filter_jobs(self.filter) if self.filter is not None else self.all_jobs
    for job in job_list:
      key = job.exp_dir
      if not key:
        continue
      
      current_keys.add(key)
      
      timestamp_str = job.timestamp
      formatted_timestamp = timestamp_str
      if timestamp_str:
        try:
          dt_object = datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")
          formatted_timestamp = dt_object.strftime('%Y-%m-%d %H:%M:%S')
        except (ValueError, TypeError):
          formatted_timestamp = timestamp_str

      row_data = (
        formatted_timestamp,
        getattr(job, 'config_name', 'N/A'),
        getattr(job, 'tag', 'N/A'),
        getattr(job, 'job_id', 'N/A'),
        getattr(job, 'status', 'UNKNOWN'),
        getattr(job, 'command', '') or '',
      )
      if getattr(job, 'status', None) in [Status.SUBMITTING.value, Status.QUEUED.value]:
        target_table = tables["queued-table"]
      elif getattr(job, 'status', None) == Status.RUNNING.value:
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
    if self.query_one("#filter-input", Input).has_focus:
      self.apply_filter()
      return
    active_tab_id = self.query_one(TabbedContent).active
    if not active_tab_id or active_tab_id != 'finished-tab':
      return
    active_table = self.query_one(f"#{active_tab_id.replace('tab', 'table')}", DataTable)
    if active_table.row_count > 0:
      coord = active_table.cursor_coordinate
      try:
        exp_dir_str = active_table.coordinate_to_cell_key(coord).row_key.value or ''
        self.app.push_screen(LogScreen(job=Job(**yaml.safe_load(open(self.experiments_root / exp_dir_str / "metadata.yaml", 'r')))))
      except RowDoesNotExist:
        pass

  def filter_jobs(self, query: str) -> List[Job]:
    if not query:
      return self.all_jobs

    filters = {}
    for part in query.split(","):
      if "=" in part:
        k, v = part.strip().split("=", 1)
        filters[k.strip()] = v.strip()
      elif ">" in part:
        k, v = part.strip().split(">", 1)
        filters[f"{k.strip()}__gt"] = v.strip()
      elif "<" in part:
        k, v = part.strip().split("<", 1)
        filters[f"{k.strip()}__lt"] = v.strip()

    def match(job: Job) -> bool:
      for key, val in filters.items():
        attr = getattr(job, key.replace("__gt", "").replace("__lt", ""), None)
        if attr is None:
          continue
        if "time" in key:
            try:
              job_time = datetime.strptime(job.timestamp, "%Y%m%d_%H%M%S")
              val_time = datetime.fromisoformat(val)
              if "__gt" in key and not (job_time > val_time): return False
              if "__lt" in key and not (job_time < val_time): return False
            except Exception:
              return False
        elif "status" in key:
          if attr.upper() != val.upper():
            return False
        elif "config" in key:
          if val.lower() not in attr.lower():
            return False
      return True

    return [job for job in self.all_jobs if job.status not in [Status.SUBMITTING.value, Status.QUEUED.value, Status.RUNNING.value] and match(job)]
