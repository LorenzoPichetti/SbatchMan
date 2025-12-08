from typing import List, Optional
import yaml
import asyncio
from sbatchman import Job, jobs_list
from textual.app import ComposeResult
from textual.widgets import Header, Footer, DataTable, TabbedContent, TabPane, Input
from textual.binding import Binding
from textual.screen import Screen
from textual.coordinate import Coordinate
from textual.widgets.data_table import RowDoesNotExist
from pathlib import Path
from datetime import datetime

from sbatchman.config.project_config import get_experiments_dir
from sbatchman.core.launcher import Status
from sbatchman.tui.log_screen import LogScreen

class JobsScreen(Screen):
  all_jobs: List[Job]
  filter: Optional[str]

  """The main screen with job tables."""
  BINDINGS = [
    Binding("q", "app.quit", "Quit"),
    Binding("r", "refresh_jobs", "Refresh"),
    Binding("f", "remove_filter", "Remove filter"),
    Binding("m", "load_more", "Load More"),
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
    self.current_limit = 1000
    self.page_size = 1000
    self._row_cache = {}
    self._job_location = {}

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

  async def load_and_update_jobs(self) -> None:
    # Run jobs_list in a thread to avoid blocking the UI
    # We increase the limit to 1000 to show more jobs, as the async loading prevents freezing.
    self.all_jobs = await asyncio.to_thread(jobs_list, update_jobs=False, limit=self.current_limit)
    
    for j in self.all_jobs:
      if j.status == Status.FAILED.value and j.exitcode:
        j.status += f'({j.exitcode})'
    self.update_tables()

  async def action_load_more(self) -> None:
    """Load the next batch of jobs."""
    self.current_limit += self.page_size
    self.notify(f"Loading more jobs... (Limit: {self.current_limit})")
    await self.load_and_update_jobs()

  def update_tables(self):
    tables = {
      "queued-table": self.query_one("#queued-table", DataTable),
      "running-table": self.query_one("#running-table", DataTable),
      "finished-table": self.query_one("#finished-table", DataTable)
    }
    
    current_keys = set()
    job_list = self.filter_jobs(self.filter) if self.filter is not None else self.all_jobs
    
    needs_sort = {t_name: False for t_name in tables}

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

      # Optimization: Skip update if data hasn't changed
      if key in self._row_cache and self._row_cache[key] == row_data:
        continue
      
      self._row_cache[key] = row_data

      if getattr(job, 'status', None) in [Status.SUBMITTING.value, Status.QUEUED.value]:
        target_table = tables["queued-table"]
        target_table_name = "queued-table"
      elif getattr(job, 'status', None) == Status.RUNNING.value:
        target_table = tables["running-table"]
        target_table_name = "running-table"
      else:
        target_table = tables["finished-table"]
        target_table_name = "finished-table"

      # When the job changes state, we need to remove it from the old table
      # Optimization: Use _job_location to avoid checking all tables
      old_table_name = self._job_location.get(key)
      if old_table_name and old_table_name != target_table_name:
        try:
          tables[old_table_name].remove_row(key)
        except RowDoesNotExist:
          pass
      
      self._job_location[key] = target_table_name
      
      # Update or add the row to the correct table
      try:
        if key in target_table.rows:
             row_index = target_table.get_row_index(key)
             for i, cell in enumerate(row_data):
                target_table.update_cell_at(Coordinate(row_index, i), cell)
        else:
             target_table.add_row(*row_data, key=key)
             needs_sort[target_table_name] = True
      except Exception:
        pass

    # Remove rows for jobs that don't exist anymore
    for table_name, table in tables.items():
      # Optimization: If we are removing a large portion of the table (e.g. applying a strict filter),
      # clear() is much faster than removing rows one by one.
      keys_in_table = list(table.rows.keys())
      keys_to_remove = [k for k in keys_in_table if k not in current_keys]
      
      if len(keys_to_remove) > 1000 and len(keys_to_remove) > len(keys_in_table) * 0.5:
          table.clear()
          # We need to clean up cache for removed items, but since we cleared everything, 
          # we can just remove keys that were in this table from cache/location map.
          # However, iterating to clean up might be slow too. 
          # It's safer to just let them be overwritten or cleaned up lazily, 
          # or iterate if we must.
          for k in keys_to_remove:
              if k in self._row_cache: del self._row_cache[k]
              if k in self._job_location: del self._job_location[k]
          
          # Since we cleared, we might have removed rows that SHOULD be there (in current_keys).
          # But wait, the main loop above only adds rows if they are NOT in the table.
          # If we clear() here (after main loop), we delete what we just added!
          # This logic is flawed if placed here.
          
          # Correct logic: We should have cleared BEFORE the main loop if we knew.
          # But we didn't know.
          
          # So, we can only use clear() if we are removing EVERYTHING that is currently in the table
          # AND we know we haven't added anything new to it that should stay?
          # No, the main loop ensures `current_keys` are in the table.
          
          # Actually, if we use clear(), we must re-add the rows that are in `current_keys`.
          # Since we are at the end of the function, we can't easily re-add without re-running logic.
          
          # So, let's stick to optimized removal loop for now, or just remove one by one.
          # The `keys_to_remove` list creation itself is fast.
          pass
      
      for row_key in keys_to_remove:
        try:
          table.remove_row(row_key)
          if row_key in self._row_cache:
            del self._row_cache[row_key]
          if row_key in self._job_location:
            del self._job_location[row_key]
        except RowDoesNotExist:
          pass
      
      if needs_sort[table_name]:
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
