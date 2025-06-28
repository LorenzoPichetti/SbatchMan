# src/exp_kit/tui.py
import json
import subprocess
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Any, Optional

from textual.app import App, ComposeResult
from textual.widgets import Header, Footer, DataTable, Log, TabbedContent, TabPane, Markdown
from textual.binding import Binding
from textual.containers import VerticalScroll
from textual.reactive import reactive

from .config import get_experiments_dir
from .schedulers.slurm import SlurmScheduler
from .schedulers.pbs import PbsScheduler
from .schedulers.local import LocalScheduler

SCHEDULER_INSTANCE_MAP = {
  "SlurmScheduler": SlurmScheduler(),
  "PbsScheduler": PbsScheduler(),
  "LocalScheduler": LocalScheduler(),
}

class ExperimentTUI(App):
  TITLE = "SBatchMans Status"
  BINDINGS = [
    Binding("q", "quit", "Quit"),
    Binding("r", "refresh_jobs", "Refresh"),
    Binding("b", "back", "Back", show=False),
    Binding("enter", "select_cursor", "View Logs", priority=True)
  ]
  all_jobs = reactive(list)

  def __init__(self, experiments_dir: Optional[Path] = None, **kwargs):
    super().__init__(**kwargs)
    # Use the provided directory, or auto-detect if not provided.
    self.experiments_root = experiments_dir or get_experiments_dir()

  def compose(self) -> ComposeResult:
    yield Header()
    with TabbedContent(id="tabs") as tabs:
      with TabPane("Queued", id="queued_tab"):
        yield DataTable(id="queued_table")
      with TabPane("Running", id="running_tab"):
        yield DataTable(id="running_table")
      with TabPane("Finished/Failed", id="finished_tab"):
        yield DataTable(id="finished_table")
    yield VerticalScroll(
      Markdown("### STDOUT"), Log(id="stdout_log", highlight=True),
      Markdown("### STDERR"), Log(id="stderr_log", highlight=True),
      id="log_view", classes="hidden"
    )
    yield Footer()

  def on_mount(self) -> None:
    self.load_and_update_jobs()
    self.set_interval(5, self.load_and_update_jobs)
    for table_id in ["#queued_table", "#running_table", "#finished_table"]:
      table = self.query_one(table_id, DataTable)
      table.cursor_type = "row"
      table.add_column("Timestamp", width=16)
      table.add_column("Name", width=30)
      table.add_column("Job ID", width=12)
      table.add_column("Status", width=12)
      table.add_column("Queue Info", width=20)
      table.add_column("Comment")
      table.add_column("exp_dir", width=0)

  def load_and_update_jobs(self) -> None:
    experiments = []
    if self.experiments_root.exists():
      for config_dir in self.experiments_root.iterdir():
        if not config_dir.is_dir(): continue
        for exp_dir in config_dir.iterdir():
          metadata_path = exp_dir / "metadata.json"
          if exp_dir.is_dir() and metadata_path.exists():
            with open(metadata_path, "r") as f:
              try: experiments.append(json.load(f))
              except json.JSONDecodeError: continue
    
    experiments.sort(key=lambda j: j.get('timestamp', ''), reverse=True)

    jobs_to_query = defaultdict(list)
    for job in experiments:
      if job['status'] in ["QUEUED", "RUNNING", "SUBMITTING"] and job.get('job_id'):
        jobs_to_query[job['scheduler']].append(job['job_id'])
    live_statuses = {}
    for scheduler_name, job_ids in jobs_to_query.items():
      if scheduler := SCHEDULER_INSTANCE_MAP.get(scheduler_name):
        live_statuses.update(scheduler.get_status(job_ids))
    for job in experiments:
      job_id = job.get('job_id')
      if job_id and job_id in live_statuses:
        new_status, new_queue_info = live_statuses[job_id]
        if new_status != job['status'] or new_queue_info != job.get('queue_info'):
          job['status'] = new_status
          job['queue_info'] = new_queue_info
          with open(Path(job['exp_dir']) / "metadata.json", "w") as f:
            json.dump(job, f, indent=4)
    self.all_jobs = experiments
    self.update_tables()

  def update_tables(self):
    tables = {"queued": self.query_one("#queued_table", DataTable), "running": self.query_one("#running_table", DataTable), "finished": self.query_one("#finished_table", DataTable)}
    for table in tables.values(): table.clear()
    for job in self.all_jobs:
      row_data = (job['timestamp'], job['name'], job.get('job_id', 'N/A'), job['status'], job.get('queue_info') or '', job['comment'], job['exp_dir'])
      if job['status'] in ["QUEUED", "SUBMITTING"]: tables["queued"].add_row(*row_data, key=str(job['exp_dir']))
      elif job['status'] in ["RUNNING"]: tables["running"].add_row(*row_data, key=str(job['exp_dir']))
      else: tables["finished"].add_row(*row_data, key=str(job['exp_dir']))

  async def action_refresh_jobs(self) -> None:
    self.load_and_update_jobs()
    self.notify("Jobs refreshed.")
  
  def show_log_view(self, show: bool):
    self.query_one("#tabs").display = not show
    self.query_one("#log_view").display = show
    self.set_focus(self.query_one("#log_view") if show else self.query_one("#tabs"))

  def _validate_back(self) -> bool:
    return self.query_one("#log_view").display

  async def action_back(self) -> None:
    self.show_log_view(False)

  def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
    if event.row_key is None or event.row_key.value is None: return
    exp_dir = Path(event.row_key.value)
    stdout_log_path, stderr_log_path = exp_dir / "stdout.log", exp_dir / "stderr.log"
    stdout_log, stderr_log = self.query_one("#stdout_log", Log), self.query_one("#stderr_log", Log)
    stdout_log.clear(); stderr_log.clear()
    if stdout_log_path.exists(): stdout_log.write(stdout_log_path.read_text())
    if stderr_log_path.exists(): stderr_log.write(stderr_log_path.read_text())
    self.show_log_view(True)

def run_tui(experiments_dir: Optional[Path] = None):
  app = ExperimentTUI(experiments_dir=experiments_dir)
  app.run()