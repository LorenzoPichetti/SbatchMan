from textual.app import App
from pathlib import Path
from typing import List, Optional

from sbatchman.config.project_config import get_experiments_dir
from sbatchman.tui.jobs_screen import JobsScreen

class ExperimentTUI(App):
  TITLE = "SbatchMan Status"
  CSS_PATH = "style.tcss"
  
  def __init__(self, experiments_dir: Optional[Path] = None, columns: Optional[List[str]] = None, **kwargs):
    super().__init__(**kwargs)
    self.animation_level = "none"
    self.experiments_root = experiments_dir or get_experiments_dir()
    self.columns = columns

  def on_mount(self) -> None:
    self.push_screen(JobsScreen(experiments_dir=self.experiments_root, columns=self.columns))

def run_tui(experiments_dir: Optional[Path] = None, columns: Optional[List[str]] = None):
  app = ExperimentTUI(experiments_dir=experiments_dir, columns=columns)
  app.run()