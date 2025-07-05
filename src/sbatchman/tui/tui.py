from textual.app import App
from pathlib import Path
from typing import Optional

from sbatchman.config.project_config import get_experiments_dir
from sbatchman.tui.jobs_screen import JobsScreen

class ExperimentTUI(App):
  TITLE = "SbatchMan Status"
  CSS_PATH = "style.tcss"
  
  def __init__(self, experiments_dir: Optional[Path] = None, **kwargs):
    super().__init__(**kwargs)
    self.animation_level = "none"
    self.experiments_root = experiments_dir or get_experiments_dir()

  def on_mount(self) -> None:
    self.push_screen(JobsScreen(experiments_dir=self.experiments_root))

def run_tui(experiments_dir: Optional[Path] = None):
  app = ExperimentTUI(experiments_dir=experiments_dir)
  app.run()