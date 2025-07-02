# src/exp_kit/schedulers/local.py
from dataclasses import dataclass
import subprocess
from typing import List, Dict, Optional, Tuple
from pathlib import Path

from .base import BaseConfig

@dataclass
class LocalConfig(BaseConfig):
  """Scheduler for running on the local machine."""

  def _generate_scheduler_directives(self) -> List[str]:
    return ["# Local execution script"]
  
  @staticmethod
  def get_scheduler_name() -> str:
    """Returns the name of the scheduler this parameters class is associated with."""
    return "local"

def local_submit(script_path: Path, exp_dir: Path) -> str:
  """Runs the job in the background on the local machine."""
  stdout_log = exp_dir / "stdout.log"
  stderr_log = exp_dir / "stderr.log"
  command_list = ["bash", str(script_path)]
  
  with open(stdout_log, "w") as out, open(stderr_log, "w") as err:
    process = subprocess.Popen(
      command_list,
      stdout=out,
      stderr=err,
      preexec_fn=lambda: __import__("os").setsid() # Detach from parent
    )
  return str(process.pid)