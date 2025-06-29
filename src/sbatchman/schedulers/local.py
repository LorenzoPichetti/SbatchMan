# src/exp_kit/schedulers/local.py
import subprocess
from typing import List, Dict, Optional, Tuple
from pathlib import Path

from .base import Scheduler

class LocalScheduler(Scheduler):
  """Scheduler for running on the local machine."""

  def _generate_scheduler_directives(self, name: str, **kwargs) -> List[str]:
    return ["# Local execution script"]

  def submit(self, script_path: Path, exp_dir: Path) -> str:
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