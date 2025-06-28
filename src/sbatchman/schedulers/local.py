# src/exp_kit/schedulers/local.py
import subprocess
from typing import List, Dict, Optional, Tuple
from pathlib import Path

from .base import Scheduler

class LocalScheduler(Scheduler):
  """Scheduler for running on the local machine."""

  def generate_script(self, name: str, **kwargs) -> str:
    lines = ["#!/bin/bash", "# Local execution script"]
    
    lines.append("\n# Environment variables")
    if envs := kwargs.get("env"):
      for env_var in envs:
        lines.append(f"export {env_var}")
    
    lines.append("\n# User command")
    lines.append('CMD="$1"')
    lines.append('echo "Running command: $CMD"')
    lines.append('eval $CMD')
    return "\n".join(lines)

  def submit(self, script_path: Path, user_command: str, exp_dir: Path) -> str:
    """Runs the job in the background on the local machine."""
    stdout_log = exp_dir / "stdout.log"
    stderr_log = exp_dir / "stderr.log"
    command_list = ["bash", str(script_path), user_command]
    
    with open(stdout_log, "w") as out, open(stderr_log, "w") as err:
      process = subprocess.Popen(
        command_list,
        stdout=out,
        stderr=err,
        preexec_fn=lambda: __import__("os").setsid() # Detach from parent
      )
    return str(process.pid)
  

  def _get_status_from_scheduler(self, job_ids: List[str]) -> Dict[str, Tuple[str, Optional[str]]]:
    statuses = {}
    for pid in job_ids:
      try:
        subprocess.run(["ps", "-p", pid], check=True, capture_output=True)
        statuses[pid] = ("RUNNING", None)
      except subprocess.CalledProcessError:
        statuses[pid] = ("FINISHED", None)
    return statuses