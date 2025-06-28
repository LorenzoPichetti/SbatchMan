# src/exp_kit/schedulers/local.py
import subprocess
from typing import List, Dict, Optional, Tuple

from .base import Scheduler

class LocalScheduler(Scheduler):
  """Scheduler for running on the local machine."""

  def _generate_scheduler_directives(self, name: str, **kwargs) -> List[str]:
    return ["# Local execution script"]
  
  def get_submit_command(self) -> str:
    # For local, we run bash in the background. The launcher will handle logs.
    return "bash"

  def parse_job_id(self, submission_output: str) -> str:
    # For local jobs, we can use the process ID (PID) as the job_id
    return submission_output.strip()

  def _get_status_from_scheduler(self, job_ids: List[str]) -> Dict[str, Tuple[str, Optional[str]]]:
    # A simple check if the process (PID) is running
    statuses = {}
    for pid in job_ids:
      try:
        # Use ps to check for the process. If it doesn't exist, ps errors.
        subprocess.run(["ps", "-p", pid], check=True, capture_output=True)
        statuses[pid] = ("RUNNING", None)
      except subprocess.CalledProcessError:
        # If ps fails, we assume the job is finished.
        # A more robust check could inspect the exit code file if we created one.
        statuses[pid] = ("FINISHED", None)
    return statuses