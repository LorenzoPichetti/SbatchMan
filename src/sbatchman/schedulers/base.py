# src/exp_kit/schedulers/base.py
from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Tuple
from pathlib import Path
import subprocess

class Scheduler(ABC):
  """Abstract base class for all job schedulers."""

  @abstractmethod
  def generate_script(self, name: str, **kwargs) -> str:
    """Generates the header of the submission script."""
    pass

  @abstractmethod
  def submit(self, script_path: Path, user_command: str, exp_dir: Path) -> str:
    """
    Submits the job to the scheduler and returns the job ID.
    This method contains all logic for running the submission command.

    Args:
      script_path: The path to the executable bash script.
      user_command: The user's command to be passed to the script.
      exp_dir: The directory for the experiment's logs.

    Returns:
      The job ID as a string.
    """
    pass

  def get_status(self, job_ids: List[str]) -> Dict[str, Tuple[str, Optional[str]]]:
    """
    Checks the status of a list of job IDs.

    Returns:
      A dictionary mapping job_id to a tuple of (status, queue_info).
      Status can be: QUEUED, RUNNING, FINISHED, FAILED, UNKNOWN.
      queue_info can be the position in queue, or None.
    """
    if not job_ids:
      return {}
    try:
      return self._get_status_from_scheduler(job_ids)
    except (subprocess.CalledProcessError, FileNotFoundError):
      return {job_id: ("UNKNOWN", None) for job_id in job_ids}

  @abstractmethod
  def _get_status_from_scheduler(self, job_ids: List[str]) -> Dict[str, Tuple[str, Optional[str]]]:
    """Scheduler-specific implementation for checking job status."""
    pass