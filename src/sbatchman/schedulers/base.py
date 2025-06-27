# src/exp_kit/schedulers/base.py
from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Tuple
import subprocess

class Scheduler(ABC):
  """Abstract base class for all job schedulers."""

  @abstractmethod
  def generate_script(self, name: str, **kwargs) -> str:
    """Generates the header of the submission script."""
    pass

  @abstractmethod
  def get_submit_command(self) -> str:
    """Returns the command used to submit a job (e.g., 'sbatch')."""
    pass

  @abstractmethod
  def parse_job_id(self, submission_output: str) -> str:
    """Parses the job ID from the submission command's output."""
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