from dataclasses import dataclass
from pathlib import Path
import subprocess
from typing import List, Optional, Union

from sbatchman.core.status import Status

from .base import BaseConfig

@dataclass
class LocalConfig(BaseConfig):
  """Scheduler for running on the local machine."""
  
  time: Optional[str] = None

  def _generate_scheduler_directives(self) -> List[str]:
    return ["# Local execution script"]

  @staticmethod
  def get_job_status(job_id: Union[int, str]) -> Status:
    """
    For local jobs, status is not tracked post-submission.
    """
    return Status.UNKNOWN

  @staticmethod
  def get_scheduler_name() -> str:
    """Returns the name of the scheduler this parameters class is associated with."""
    return "local"
  
  def local_submit(self, script_path: Path, exp_dir: Path) -> tuple[int, bool]:
    """Runs the job in the background on the local machine, with optional time limit.
    Returns (pid, timed_out: bool).
    """
    stdout_log = exp_dir / "stdout.log"
    stderr_log = exp_dir / "stderr.log"
    command_list = ["bash", str(script_path)]
    timed_out = False

    # If self.time is set, prepend 'timeout' to the command
    if self.time:
      # Convert D-HH:mm:ss to seconds if needed
      def parse_time(t: str) -> int:
        if '-' in t:
          days, rest = t.split('-')
          h, m, s = map(int, rest.split(':'))
          return int(days) * 86400 + h * 3600 + m * 60 + s
        else:
          h, m, s = map(int, t.split(':'))
          return h * 3600 + m * 60 + s
      timeout_seconds = parse_time(self.time)
      command_list = ["timeout", str(timeout_seconds)] + command_list

    with open(stdout_log, "w") as out, open(stderr_log, "w") as err:
      process = subprocess.Popen(
        command_list,
        stdout=out,
        stderr=err,
      )
      process.wait()
      # If timeout was used, check if process exited with 124 (timeout exit code)
      if self.time and process.returncode == 124:
        timed_out = True

    return process.pid, timed_out