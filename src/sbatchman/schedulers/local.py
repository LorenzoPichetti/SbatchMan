from dataclasses import dataclass
from pathlib import Path
import os
import signal
import subprocess
import datetime
import yaml
from typing import List, Optional, Tuple, Union

from sbatchman.core.status import Status

from .base import BaseConfig

@dataclass
class LocalConfig(BaseConfig):
  """Scheduler for running on the local machine."""
  
  scheduler: str = 'local'
  time: Optional[str] = None

  def _generate_scheduler_directives(self) -> List[str]:
    return ["# Local execution script"]

  @staticmethod
  def get_job_status(job_id: Union[str, int]) -> Status:
    """
    For local jobs, status is not tracked post-submission.
    """
    return Status.UNKNOWN

  @staticmethod
  def get_scheduler_name() -> str:
    """Returns the name of the scheduler this parameters class is associated with."""
    return "local"

  def cancel_job(self, job_id: Union[str, int]) -> None:
    pid = int(job_id)
    try:
      # New local jobs run in their own session so their shell and child
      # processes can all be stopped together. Fall back to the shell PID for
      # jobs started by older SbatchMan versions.
      if os.getpgid(pid) == pid:
        os.killpg(pid, signal.SIGTERM)
      else:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
      return  # The process finished between status refresh and cancellation.
  
  def local_submit(self, script_path: Path, exp_dir: Path) -> Tuple[int, bool]:
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

    process = None
    previous_handlers = {}

    def interrupt(signum, _frame):
      raise InterruptedError(f"local job interrupted by signal {signum}")

    # A local job runs in a separate process group, so terminal signals sent
    # to the CLI do not reach it automatically. Convert common termination
    # signals into an exception so we can stop that whole group and persist a
    # terminal status before returning control to the CLI.
    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
      previous_handlers[sig] = signal.getsignal(sig)
      signal.signal(sig, interrupt)

    try:
      with open(stdout_log, "w") as out, open(stderr_log, "w") as err:
        process = subprocess.Popen(
          command_list,
          stdout=out,
          stderr=err,
          start_new_session=True,
        )
        process.wait()
        # If timeout was used, check if process exited with 124 (timeout exit code)
        if self.time and process.returncode == 124:
          timed_out = True
    except BaseException:
      if process is not None:
        try:
          os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
          pass
        if process.poll() is None:
          try:
            process.wait(timeout=5)
          except subprocess.TimeoutExpired:
            try:
              os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
              pass
            process.wait()
        self._mark_interrupted(exp_dir)
      raise
    finally:
      for sig, handler in previous_handlers.items():
        signal.signal(sig, handler)

    return process.pid, timed_out

  @staticmethod
  def _mark_interrupted(exp_dir: Path) -> None:
    metadata_path = exp_dir / "metadata.yaml"
    try:
      with open(metadata_path, "r", encoding="utf-8") as metadata_file:
        metadata = yaml.safe_load(metadata_file) or {}
      metadata["status"] = Status.CANCELLED.value
      metadata["exitcode"] = -int(signal.SIGTERM)
      metadata["end_timestamp"] = datetime.datetime.now().strftime("%Y%m%d_%H%M%S.%f")
      with open(metadata_path, "w", encoding="utf-8") as metadata_file:
        yaml.safe_dump(metadata, metadata_file, sort_keys=False)
    except (OSError, yaml.YAMLError):
      # Preserve the original signal/exception; metadata may be unavailable
      # during early submission failures.
      pass
