from pathlib import Path
import subprocess
from typing import List, Optional, Union
from dataclasses import dataclass

from sbatchman.core.status import Status

from .base import BaseConfig

PBS_STATUS_MAP = {
  # Queued states
  'Q': Status.QUEUED,    # Queued
  'H': Status.QUEUED,    # Held
  'T': Status.QUEUED,    # Transit (being moved)
  'W': Status.QUEUED,    # Waiting
  'S': Status.QUEUED,    # Suspended

  # Running states
  'R': Status.RUNNING,   # Running
  'E': Status.RUNNING,   # Exiting (job is finishing up)

  # Terminal state
  'C': Status.COMPLETED, # Completed
}

@dataclass
class PbsConfig(BaseConfig):
  """Config for OpenPBS."""


  queue: Optional[str] = None
  cpus: Optional[int] = None
  mem: Optional[str] = None
  walltime: Optional[str] = None

  def _generate_scheduler_directives(self) -> List[str]:
    lines = []
    lines.append(f"#PBS -N {self.name}")
    lines.append(f"#PBS -o {{EXP_DIR}}/stdout.log")
    lines.append(f"#PBS -e {{EXP_DIR}}/stderr.log")

    resources = []
    if c := self.cpus: resources.append(f"ncpus={c}")
    if m := self.mem: resources.append(f"mem={m}")
    if w := self.walltime: resources.append(f"walltime={w}")

    if resources:
      lines.append(f"#PBS -l {','.join(resources)}")

    if q := self.queue: lines.append(f"#PBS -q {q}")
    return lines
  
  @staticmethod
  def get_job_status(job_id: Union[int, str]) -> Status:
    """
    Returns the status of a PBS job.
    """
    process = subprocess.run(
      f"qstat -f {job_id}",
      shell=True,
      capture_output=True,
      text=True
    )
    returncode = process.returncode
    stdout = process.stdout.strip()
    if returncode == 0 and stdout:
      job_state = None
      exit_status = None
      for line in stdout.split('\n'):
        line = line.strip()
        if "job_state =" in line:
          job_state = line.split("=")[1].strip()
        elif "exit_status =" in line:
          exit_status = int(line.split("=")[1].strip())

      if job_state:
        # If job is completed, check exit status to determine if it failed
        if job_state == 'C':
          if exit_status is not None and exit_status != 0:
            return Status.FAILED
          return Status.COMPLETED
        
        return PBS_STATUS_MAP.get(job_state, Status.UNKNOWN)

    return Status.UNKNOWN

  @staticmethod
  def get_scheduler_name() -> str:
    """Returns the name of the scheduler this parameters class is associated with."""
    return "pbs"
  
def pbs_submit(script_path: Path, exp_dir: Path, previous_job_id: Optional[int] = None) -> int:
  """Submits the job to PBS."""
  if previous_job_id:
    command_list = ["qsub", '-W', f'depend=afterany:{previous_job_id}', str(script_path)]
  else:
    command_list = ["qsub", str(script_path)]
    
  result = subprocess.run(
    command_list,
    capture_output=True,
    text=True,
    check=True,
    cwd=exp_dir,
  )
  job_id = result.stdout.strip().split('.')[0]
  if job_id:
    return int(job_id)
  raise ValueError(f"Could not parse job ID from qsub output: {result.stdout}")