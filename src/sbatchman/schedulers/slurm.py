from pathlib import Path
import re
import subprocess
from typing import List, Optional, Union
from dataclasses import dataclass

from sbatchman.core.status import Status

from .base import BaseConfig

SLURM_STATUS_MAP = {
  # Pending states
  "PENDING": Status.QUEUED,
  "CONFIGURING": Status.QUEUED,
  "REQUEUED": Status.QUEUED,
  "RESIZING": Status.QUEUED, # Often precedes running
  "SUSPENDED": Status.QUEUED,

  # Running states
  "RUNNING": Status.RUNNING,
  "COMPLETING": Status.RUNNING,

  # Terminal states (Success)
  "COMPLETED": Status.COMPLETED,

  # Terminal states (Failure)
  "FAILED": Status.FAILED,
  "NODE_FAIL": Status.FAILED,
  "PREEMPTED": Status.FAILED,
  "SPECIAL_EXIT": Status.FAILED,

  # Terminal states (Cancelled/Timeout)
  "CANCELLED": Status.CANCELLED,
  "DEADLINE": Status.TIMEOUT,
  "TIMEOUT": Status.TIMEOUT,
  "STOPPED": Status.CANCELLED,
  "REVOKED": Status.CANCELLED,
}

@dataclass
class SlurmConfig(BaseConfig):
  """Scheduler for SLURM."""

  partition: Optional[str] = None
  nodes: Optional[str] = None
  ntasks: Optional[str] = None
  cpus_per_task: Optional[int] = None
  mem: Optional[str] = None
  account: Optional[str] = None
  time: Optional[str] = None
  gpus: Optional[int] = None
  constraint: Optional[str] = None
  nodelist: Optional[str] = None
  qos: Optional[str] = None
  reservation: Optional[str] = None

  def _generate_scheduler_directives(self) -> List[str]:
    lines = []
    lines.append(f"#SBATCH --job-name={self.name}")
    lines.append(f"#SBATCH --output={{EXP_DIR}}/stdout.log")
    lines.append(f"#SBATCH --error={{EXP_DIR}}/stderr.log")

    if p := self.partition: lines.append(f"#SBATCH --partition={p}")
    if n := self.nodes: lines.append(f"#SBATCH --nodes={n}")
    if t := self.ntasks: lines.append(f"#SBATCH --ntasks={t}")
    if c := self.cpus_per_task: lines.append(f"#SBATCH --cpus-per-task={c}")
    if a := self.account: lines.append(f"#SBATCH --account={a}")
    if m := self.mem: lines.append(f"#SBATCH --mem={m}")
    if t := self.time: lines.append(f"#SBATCH --time={t}")
    if self.gpus is not None: lines.append(f"#SBATCH --gres=gpu:{self.gpus}")
    if con := self.constraint: lines.append(f"#SBATCH --constraint={con}")
    if nl := self.nodelist: lines.append(f"#SBATCH --nodelist={nl}")
    if q := self.qos: lines.append(f"#SBATCH --qos={q}")
    if r := self.reservation: lines.append(f"#SBATCH --reservation={r}")
    
    return lines
  
  @staticmethod
  def get_job_status(job_id: Union[int, str]) -> Status:
    """
    Returns the status of a SLURM job.
    """
    # TODO double check if this always works properly
    process = subprocess.run(
      f"sacct -j {job_id} -o State -D -n -u $(whoami)",
      shell=True,
      capture_output=True,
      text=True
    )
    returncode = process.returncode
    stdout = process.stdout.strip()
    
    if returncode == 0 and stdout:
      return SLURM_STATUS_MAP.get(stdout.split('\n')[0].strip(), Status.UNKNOWN)
    
    # If the job is not in the queue, it might be completed or failed.
    # The calling function will handle this logic.
    return Status.UNKNOWN

  @staticmethod
  def get_scheduler_name() -> str:
    """Returns the name of the scheduler this parameters class is associated with."""
    return "slurm"
  
def slurm_submit(script_path: Path, exp_dir: Path, previous_job_id: Optional[int] = None) -> int:
  """Submits the job to SLURM."""
  if previous_job_id:
    command_list = ["sbatch", f'--dependency=afterany:{previous_job_id}', str(script_path)]
  else:
    command_list = ["sbatch", str(script_path)]
    
  result = subprocess.run(
    command_list,
    capture_output=True,
    text=True,
    check=True,
    cwd=exp_dir,
  )

  # Parse the job ID from the sbatch output
  match = re.search(r"Submitted batch job (\d+)", result.stdout)
  if match:
    return int(match.group(1))
  raise ValueError(f"Could not parse job ID from sbatch output: {result.stdout}")