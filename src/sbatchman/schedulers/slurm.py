# src/exp_kit/schedulers/slurm.py
import re
import subprocess
from typing import List, Dict, Optional, Tuple
from pathlib import Path
from dataclasses import dataclass

from .base import BaseConfig

@dataclass
class SlurmConfig(BaseConfig):
  """Scheduler for SLURM."""

  partition: Optional[str] = None
  nodes: Optional[str] = None
  ntasks: Optional[str] = None
  cpus_per_task: Optional[int] = None
  mem: Optional[str] = None
  time: Optional[str] = None
  gpus: Optional[int] = None

  def _generate_scheduler_directives(self) -> List[str]:
    lines = []
    lines.append(f"#SBATCH --job-name={self.name}")
    lines.append(f"#SBATCH --output={{EXP_DIR}}/stdout.log")
    lines.append(f"#SBATCH --error={{EXP_DIR}}/stderr.log")

    if p := self.partition: lines.append(f"#SBATCH --partition={p}")
    if n := self.nodes: lines.append(f"#SBATCH --nodes={n}")
    if t := self.ntasks: lines.append(f"#SBATCH --ntasks={t}")
    if c := self.cpus_per_task: lines.append(f"#SBATCH --cpus-per-task={c}")
    if m := self.mem: lines.append(f"#SBATCH --mem={m}")
    if t := self.time: lines.append(f"#SBATCH --time={t}")
    if g := self.gpus: lines.append(f"#SBATCH --gpus={g}")
    
    return lines

  @staticmethod
  def get_scheduler_name() -> str:
    """Returns the name of the scheduler this parameters class is associated with."""
    return "slurm"

def _parse_job_id(submission_output: str) -> str:
  """Parses the job ID from the sbatch command's output."""
  match = re.search(r"Submitted batch job (\d+)", submission_output)
  if match:
    return match.group(1)
  raise ValueError(f"Could not parse job ID from sbatch output: {submission_output}")

def slurm_submit(script_path: Path, exp_dir: Path) -> str:
  """Submits the job to SLURM."""
  command_list = ["sbatch", str(script_path)]
  result = subprocess.run(
    command_list,
    capture_output=True,
    text=True,
    check=True,
    cwd=exp_dir,
  )
  return _parse_job_id(result.stdout)