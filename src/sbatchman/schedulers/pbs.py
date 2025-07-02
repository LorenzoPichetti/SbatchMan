# src/exp_kit/schedulers/pbs.py
import re
import subprocess
from typing import List, Dict, Optional, Tuple
from pathlib import Path
from dataclasses import dataclass

from .base import BaseConfig

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
  def get_scheduler_name() -> str:
    """Returns the name of the scheduler this parameters class is associated with."""
    return "pbs"

def _parse_job_id(submission_output: str) -> str:
  """Parses the job ID from the qsub command's output."""
  job_id = submission_output.strip().split('.')[0]
  if not job_id:
    raise ValueError(f"Could not parse job ID from qsub output: {submission_output}")
  return job_id

def pbs_submit(script_path: Path, exp_dir: Path) -> str:
  """Submits the job to PBS."""
  command_list = ["qsub", str(script_path)]
  result = subprocess.run(
    command_list,
    capture_output=True,
    text=True,
    check=True,
    cwd=exp_dir,
  )
  return _parse_job_id(result.stdout)