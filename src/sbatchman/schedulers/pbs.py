# src/exp_kit/schedulers/pbs.py
import re
import subprocess
from typing import List, Dict, Optional, Tuple
from pathlib import Path

from .base import Scheduler

class PbsScheduler(Scheduler):
  """Scheduler for OpenPBS."""

  def _generate_scheduler_directives(self, name: str, **kwargs) -> List[str]:
    lines = []
    lines.append(f"#PBS -N {name}")
    lines.append(f"#PBS -o {{EXP_DIR}}/stdout.log")
    lines.append(f"#PBS -e {{EXP_DIR}}/stderr.log")

    resources = []
    if c := kwargs.get("cpus"): resources.append(f"ncpus={c}")
    if m := kwargs.get("mem"): resources.append(f"mem={m}")
    if w := kwargs.get("walltime"): resources.append(f"walltime={w}")

    if resources:
      lines.append(f"#PBS -l {','.join(resources)}")

    if q := kwargs.get("queue"): lines.append(f"#PBS -q {q}")
    return lines

  def _parse_job_id(self, submission_output: str) -> str:
    """Parses the job ID from the qsub command's output."""
    job_id = submission_output.strip().split('.')[0]
    if not job_id:
      raise ValueError(f"Could not parse job ID from qsub output: {submission_output}")
    return job_id

  def submit(self, script_path: Path, exp_dir: Path) -> str:
    """Submits the job to PBS."""
    command_list = ["qsub", str(script_path)]
    result = subprocess.run(
      command_list,
      capture_output=True,
      text=True,
      check=True,
      cwd=exp_dir,
    )
    return self._parse_job_id(result.stdout)