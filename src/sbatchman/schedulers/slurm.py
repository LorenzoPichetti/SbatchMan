# src/exp_kit/schedulers/slurm.py
import re
import subprocess
from typing import List, Dict, Optional, Tuple
from pathlib import Path

from .base import Scheduler

class SlurmScheduler(Scheduler):
  """Scheduler for SLURM."""

  def _generate_scheduler_directives(self, name: str, **kwargs) -> List[str]:
    lines = []
    lines.append(f"#SBATCH --job-name={name}")
    lines.append(f"#SBATCH --output={{EXP_DIR}}/stdout.log")
    lines.append(f"#SBATCH --error={{EXP_DIR}}/stderr.log")

    if p := kwargs.get("partition"): lines.append(f"#SBATCH --partition={p}")
    if n := kwargs.get("nodes"): lines.append(f"#SBATCH --nodes={n}")
    if t := kwargs.get("ntasks"): lines.append(f"#SBATCH --ntasks={t}")
    if c := kwargs.get("cpus_per_task"): lines.append(f"#SBATCH --cpus-per-task={c}")
    if m := kwargs.get("mem"): lines.append(f"#SBATCH --mem={m}")
    if t := kwargs.get("time"): lines.append(f"#SBATCH --time={t}")
    if g := kwargs.get("gpus"): lines.append(f"#SBATCH --gpus={g}")
    
    return lines


  def _parse_job_id(self, submission_output: str) -> str:
    """Parses the job ID from the sbatch command's output."""
    match = re.search(r"Submitted batch job (\d+)", submission_output)
    if match:
      return match.group(1)
    raise ValueError(f"Could not parse job ID from sbatch output: {submission_output}")

  def submit(self, script_path: Path, exp_dir: Path) -> str:
    """Submits the job to SLURM."""
    command_list = ["sbatch", str(script_path)]
    result = subprocess.run(
      command_list,
      capture_output=True,
      text=True,
      check=True,
      cwd=exp_dir,
    )
    return self._parse_job_id(result.stdout)