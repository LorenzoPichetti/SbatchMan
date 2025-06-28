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
    lines.append(f"#SBATCH --output={{LOG_DIR}}/slurm-%j.out")

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

  def submit(self, script_path: Path, user_command: str, exp_dir: Path) -> str:
    """Submits the job to SLURM."""
    command_list = ["sbatch", str(script_path), user_command]
    result = subprocess.run(
      command_list,
      capture_output=True,
      text=True,
      check=True,
      cwd=exp_dir,
    )
    return self._parse_job_id(result.stdout)

  def _get_status_from_scheduler(self, job_ids: List[str]) -> Dict[str, Tuple[str, Optional[str]]]:
    if not job_ids:
      return {}
    
    cmd = ["squeue", "-h", "-o", "%i %t %r", "-j", ",".join(job_ids)]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    
    statuses = {}
    squeue_map = {
      "PD": "QUEUED",
      "R": "RUNNING",
      "CG": "RUNNING", # Completing
      "CD": "FINISHED",
      "F": "FAILED",
      "TO": "FAILED", # Timeout
      "CA": "CANCELLED",
      "NF": "FAILED", # Node Failure
    }
    
    for line in result.stdout.strip().split('\n'):
      if not line:
        continue
      parts = line.split()
      job_id, state, reason = parts[0], parts[1], " ".join(parts[2:])
      status = squeue_map.get(state, "UNKNOWN")
      queue_info = reason if status == "QUEUED" else None
      statuses[job_id] = (status, queue_info)
        
    return statuses