# src/exp_kit/schedulers/pbs.py
import re
import subprocess
from typing import List, Dict, Optional, Tuple
from pathlib import Path

from .base import Scheduler

class PbsScheduler(Scheduler):
  """Scheduler for OpenPBS/Torque."""

  def generate_script(self, name: str, **kwargs) -> str:
    lines = ["#!/bin/bash"]
    lines.append(f"#PBS -N {name}")
    lines.append(f"#PBS -o {{LOG_DIR}}/pbs-${{PBS_JOBID}}.out")
    lines.append(f"#PBS -e {{LOG_DIR}}/pbs-${{PBS_JOBID}}.err")

    resources = []
    if c := kwargs.get("cpus"): resources.append(f"ncpus={c}")
    if m := kwargs.get("mem"): resources.append(f"mem={m}")
    if w := kwargs.get("walltime"): resources.append(f"walltime={w}")

    if resources:
      lines.append(f"#PBS -l {','.join(resources)}")

    if q := kwargs.get("queue"): lines.append(f"#PBS -q {q}")

    lines.append("\n# Environment variables")
    if envs := kwargs.get("env"):
      for env_var in envs:
        lines.append(f"export {env_var}")

    lines.append("\n# Change to the submission directory")
    lines.append("cd $PBS_O_WORKDIR")
    
    lines.append("\n# User command")
    lines.append('CMD="$1"')
    lines.append('echo "Running command: $CMD"')
    lines.append('eval $CMD')
    return "\n".join(lines)

  def _parse_job_id(self, submission_output: str) -> str:
    """Parses the job ID from the qsub command's output."""
    job_id = submission_output.strip().split('.')[0]
    if not job_id:
      raise ValueError(f"Could not parse job ID from qsub output: {submission_output}")
    return job_id

  def submit(self, script_path: Path, user_command: str, exp_dir: Path) -> str:
    """Submits the job to PBS."""
    command_list = ["qsub", str(script_path), user_command]
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

    cmd = ["qstat", "-f"] + job_ids
    result = subprocess.run(cmd, capture_output=True, text=True) # Don't check=True, qstat errors if job is not found
    
    statuses = {}
    qstat_map = {
      "Q": "QUEUED",
      "R": "RUNNING",
      "E": "RUNNING", # Exiting
      "C": "FINISHED",
      "H": "QUEUED", # Held
    }

    # qstat -f output is multiline per job
    # A simple regex can find the job_state for each job
    for job_block in result.stdout.split("Job Id:"):
      if not job_block.strip():
        continue
      
      job_id_match = re.search(r"([\w.-]+)", job_block)
      if not job_id_match:
        continue
      
      job_id = job_id_match.group(1).split('.')[0]

      state_match = re.search(r"job_state\s*=\s*(\w)", job_block)
      if state_match:
        state = state_match.group(1)
        status = qstat_map.get(state, "UNKNOWN")
        statuses[job_id] = (status, None) # PBS doesn't easily give queue position

    return statuses