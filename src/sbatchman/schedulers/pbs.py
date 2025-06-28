# src/exp_kit/schedulers/pbs.py
import re
import subprocess
from typing import List, Dict, Optional, Tuple

from .base import Scheduler

class PbsScheduler(Scheduler):
  """Scheduler for OpenPBS."""

  def _generate_scheduler_directives(self, name: str, **kwargs) -> List[str]:
    lines = []
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

    return lines

  def get_submit_command(self) -> str:
    return "qsub"

  def parse_job_id(self, submission_output: str) -> str:
    # qsub usually returns just the job ID
    job_id = submission_output.strip().split('.')[0]
    if not job_id:
      raise ValueError(f"Could not parse job ID from qsub output: {submission_output}")
    return job_id

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