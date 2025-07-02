# src/exp_kit/launcher.py
from dataclasses import dataclass
import subprocess
import datetime
from pathlib import Path
from typing import Dict, Any, Optional, TypedDict

from sbatchman.exceptions import ConfigurationError, ConfigurationNotFoundError, HostnameNotSetError
from sbatchman.config.global_config import get_hostname
from sbatchman.config.project_config import get_scheduler_from_hostname

from sbatchman.config.project_config import get_project_config_dir, get_experiments_dir
from ..schedulers.local import LocalConfig, local_submit
from ..schedulers.slurm import slurm_submit
from ..schedulers.pbs import pbs_submit
import yaml

class Job(TypedDict):
  config_name: str
  hostname: str
  timestamp: str
  exp_dir: str
  command: str
  status: str
  scheduler: str
  job_id: str
  archive_name: Optional[str]

def launch_job(config_name: str, command: str, hostname: Optional[str] = None, tag: str = "default") -> Job:
  """
  Launches an experiment based on a configuration name.
  """
  if not hostname:
    try:
      hostname = get_hostname()
    except HostnameNotSetError:
      raise ConfigurationError(
        "Hostname not specified and not set globally. "
        "Please provide --hostname or use 'sbatchman set-hostname <hostname>' to set a global default."
      )

  scheduler = get_scheduler_from_hostname(hostname)

  # Capture the Current Working Directory at the time of launch
  submission_cwd = Path.cwd()

  # 1. Resolve the config path
  config_path = get_project_config_dir() / hostname / scheduler / f"{config_name}.sh"

  if not config_path.exists():
    raise ConfigurationNotFoundError(f"Configuration '{config_name}' for hostname '{hostname}' not found at '{config_path}'.")
    
  # 2. Create a unique, nested directory for this experiment run
  timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
  # Directory structure: <hostname>/<config_name>/<tag>/<timestamp>
  # Find a directory name that has not been used yet
  base_exp_dir_local = Path(hostname) / config_name / tag / timestamp
  exp_dir_local = base_exp_dir_local
  exp_dir = get_experiments_dir() / exp_dir_local
  counter = 1
  while exp_dir.exists():
    exp_dir_local = base_exp_dir_local.with_name(f"{base_exp_dir_local.name}_{counter}")
    exp_dir = get_experiments_dir() / exp_dir_local
    counter += 1
  exp_dir.mkdir(parents=True, exist_ok=False)

  # 4. Prepare the final runnable script
  with open(config_path, "r") as f:
    template_script = f.read()
  
  # Replace placeholders for log and CWD
  final_script_content = template_script.replace(
    "{EXP_DIR}", str(exp_dir.resolve())
  ).replace(
    "{CWD}", str(submission_cwd.resolve())
  ).replace(
    "{CMD}", str(command)
  )
  
  run_script_path = exp_dir / "run.sh"
  with open(run_script_path, "w") as f:
    f.write(final_script_content)
  run_script_path.chmod(0o755)

  metadata: Job = {
    "config_name": config_name,
    "hostname": hostname,
    "timestamp": timestamp,
    "exp_dir": str(exp_dir_local),
    "command": command,
    "status": "SUBMITTING",
    "scheduler": scheduler,
    "job_id": "",
    "archive_name": None,
  }

  try:
    # 5. Submit the job using the scheduler's own logic
    job_id = None
    if scheduler == 'slurm':
      job_id = slurm_submit(run_script_path, exp_dir)
    elif scheduler == 'pbs':
      job_id = pbs_submit(run_script_path, exp_dir)
    elif scheduler == 'local':
      job_id = local_submit(run_script_path, exp_dir)
    else:
      raise ConfigurationError(f"No submission class found for scheduler '{scheduler}'. Supported schedulers are: slurm, pbs, local.")
    
    metadata["job_id"] = job_id

  except (subprocess.CalledProcessError, ValueError, FileNotFoundError) as e:
    metadata["status"] = "FAILED_SUBMISSION"
    raise
  finally:
    # 6. Save metadata
    with open(exp_dir / "metadata.yaml", "w") as f:
      yaml.safe_dump(metadata, f, default_flow_style=False)
    return metadata