# src/exp_kit/launcher.py
import json
import subprocess
import datetime
from pathlib import Path
from typing import Dict, Any, Optional

from .config import get_config_dir, get_experiments_dir
from .schedulers.local import LocalScheduler
from .schedulers.slurm import SlurmScheduler
from .schedulers.pbs import PbsScheduler
from .schedulers.base import Scheduler

SCHEDULER_MAP = {
  "#SBATCH": SlurmScheduler(),
  "#PBS": PbsScheduler(),
}

def get_scheduler_from_config(config_path: Path) -> Scheduler:
  """Detects the scheduler from the config file's header."""
  with open(config_path, "r") as f:
    for line in f:
      for directive, scheduler_class in SCHEDULER_MAP.items():
        if line.strip().startswith(directive):
          return scheduler_class
  # Default to local if no other scheduler directive is found
  return LocalScheduler()

def launch_experiment(config_path_or_name: str, command: str):
  """
  Launches an experiment based on a configuration name or path.
  """
  # Capture the Current Working Directory at the time of launch
  submission_cwd = Path.cwd()
  
  # 1. Resolve the config path
  config_path = Path(config_path_or_name)
  if not config_path.is_file():
    # It's not a direct path, so assume it's a name and search for it.
    config_path = get_config_dir() / f"{config_path_or_name}.sh"

  if not config_path.exists():
    raise FileNotFoundError(f"Configuration '{config_path_or_name}' not found at {config_path}")
  
  config_name = config_path.stem

  # 2. Create a unique, nested directory for this experiment run
  timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
  # Use the auto-detected or default experiments directory
  experiments_root = get_experiments_dir()
  exp_dir_local = Path(config_name) / timestamp
  exp_dir = experiments_root / exp_dir_local
  exp_dir.mkdir(parents=True, exist_ok=True)

  # 3. Identify the scheduler
  scheduler = get_scheduler_from_config(config_path)

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
  
  metadata: Dict[str, Any] = {
    "name": config_name,
    "timestamp": timestamp,
    "exp_dir": exp_dir_local.__str__(),
    "command": command,
    "status": "SUBMITTING",
    "scheduler": scheduler.__class__.__name__,
  }

  try:
    # 5. Submit the job using the scheduler's own logic
    job_id = scheduler.submit(run_script_path, exp_dir)
    
    metadata["job_id"] = job_id
    # Set initial status based on scheduler type
    if isinstance(scheduler, LocalScheduler):
      metadata["status"] = "RUNNING"
    else:
      metadata["status"] = "QUEUED"

    print(f"✅ Experiment for config '{config_name}' submitted successfully.")
    print(f"   Job ID: {job_id}")
    print(f"   Logs: {exp_dir}")

  except (subprocess.CalledProcessError, ValueError, FileNotFoundError) as e:
    metadata["status"] = "FAILED_SUBMISSION"
    print(f"❌ Failed to submit experiment for config '{config_name}'.")
    print(f"   Error: {e}")
  finally:
    # 6. Save metadata
    with open(exp_dir / "metadata.json", "w") as f:
      json.dump(metadata, f, indent=4)