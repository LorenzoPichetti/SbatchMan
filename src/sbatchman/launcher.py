# src/exp_kit/launcher.py
import json
import subprocess
import datetime
from pathlib import Path
from typing import Dict, Any, Optional

from .config import EXPERIMENTS_DIR, CONFIG_DIR
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


def launch_experiment(config_name: str, command: str, comment: str):
  """
  Launches an experiment based on a configuration.
  """
  config_path = CONFIG_DIR / f"{config_name}.sh"
  if not config_path.exists():
    raise FileNotFoundError(f"Configuration '{config_name}' not found at {config_path}")

  # 1. Create a unique directory for this experiment run
  timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
  config_exp_dir = EXPERIMENTS_DIR / config_name
  exp_dir = config_exp_dir / timestamp
  exp_dir.mkdir(parents=True, exist_ok=True)

  # 2. Identify the scheduler
  scheduler = get_scheduler_from_config(config_path)

  # 3. Prepare the final runnable script
  with open(config_path, "r") as f:
    template_script = f.read()
  
  # Replace placeholders for log directories
  final_script_content = template_script.replace("{LOG_DIR}", str(exp_dir.resolve()))
  
  run_script_path = exp_dir / "run.sh"
  with open(run_script_path, "w") as f:
    f.write(final_script_content)
  run_script_path.chmod(0o755)

  # 4. Submit the job
  submit_cmd = scheduler.get_submit_command()
  job_id = None

  stdout_log = exp_dir / "stdout.log"
  stderr_log = exp_dir / "stderr.log"

  metadata: Dict[str, Any] = {
    "name": config_name,
    "timestamp": timestamp,
    "exp_dir": str(exp_dir),
    "command": command,
    "comment": comment,
    "job_id": None,
    "status": "SUBMITTING",
    "scheduler": scheduler.__class__.__name__,
    "queue_info": None,
  }

  try:
    if isinstance(scheduler, LocalScheduler):
      # For local, run in background and redirect output
      with open(stdout_log, "w") as out, open(stderr_log, "w") as err:
        process = subprocess.Popen(
          [str(run_script_path), command],
          stdout=out,
          stderr=err,
          preexec_fn=lambda: __import__("os").setsid() # a bit of a hack to detatch process
        )
      job_id = str(process.pid)
      metadata["status"] = "RUNNING"
    else:
      # For clusters, submit and capture job ID
      result = subprocess.run(
        [submit_cmd, str(run_script_path), command],
        capture_output=True, text=True, check=True,
        cwd=exp_dir # Run from experiment dir to catch scheduler logs
      )
      job_id = scheduler.parse_job_id(result.stdout)
      metadata["status"] = "QUEUED"

    metadata["job_id"] = job_id
    print(f"✅ Experiment for config '{config_name}' submitted successfully.")
    print(f"   Job ID: {job_id}")
    print(f"   Logs: {exp_dir}")


  except (subprocess.CalledProcessError, ValueError, FileNotFoundError) as e:
    metadata["status"] = "FAILED_SUBMISSION"
    print(f"❌ Failed to submit experiment for config '{config_name}'.")
    print(f"   Error: {e}")
  finally:
    # 5. Save metadata
    with open(exp_dir / "metadata.json", "w") as f:
      json.dump(metadata, f, indent=4)