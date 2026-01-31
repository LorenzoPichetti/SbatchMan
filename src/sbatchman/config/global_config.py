from pathlib import Path
import shutil
from typing import Optional
import yaml
import platformdirs

from sbatchman.exceptions import ClusterNameNotSetError

def get_global_config_path() -> Path:
  """Returns the path to the global sbatchman config.yaml file using platformdirs."""
  config_dir = Path(platformdirs.user_config_dir('sbatchman', 'sbatchman'))
  return config_dir / "config.yaml"

def _load_global_config() -> dict:
  """Loads the global configuration file and returns its contents as a dict."""
  config_path = get_global_config_path()
  if not config_path.exists():
    return {}
  with open(config_path, 'r') as f:
    return yaml.safe_load(f) or {}

def _save_global_config(config: dict):
  """Saves the given configuration dict to the global config file."""
  config_path = get_global_config_path()
  config_path.parent.mkdir(parents=True, exist_ok=True)
  with open(config_path, 'w') as f:
    yaml.dump(config, f, default_flow_style=False, sort_keys=False)

def get_cluster_name() -> str:
  """Reads and returns the cluster name from the global configuration.
  
  Raises:
    ClusterNameNotSetError: If the cluster name is not set in the config file.
  """
  config_path = get_global_config_path()
  if not config_path.exists():
    raise ClusterNameNotSetError
  with open(config_path, 'r') as f:
    return yaml.safe_load(f).get('cluster_name', {})

def set_cluster_name(cluster_name: str):
  """Writes the cluster name to the global configuration file."""
  config = _load_global_config()
  config["cluster_name"] = cluster_name
  _save_global_config(config)

def get_max_queued_jobs() -> Optional[int]:
  """Returns the maximum number of queued jobs allowed, or None if unlimited."""
  config = _load_global_config()
  return config.get('max_queued_jobs', None)

def set_max_queued_jobs(max_jobs: Optional[int]):
  """Sets the maximum number of queued jobs allowed. Pass None to disable the limit."""
  config = _load_global_config()
  if max_jobs is None:
    config.pop('max_queued_jobs', None)
  else:
    config['max_queued_jobs'] = max_jobs
  _save_global_config(config)

def ensure_global_config_exists():
  """
  Checks if the global config file exists.
  """
  config_path = get_global_config_path()
  return config_path.exists()

def detect_scheduler() -> str:
  """
  Detects the available job scheduler by checking for common commands.
  
  Returns:
    The name of the detected scheduler class.
  """
  if shutil.which("sbatch"):
    return "slurm"
  if shutil.which("qsub"):
    return "pbs"
  return "local"