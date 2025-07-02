from pathlib import Path
import shutil
from typing import Dict, Any
import yaml
import socket
import platformdirs

from sbatchman.exceptions import HostnameNotSetError, ProjectNotInitializedError
from sbatchman.schedulers.local import LocalConfig
from sbatchman.schedulers.pbs import PbsConfig
from .schedulers.slurm import SlurmConfig

# --- Global Configuration Functions ---

def get_global_config_path() -> Path:
  """Returns the path to the global sbatchman config.yaml file using platformdirs."""
  config_dir = Path(platformdirs.user_config_dir('sbatchman', 'sbatchman'))
  return config_dir / "config.yaml"

def get_hostname() -> str:
  """Reads and returns the hostname from the global configuration."""
  config_path = get_global_config_path()
  if not config_path.exists():
    raise HostnameNotSetError
  with open(config_path, 'r') as f:
    return yaml.safe_load(f).get('hostname', {})

def set_hostname(hostname: str):
  """Writes the hostname to the global configuration file."""
  config_path = get_global_config_path()
  config_path.parent.mkdir(parents=True, exist_ok=True)
  config = {
    "hostname": hostname,
  }
  with open(config_path, 'w') as f:
    yaml.dump(config, f, default_flow_style=False, sort_keys=False)

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
    return SlurmConfig.get_scheduler_name()
  if shutil.which("qsub"):
    return PbsConfig.get_scheduler_name()
  return LocalConfig.get_scheduler_name()