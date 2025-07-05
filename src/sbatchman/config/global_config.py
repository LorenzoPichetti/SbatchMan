from pathlib import Path
import shutil
import yaml
import platformdirs

from sbatchman.exceptions import ClusterNameNotSetError

def get_global_config_path() -> Path:
  """Returns the path to the global sbatchman config.yaml file using platformdirs."""
  config_dir = Path(platformdirs.user_config_dir('sbatchman', 'sbatchman'))
  return config_dir / "config.yaml"

def get_cluster_name() -> str:
  """Reads and returns the cluster name from the global configuration."""
  config_path = get_global_config_path()
  if not config_path.exists():
    raise ClusterNameNotSetError
  with open(config_path, 'r') as f:
    return yaml.safe_load(f).get('cluster_name', {})

def set_cluster_name(cluster_name: str):
  """Writes the cluster name to the global configuration file."""
  config_path = get_global_config_path()
  config_path.parent.mkdir(parents=True, exist_ok=True)
  config = {
    "cluster_name": cluster_name,
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
    return "slurm"
  if shutil.which("qsub"):
    return "pbs"
  return "local"