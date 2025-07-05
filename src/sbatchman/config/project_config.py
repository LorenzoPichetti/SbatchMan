from pathlib import Path
from typing import Optional
import yaml

from sbatchman.exceptions import ConfigurationError, ProjectExistsError, ProjectNotInitializedError

# The name of the root directory to search for.
PROJECT_ROOT_DIR_NAME = "SbatchMan"

_cached_sbatchman_home: Optional[Path] = None

def init_project(path: Path):
  """Initializes a new SbatchMan root directory."""
  project_dir = path / PROJECT_ROOT_DIR_NAME
  if project_dir.exists():
    raise ProjectExistsError()
  
  configs_dir = project_dir / "configs"
  configs_dir.mkdir(parents=True, exist_ok=True)
  (project_dir / "experiments").mkdir(exist_ok=True)

  main_config_path = configs_dir / "configurations.yaml"
  if not main_config_path.exists():
    with open(main_config_path, "w") as f:
      yaml.dump({}, f)

def get_project_root() -> Path:
  """
  Searches for the project root directory (SbatchMan) upwards from the CWD.
  """
  global _cached_sbatchman_home
  if _cached_sbatchman_home is not None:
      return _cached_sbatchman_home

  current_dir = Path.cwd()
  home_dir = Path.home()

  # Search upwards from CWD to home directory
  while current_dir != home_dir and current_dir.parent != current_dir:
    project_dir = current_dir / PROJECT_ROOT_DIR_NAME
    if project_dir.is_dir():
      _cached_sbatchman_home = project_dir
      return project_dir
    current_dir = current_dir.parent

  # Check home directory as the last stop
  home_project_dir = home_dir / PROJECT_ROOT_DIR_NAME
  if home_project_dir.is_dir():
    _cached_sbatchman_home = home_project_dir
    return home_project_dir
  
  # If not found anywhere, raise an error.
  raise ProjectNotInitializedError()


def get_project_config_dir() -> Path:
  """Returns the path to the configuration directory."""
  path = get_project_root() / "configs"
  path.mkdir(parents=True, exist_ok=True)
  return path

def get_project_configs_file_path() -> Path:
  """Returns the path to the main configurations.yaml file."""
  return get_project_root() / "configs" / "configurations.yaml"

def get_experiments_dir() -> Path:
  """Returns the path to the experiments directory."""
  path = get_project_root() / "experiments"
  path.mkdir(parents=True, exist_ok=True)
  return path

def get_scheduler_from_cluster_name(cluster_name: str) -> str:
  """
  Detects the scheduler type based on the cluster name, as stored in the project configuration.
  Returns the scheduler name as a string.
  """
  config_path = get_project_configs_file_path()
  
  if not config_path.exists():
    raise ConfigurationError(f"Project configuration file not found.")
  
  with open(config_path, 'r') as f:
    all_configs = yaml.safe_load(f) or {}
  
  if cluster_name not in all_configs:
    raise ConfigurationError(f"No configurations found for cluster '{cluster_name}'.")
  
  return all_configs[cluster_name].get('scheduler', '')

def get_archive_dir() -> Path:
  """Returns the path to the archive directory."""
  archive_dir = get_project_root() / "archive"
  archive_dir.mkdir(exist_ok=True)
  return archive_dir