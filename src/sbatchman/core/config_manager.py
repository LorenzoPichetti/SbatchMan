from pathlib import Path
from typing import Dict, Any
import yaml

from sbatchman import api
from sbatchman.exceptions import ConfigurationError

def create_configs_from_file(file_path: Path, overwrite: bool = False):
  """
  Parses a YAML file and creates job configurations.

  Args:
    file_path: The path to the YAML configuration file.
    overwrite: If True, overwrite existing configurations.
  """
  try:
    with open(file_path, 'r') as f:
      data = yaml.safe_load(f)
  except FileNotFoundError:
    raise ConfigurationError(f"Configuration file not found at: {file_path}")
  except yaml.YAMLError as e:
    raise ConfigurationError(f"Error parsing YAML file: {e}")

  if not isinstance(data, dict):
    raise ConfigurationError("The root of the configuration file must be a dictionary of clusters.")

  for cluster_name, cluster_config in data.items():
    scheduler = cluster_config.get("scheduler")
    if not scheduler:
      raise ConfigurationError(f"Cluster '{cluster_name}' must have a 'scheduler' defined.")

    default_conf = cluster_config.get("default_conf", {})
    configs = cluster_config.get("configs", {})

    if not configs:
      continue

    for config_name, config_params in configs.items():
      # Merge default params with specific config params
      final_params = default_conf.copy()
      final_params.update(config_params)

      # Add common parameters
      final_params["name"] = config_name
      final_params["cluster_name"] = cluster_name
      final_params["overwrite"] = overwrite

      _create_config_from_params(scheduler, final_params)

def _create_config_from_params(scheduler: str, params: Dict[str, Any]):
  """
  Calls the appropriate API function to create a single configuration.
  """
  if scheduler == "slurm":
    api.create_slurm_config(**params)
  elif scheduler == "pbs":
    api.create_pbs_config(**params)
  elif scheduler == "local":
    api.create_local_config(**params)
  else:
    raise ConfigurationError(f"Unsupported scheduler '{scheduler}'. Supported schedulers are: slurm, pbs, local.")