from pathlib import Path
from typing import Dict, Any, List
import yaml

from sbatchman import api
from sbatchman.exceptions import ConfigurationError
from sbatchman.schedulers.base import BaseConfig

def create_configs_from_file(file_path: Path, overwrite: bool = False) -> List[BaseConfig]:
  """
  Parses a YAML file and creates job configurations.

  Args:
    file_path: The path to the YAML configuration file.
    overwrite: If True, overwrite existing configurations.
  """
  created_configs = []

  try:
    with open(file_path, 'r') as f:
      data = yaml.safe_load(f)
  except FileNotFoundError:
    raise ConfigurationError(f"Configuration file not found at: {file_path}")
  except yaml.YAMLError as e:
    raise ConfigurationError(f"Error parsing YAML file: {e}")

  if not isinstance(data, dict):
    raise ConfigurationError("The root of the configuration file must be a dictionary of clusters.")

  for cluster_name, cluster_configs in data.items():
    scheduler = cluster_configs.get("scheduler")
    if not scheduler:
      raise ConfigurationError(f"Cluster '{cluster_name}' must have a 'scheduler' defined.")

    default_conf = cluster_configs.get("default_conf", {})
    configs = cluster_configs.get("configs", {})

    if not configs:
      continue

    for config_params in configs:
      config_name = config_params['name']

      # Merge default params with specific config params
      final_params = default_conf.copy()
      final_params.update(config_params if config_params else {})

      # Add common parameters
      final_params["name"] = config_name
      final_params["cluster_name"] = cluster_name
      final_params["overwrite"] = overwrite

      created_configs.append(_create_config_from_params(scheduler, final_params))

  return created_configs


def _create_config_from_params(scheduler: str, params: Dict[str, Any]) -> BaseConfig:
  """
  Calls the appropriate API function to create a single configuration.
  """
  if scheduler == "slurm":
    return api.create_slurm_config(**params)
  elif scheduler == "pbs":
    return api.create_pbs_config(**params)
  elif scheduler == "local":
    return api.create_local_config(**params)
  else:
    raise ConfigurationError(f"Unsupported scheduler '{scheduler}'. Supported schedulers are: slurm, pbs, local.")