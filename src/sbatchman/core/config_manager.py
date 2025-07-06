from pathlib import Path
from typing import Dict, Any, List, Optional
import yaml

from sbatchman.config.global_config import get_cluster_name
from sbatchman.exceptions import ConfigurationError
from typing import TYPE_CHECKING
from sbatchman.schedulers.base import BaseConfig
from sbatchman.schedulers.local import LocalConfig
from sbatchman.schedulers.pbs import PbsConfig
from sbatchman.schedulers.slurm import SlurmConfig

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
  params = { k.replace('-', '_'): v for k,v in params.items() }
  if scheduler == "slurm":
    return create_slurm_config(**params)
  elif scheduler == "pbs":
    return create_pbs_config(**params)
  elif scheduler == "local":
    return create_local_config(**params)
  else:
    raise ConfigurationError(f"Unsupported scheduler '{scheduler}'. Supported schedulers are: slurm, pbs, local.")
  
def create_local_config(
  name: str,
  cluster_name: Optional[str] = None,
  env: Optional[List[str]] = None,
  modules: Optional[List[str]] = None,
  overwrite: bool = False,
) -> LocalConfig:
  """Creates and saves a configuration file for local execution.

  Args:
    name: The name of the configuration.
    cluster_name: The name of the cluster this configuration belongs to.
      Defaults to the system's hostname.
    env: A list of environment variables to set.
    modules: A list of environment modules to load (for compatibility,
      though they may not be used in a standard local shell).
    overwrite: If True, overwrite an existing configuration with the same name.

  Returns:
    The path to the newly created configuration file.
  """
  config = LocalConfig(name=name, cluster_name=cluster_name if cluster_name else get_cluster_name(), env=env, modules=modules)
  config.save_config(overwrite)
  return config

def create_pbs_config(
  name: str,
  cluster_name: Optional[str] = None,
  queue: Optional[str] = None,
  cpus: Optional[int] = None,
  mem: Optional[str] = None,
  walltime: Optional[str] = None,
  env: Optional[List[str]] = None,
  modules: Optional[List[str]] = None,
  overwrite: bool = False,
) -> PbsConfig:
  """Creates and saves a PBS configuration file.

  Args:
    name: The name of the configuration.
    cluster_name: The name of the cluster this configuration belongs to.
      Defaults to the system's hostname.
    queue: The PBS queue to submit the job to.
    cpus: The number of CPUs to request.
    mem: The amount of memory to request (e.g., "16gb", "100mb").
    walltime: The maximum wall time for the job (e.g., "24:00:00").
    env: A list of environment variables to set.
    modules: A list of environment modules to load.
    overwrite: If True, overwrite an existing configuration with the same name.

  Returns:
    The path to the newly created configuration file.
  """
  config = PbsConfig(
    name=name, cluster_name=cluster_name if cluster_name else get_cluster_name(), queue=queue, cpus=cpus, mem=mem, walltime=walltime, env=env, modules=modules
  )
  config.save_config(overwrite)
  return config

def create_slurm_config(
  name: str,
  cluster_name: Optional[str] = None,
  partition: Optional[str] = None,
  nodes: Optional[str] = None,
  ntasks: Optional[str] = None,
  cpus_per_task: Optional[int] = None,
  mem: Optional[str] = None,
  account: Optional[str] = None,
  time: Optional[str] = None,
  gpus: Optional[int] = None,
  constraint: Optional[str] = None,
  nodelist: Optional[str] = None,
  qos: Optional[str] = None,
  reservation: Optional[str] = None,
  env: Optional[List[str]] = None,
  modules: Optional[List[str]] = None,
  overwrite: bool = False,
) -> SlurmConfig:
  """Creates and saves a SLURM configuration file.

  Args:
    name: The name of the configuration.
    cluster_name: The name of the cluster this configuration belongs to.
      Defaults to the system's hostname.
    partition: The SLURM partition (queue) to submit the job to.
    nodes: The number of nodes to request.
    ntasks: The number of tasks to run.
    cpus_per_task: The number of CPUs to request per task.
    mem: The amount of memory to request (e.g., "16G", "100M").
    account: The account to charge for the job.
    time: The maximum wall time for the job (e.g., "01-00:00:00").
    gpus: The number of GPUs to request.
    constraint: Specific features required for the job's nodes.
    nodelist: A specific list of nodes to use.
    qos: The Quality of Service for the job.
    reservation: The reservation to use for the job.
    env: A list of environment variables to set.
    modules: A list of environment modules to load.
    overwrite: If True, overwrite an existing configuration with the same name.

  Returns:
    The path to the newly created configuration file.
  """
  config = SlurmConfig(
    name=name, cluster_name=cluster_name if cluster_name else get_cluster_name(), 
    partition=partition, nodes=nodes, ntasks=ntasks, cpus_per_task=cpus_per_task, mem=mem, account=account,
    time=time, gpus=gpus, constraint=constraint, nodelist=nodelist, qos=qos, reservation=reservation,
    env=env, modules=modules
  )
  config.save_config(overwrite)
  return config