from copy import deepcopy
from pathlib import Path
import yaml
import itertools
import re

from sbatchman.config.global_config import get_cluster_name
from sbatchman.config.project_config import get_project_configs_file_path
from sbatchman.exceptions import ConfigurationError
from typing import TYPE_CHECKING, Any, List, Optional, Union
from sbatchman.schedulers.base import BaseConfig
from sbatchman.schedulers.local import LocalConfig
from sbatchman.schedulers.pbs import PbsConfig
from sbatchman.schedulers.slurm import SlurmConfig

def _load_variable_values(var_value):
  # If var_value is a list, return as is
  if isinstance(var_value, list):
    return var_value
  elif isinstance(var_value, str):
    path = Path(var_value)
    if path.is_file():
      with open(path, "r") as f:
        return [line.strip().removesuffix('\n') for line in f if line.strip()]
    elif path.is_dir():
      return sorted([str(p) for p in path.iterdir() if p.is_file()])
    else:
      return [var_value]
  else:
    return [var_value]

def _extract_used_vars(template):
  if not isinstance(template, str):
    return set()
  return set(re.findall(r"{(\w+)}", template))

def _substitute(template, variables):
  if not isinstance(template, str):
    return template
  return template.format(**variables)

def create_configs_from_file(file_path: Path, overwrite: bool = False) -> List[BaseConfig]:
  """Parses a YAML file to create a list of job configurations.

  This function reads a YAML configuration file, processes variables, and
  generates a list of configuration objects.

  The YAML file structure should be as follows:
  - An optional `variables` section at the root to define substitution
    variables. These can be single values, lists, or file paths with
    wildcards (glob patterns).
  - Cluster names as top-level keys.
  - Each cluster must define a `scheduler` (e.g., 'slurm').
  - Each cluster can have a `default_conf` dictionary to specify common
    parameters for all jobs on that cluster.
  - Each cluster must have a `configs` list, where each item is a
    dictionary representing a job configuration.

  The function expands configurations based on the variables used. If a
  configuration's name or parameters reference variables that are lists
  or expand from wildcards, it creates a Cartesian product of all
  possible variable combinations, generating a distinct configuration for each one.

  Args:
    file_path (Path): The path to the YAML configuration file.
    overwrite (bool, optional): If True, indicates that existing
      configurations with the same name can be overwritten.
      Defaults to False.

  Returns:
    List[BaseConfig]: A list of fully resolved configuration objects
      (e.g., SlurmConfig) created from the file.

  Raises:
    ConfigurationError: If the file is not found, contains invalid YAML,
      or does not adhere to the required structure (e.g., missing
      'scheduler' key, root is not a dictionary).
  """
  created_configs = []

  try:
    with open(file_path, 'r') as f:
      data = yaml.safe_load(f)
  except FileNotFoundError:
    raise ConfigurationError(f"Configuration file not found at: {file_path}")
  except yaml.YAMLError as e:
    raise ConfigurationError(f"Error parsing YAML file: {e}")

  # Handle variables at the top level
  variables = data.pop("variables", {})
  expanded_vars = {k: _load_variable_values(v) for k, v in variables.items()}

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
      config_name_template = config_params['name']
      # Find variables used in config name and params
      used_vars = set()
      used_vars |= _extract_used_vars(config_name_template)
      for v in config_params.values():
        used_vars |= _extract_used_vars(v) if isinstance(v, str) else set()
      # Only expand over variables that are actually used
      relevant_vars = {k: expanded_vars[k] for k in used_vars if k in expanded_vars}
      if relevant_vars:
        keys, values = zip(*relevant_vars.items())
        for combination in itertools.product(*values):
          var_dict = dict(zip(keys, combination))
          config_name = _substitute(config_name_template, var_dict)
          # Merge default params with specific config params, substituting variables
          final_params = deepcopy(default_conf)
          for k, v in config_params.items():
            if isinstance(v, (int, float, str)):
              final_params[k] = _substitute(v, var_dict)
            elif isinstance(v, list) and len(v) > 0:
              if final_params.get(k) and isinstance(final_params[k], list):
                for i in range(len(final_params[k])):
                  if isinstance(final_params[k][i], str):
                    final_params[k][i] = _substitute(final_params[k][i], var_dict)
              else:
                final_params[k] = []
              for lv in v:
                final_params[k].append(_substitute(lv, var_dict) if isinstance(lv, str) else lv)
              
          final_params["name"] = config_name
          final_params["cluster_name"] = cluster_name
          final_params["overwrite"] = overwrite
          created_configs.append(_create_config_from_params(scheduler, final_params))
      else:
        # No variables to expand
        final_params = default_conf.copy()
        final_params.update(config_params if config_params else {})
        final_params["name"] = config_name_template
        final_params["cluster_name"] = cluster_name
        final_params["overwrite"] = overwrite
        created_configs.append(_create_config_from_params(scheduler, final_params))

  return created_configs

def _create_config_from_params(scheduler: str, params: dict[str, Any]) -> BaseConfig:
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
  time: Optional[str] = None,
  overwrite: bool = False,
) -> LocalConfig:
  """Creates and saves a configuration file for local execution.

  Args:
    name: The name of the configuration.
    cluster_name: The name of the cluster this configuration belongs to.
      Defaults to the system's hostname.
    env: A list of environment variables to set.
    modules: A list of modules to load in sbatch scripts before running commands.
    time: Walltime (e.g., 01-00:00:00).
    overwrite: If True, overwrite an existing configuration with the same name.

  Returns:
    The path to the newly created configuration file.
  """
  config = LocalConfig(name=name, cluster_name=cluster_name if cluster_name else get_cluster_name(), env=env, time=time, modules=modules)
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
  custom_headers: Optional[List[str]] = None,
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
    overwrite: If True, overwrite an existing configuration with the same name.
    custom_headers: Custom scheduler headers (e.g., ['#SBATCH --my_header=my_value'])

  Returns:
    The path to the newly created configuration file.
  """
  config = PbsConfig(
    name=name, cluster_name=cluster_name if cluster_name else get_cluster_name(), queue=queue, cpus=cpus, mem=mem, walltime=walltime, env=env, custom_headers=custom_headers,
  )
  config.save_config(overwrite)
  return config

def create_slurm_config(
  name: str,
  cluster_name: Optional[str] = None,
  partition: Optional[str] = None,
  nodes: Optional[str] = None,
  ntasks: Optional[str] = None,
  tasks_per_node: Optional[int] = None,
  cpus_per_task: Optional[int] = None,
  mem: Optional[str] = None,
  account: Optional[str] = None,
  time: Optional[str] = None,
  gpus: Optional[int] = None,
  constraint: Optional[str] = None,
  nodelist: Optional[Union[str,List[str]]] = None,
  exclude: Optional[List[str]] = None,
  qos: Optional[str] = None,
  reservation: Optional[str] = None,
  exclusive: Optional[bool] = False,
  modules: Optional[List[str]] = None,
  env: Optional[List[str]] = None,
  custom_headers: Optional[List[str]] = None,
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
    tasks_per_node: The number of tasks per node.
    cpus_per_task: The number of CPUs to request per task.
    mem: The amount of memory to request (e.g., "16G", "100M").
    account: The account to charge for the job.
    time: The maximum wall time for the job (e.g., "01-00:00:00").
    gpus: The number of GPUs to request.
    constraint: Specific features required for the job's nodes.
    nodelist: A specific list of nodes to use (either a string or a list of strings to concatenate using "," as separator).
    exclude: A specific list of nodes NOT to use.
    qos: The Quality of Service for the job.
    reservation: The reservation to use for the job.
    exclusive: Enables the --exclusive flag (may not work on some clusters).
    modules: Modules to load with `module load`.
    env: A list of environment variables to set.
    overwrite: If True, overwrite an existing configuration with the same name.
    custom_headers: Custom scheduler headers (e.g., ['#SBATCH --my_header=my_value'])

  Returns:
    The path to the newly created configuration file.
  """
  config = SlurmConfig(
    name=name, cluster_name=cluster_name if cluster_name else get_cluster_name(), 
    partition=partition, nodes=nodes, ntasks=ntasks, tasks_per_node=tasks_per_node, cpus_per_task=cpus_per_task, mem=mem, account=account,
    time=time, gpus=str(gpus), constraint=constraint, nodelist=nodelist, exclude=exclude, qos=qos, reservation=reservation, exclusive=exclusive,
    modules=modules, env=env, custom_headers=custom_headers,
  )
  config.save_config(overwrite)
  return config

def load_local_config(name: str) -> Optional[LocalConfig]:
  file = get_project_configs_file_path()
  cluster_name = get_cluster_name()
  try:
    with open(file, "r") as f:
      data = yaml.safe_load(f)
  except Exception as e:
    raise ConfigurationError(f"Could not read config file: {e}")

  cluster_data = data.get(cluster_name, {})
  if not cluster_data or cluster_data.get("scheduler") != "local":
    return None

  for conf_name, conf in cluster_data.get("configs", []).items():
    if conf_name == name:
      conf["name"] = conf_name
      conf["cluster_name"] = cluster_name
      allowed_keys = {"name", "cluster_name", "env", "time"}
      filtered_conf = {k: v for k, v in conf.items() if k in allowed_keys}
      return LocalConfig(**filtered_conf)
    
  return None
