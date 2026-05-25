from copy import deepcopy
import os
from pathlib import Path
import re
import yaml
import itertools

from sbatchman.core.variables import extract_used_vars, substitute, load_variable_values, map_info_to_vars, resolve_map_variable
from sbatchman.config.global_config import get_cluster_name
from sbatchman.config.project_config import get_project_configs_file_path
from sbatchman.exceptions import ConfigurationError, SyntaxError
from typing import Any, List, Optional, Union
from sbatchman.schedulers.base import BaseConfig
from sbatchman.schedulers.local import LocalConfig
from sbatchman.schedulers.pbs import PbsConfig
from sbatchman.schedulers.slurm import SlurmConfig


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
 
  Map variables (dicts with '__map__' key) are resolved dynamically based on 
  their key variable's value.
 
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
  expanded_vars = {k: load_variable_values(v) for k, v in variables.items()}
 
  if not isinstance(data, dict):
    raise ConfigurationError("The root of the configuration file must be a dictionary of clusters.")
 
  for cluster_name, cluster_configs in data.items():
    if cluster_name != get_cluster_name():
      continue
    
    scheduler = cluster_configs.get("scheduler")
    if not scheduler:
      raise ConfigurationError(f"Cluster '{cluster_name}' must have a 'scheduler' defined.")
 
    default_conf = cluster_configs.get("default_conf", {})
    configs = cluster_configs.get("configs", {})
 
    if not configs:
      continue
 
    for config_params in configs:
      config_name_template = config_params['name']
      
      # Find which variables are used in config parameters
      used_vars = set()
      used_vars |= extract_used_vars(config_name_template)
      for v in config_params.values():
        if isinstance(v, str):
          used_vars |= extract_used_vars(v)
        elif isinstance(v, list):
          for lv in v:
            if isinstance(lv, str):
              used_vars |= extract_used_vars(lv)
      
      # Identify map variables and their key variables
      map_info = {}  # map_name -> (map_dict, key_var_name)
      filtered_vars = {}
      
      for k, v in expanded_vars.items():
        if isinstance(v, dict) and '__map__' in v:
          if k in used_vars:
            # Find the key variable for this map by searching config parameters
            key_var = None
            for template in [config_name_template] + [val for val in config_params.values() if isinstance(val, str)]:
              if isinstance(template, str):
                pattern = re.compile(rf"\{{{k}\[(\w+)\]\}}")
                match = pattern.search(template)
                if match:
                  key_var = match.group(1)
                  break
            if key_var:
              map_info[k] = (v, key_var)
          continue
        
        # Handle tuples (file paths)
        if len(v) > 0 and isinstance(v[0], tuple):
          if k in used_vars or f'{k}_filename' in used_vars:
            filtered_vars[k] = v
        else:
          if k in used_vars:
            filtered_vars[k] = v
      
      if not filtered_vars and not map_info:
        # No variables to expand, create single config
        final_params = deepcopy(default_conf)
        final_params.update(config_params if config_params else {})
        final_params["name"] = config_name_template
        final_params["cluster_name"] = cluster_name
        final_params["overwrite"] = overwrite
        created_configs.append(_create_config_from_params(scheduler, final_params))
      else:
        # Expand over variable combinations
        keys, values = zip(*filtered_vars.items()) if filtered_vars else ([], [])
        
        # Generate cartesian product of base variables
        combinations_iter = itertools.product(*values) if filtered_vars else [()]
        
        for combination in combinations_iter:
          var_dict = dict(zip(keys, combination)) if keys else {}
          
          # Merge with map variable structure for substitution
          substitution_vars = {**map_info_to_vars(map_info), **var_dict}
          
          # Resolve map variables based on their key variables
          map_resolved = {}
          for map_name, (map_var_dict, key_var_name) in map_info.items():
            if key_var_name in var_dict:
              try:
                resolved_list = resolve_map_variable(map_var_dict, var_dict[key_var_name])
                map_resolved[map_name] = resolved_list
              except KeyError as e:
                raise ConfigurationError(
                  f"Error resolving map variable '{map_name}': {e}"
                )
          
          # If there are resolved map variables, iterate through their combinations
          if map_resolved:
            map_keys, map_values = zip(*map_resolved.items())
            for map_combination in itertools.product(*map_values):
              map_dict = dict(zip(map_keys, map_combination))
              final_vars = {**substitution_vars, **map_dict}
              
              _create_config_from_params_helper(
                config_params, config_name_template, default_conf,
                final_vars, scheduler, cluster_name, overwrite,
                created_configs
              )
          else:
            # No map variables to resolve
            _create_config_from_params_helper(
              config_params, config_name_template, default_conf,
              substitution_vars, scheduler, cluster_name, overwrite,
              created_configs
            )
  
  return created_configs
 
 
def _create_config_from_params_helper(
  config_params: dict,
  config_name_template: str,
  default_conf: dict,
  final_vars: dict,
  scheduler: str,
  cluster_name: str,
  overwrite: bool,
  created_configs: List[BaseConfig],
) -> None:
  """Helper function to create a config with substituted variables."""
  config_name = substitute(config_name_template, final_vars)
  
  # Merge default params with specific config params, substituting variables
  final_params = deepcopy(default_conf)
  for k, v in config_params.items():
    if isinstance(v, (int, float, str)):
      final_params[k] = substitute(v, final_vars)
    elif isinstance(v, list) and len(v) > 0:
      if final_params.get(k) and isinstance(final_params[k], list):
        for i in range(len(final_params[k])):
          if isinstance(final_params[k][i], str):
            final_params[k][i] = substitute(final_params[k][i], final_vars)
      else:
        final_params[k] = []
      for lv in v:
        final_params[k].append(substitute(lv, final_vars) if isinstance(lv, str) else lv)
  
  final_params["name"] = config_name
  final_params["cluster_name"] = cluster_name
  final_params["overwrite"] = overwrite
  created_configs.append(_create_config_from_params(scheduler, final_params))
 
 
def _create_config_from_params(scheduler: str, params: dict[str, Any]) -> BaseConfig:
  """
  Calls the appropriate API function to create a single configuration.
  """
  params = {k.replace('-', '_'): v for k, v in params.items()}
  
  if 'scheduler' in params:
    scheduler = params['scheduler']
    del params['scheduler']
    
  try:
    if scheduler == "slurm":
      return create_slurm_config(**params)
    elif scheduler == "pbs":
      return create_pbs_config(**params)
    elif scheduler == "local":
      return create_local_config(**params)
    else:
      raise ConfigurationError(f"Unsupported scheduler '{scheduler}'. Supported schedulers are: slurm, pbs, local.")
  except Exception as e:
    raise SyntaxError(str(e))

  
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
