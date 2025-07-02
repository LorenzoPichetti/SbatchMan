# src/exp_kit/launcher.py
import subprocess
import datetime
import itertools
import re
import yaml
from pathlib import Path
from typing import List, Optional
from dataclasses import dataclass, asdict
import shlex

from sbatchman.exceptions import ConfigurationError, ConfigurationNotFoundError, ClusterNameNotSetError, JobSubmitError
from sbatchman.config.global_config import get_cluster_name
from sbatchman.config.project_config import get_project_configs_file_path, get_scheduler_from_cluster_name

from sbatchman.config.project_config import get_project_config_dir, get_experiments_dir
from ..schedulers.local import BaseConfig, LocalConfig, local_submit
from ..schedulers.slurm import SlurmConfig, slurm_submit
from ..schedulers.pbs import PbsConfig, pbs_submit

@dataclass
class Job:
  config_name: str
  cluster_name: str
  timestamp: str
  exp_dir: str
  command: str
  status: str
  scheduler: str
  job_id: str
  archive_name: Optional[str] = None

  def get_job_config(self) -> BaseConfig:
    """
    Returns the configuration of the job. It will specialize the class to either SlurmConfig, LocalConfig or PbsConfig
    """
    return get_config(self.cluster_name, self.config_name)

  def parse_command_args(self):
    """
    Parses the command string if it is a simple CLI command (no pipes, redirections, or shell operators).
    Returns (executable, args_dict) where args_dict maps argument names to values.
    """
    if any(op in self.command for op in ['|', '>', '<', ';', '&&', '||']):
      return None, None

    tokens = shlex.split(self.command)
    if not tokens:
      return None, None

    executable = tokens[0]
    args_dict = {}
    key = None
    for token in tokens[1:]:
      if token.startswith('--'):
        if '=' in token:
          k, v = token[2:].split('=', 1)
          args_dict[k] = v
          key = None
        else:
          key = token[2:]
          args_dict[key] = True
      elif token.startswith('-') and len(token) > 1:
        key = token[1:]
        args_dict[key] = True
      else:
        if key:
          args_dict[key] = token
          key = None
    return executable, args_dict

  def get_stdout(self) -> Optional[str]:
    """
    Returns the contents of the stdout log file for this job, or None if not found.
    """
    exp_dir_path = get_experiments_dir() / self.exp_dir
    stdout_path = exp_dir_path / "stdout.log"
    if stdout_path.exists():
      with open(stdout_path, "r") as f:
        return f.read()
    return None

  def get_stderr(self) -> Optional[str]:
    """
    Returns the contents of the stderr log file for this job, or None if not found.
    """
    exp_dir_path = get_experiments_dir() / self.exp_dir
    stderr_path = exp_dir_path / "stderr.log"
    if stderr_path.exists():
      with open(stderr_path, "r") as f:
        return f.read()
    return None
  

def get_config(cluster_name: str, config_name: str) -> BaseConfig:
  """
  Returns the configuration of the job. It will specialize the class to either SlurmConfig, LocalConfig or PbsConfig
  """
  configs_file_path = get_project_configs_file_path()

  if not configs_file_path.exists():
    raise ConfigurationNotFoundError(f"Configuration '{configs_file_path}' for cluster '{cluster_name}' not found at '{configs_file_path}'.")
  
  configs = yaml.safe_load(open(configs_file_path, 'r'))
  if cluster_name not in configs:
    raise ConfigurationError(f"Could not find cluster '{cluster_name}' in configurations.yaml file ({configs_file_path})")
  
  scheduler = configs[cluster_name]['scheduler']
  configs = configs[cluster_name]['configs']
  if config_name not in configs:
    raise ConfigurationError(f"Could not find configuration '{config_name}' in configurations.yaml file ({configs_file_path})")
  
  config_dict = configs[config_name]
  if scheduler == 'slurm':
    return SlurmConfig(**config_dict)
  elif scheduler == 'pbs':
    return PbsConfig(**config_dict)
  elif scheduler == 'local':
    return LocalConfig(**config_dict)
  else:
    raise ConfigurationError(f"No class found for scheduler '{scheduler}'. Supported schedulers are: slurm, pbs, local.")


def get_config_script_template(config_name: str, cluster_name: str) -> str:
  config_path = get_project_config_dir() / cluster_name / f"{config_name}.sh"

  if not config_path.exists():
    raise ConfigurationNotFoundError(f"Configuration '{config_name}' for cluster '{cluster_name}' not found at '{config_path}'.")

  return open(config_path, "r").read()
  

def launch_job(config_name: str, command: str, cluster_name: Optional[str] = None, tag: str = "default") -> Job:
  """
  Launches an experiment based on a configuration name.
  """
  if not cluster_name:
    try:
      cluster_name = get_cluster_name()
    except ClusterNameNotSetError:
      raise ConfigurationError(
        "Cluster name not specified and not set globally. "
        "Please provide --cluster-name or use 'sbatchman set-cluster-name <cluster_name>' to set a global default."
      )

  scheduler = get_scheduler_from_cluster_name(cluster_name)
  template_script = get_config_script_template(config_name, scheduler)

  # Capture the Current Working Directory at the time of launch
  submission_cwd = Path.cwd()
    
  # 2. Create a unique, nested directory for this experiment run
  timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
  # Directory structure: <cluster_name>/<config_name>/<tag>/<timestamp>
  # Find a directory name that has not been used yet
  base_exp_dir_local = Path(cluster_name) / config_name / tag / timestamp
  exp_dir_local = base_exp_dir_local
  exp_dir = get_experiments_dir() / exp_dir_local
  counter = 1
  while exp_dir.exists():
    exp_dir_local = base_exp_dir_local.with_name(f"{base_exp_dir_local.name}_{counter}")
    exp_dir = get_experiments_dir() / exp_dir_local
    counter += 1
  exp_dir.mkdir(parents=True, exist_ok=False)

  # 3. Prepare the final runnable script
  # Replace placeholders for log and CWD
  final_script_content = template_script.replace(
    "{EXP_DIR}", str(exp_dir.resolve())
  ).replace(
    "{CWD}", str(submission_cwd.resolve())
  ).replace(
    "{CMD}", str(command)
  )
  
  run_script_path = exp_dir / "run.sh"
  with open(run_script_path, "w") as f:
    f.write(final_script_content)
  run_script_path.chmod(0o755)

  metadata = Job(
    config_name=config_name,
    cluster_name=cluster_name,
    timestamp=timestamp,
    exp_dir=str(exp_dir_local),
    command=command,
    status="SUBMITTING",
    scheduler=scheduler,
    job_id="",
    archive_name=None,
  )

  # 4. Write metadata.yaml before job submission
  with open(exp_dir / "metadata.yaml", "w") as f:
    yaml.safe_dump(asdict(metadata), f, default_flow_style=False)

  try:
    # 5. Submit the job using the scheduler's own logic
    job_id = None
    if scheduler == 'slurm':
      job_id = slurm_submit(run_script_path, exp_dir)
    elif scheduler == 'pbs':
      job_id = pbs_submit(run_script_path, exp_dir)
    elif scheduler == 'local':
      job_id = local_submit(run_script_path, exp_dir)
    else:
      raise ConfigurationError(f"No submission class found for scheduler '{scheduler}'. Supported schedulers are: slurm, pbs, local.")
    
    metadata.job_id = job_id
  except (subprocess.CalledProcessError, ValueError, FileNotFoundError) as e:
    metadata.status = "FAILED_SUBMISSION"
    with open(exp_dir / "metadata.yaml", "w") as f:
      yaml.safe_dump(asdict(metadata), f, default_flow_style=False)
    raise
  finally:    
    return metadata


def _load_variable_values(var_value):
  # If var_value is a list, return as is
  if isinstance(var_value, list):
    return var_value
  # If var_value is a string and a file, read lines
  elif isinstance(var_value, str):
    path = Path(var_value)
    if path.is_file():
      with open(path, "r") as f:
        return [line.strip().replace('\n', '') for line in f if line.strip()]
    elif path.is_dir():
      # Return sorted list of file names in the directory
      return sorted([str(p.absolute()) for p in path.iterdir() if p.is_file()])
    else:
      raise JobSubmitError(
        f"Variable value '{var_value}' is not a list, file, or directory.\n"
        "YAML script semantics:\n"
        "- Variables can be lists, a path to a file (one value per line), or a path to a directory (all file absolute paths used as values).\n"
        "- The cartesian product of all variable values is used to generate jobs.\n"
        "- Experiments can define configuration names (possibly using variables) and tags.\n"
        "- 'command' and 'variables' can be redefined or extended in inner YAML tags.\n"
        "- The '{var_name}' syntax is substituted with the actual value of 'var_name'."
      )
  else:
    return [var_value]


def _merge_dicts(base, override):
  # Recursively merge two dictionaries
  result = dict(base)
  for k, v in override.items():
    if k in result and isinstance(result[k], dict) and isinstance(v, dict):
      result[k] = _merge_dicts(result[k], v)
    else:
      result[k] = v
  return result


def _substitute(template, variables):
  # Replace {var} in template with values from variables
  if not isinstance(template, str):
    return template
  return template.format(**variables)


def _extract_used_vars(*templates):
  """Extract variable names used in {var} format from given templates."""
  var_names = set()
  for template in templates:
    if isinstance(template, str):
      var_names.update(re.findall(r"{(\w+)}", template))
  return var_names

def launch_jobs_from_file(jobs_file_path: Path) -> List[Job]:
  with open(jobs_file_path, "r") as f:
    config = yaml.safe_load(f)

  global_vars = config.get("variables", {})
  global_command = config.get("command", None)
  experiments = config.get("experiments", {})

  # Prepare global variable values (expand files if needed)
  expanded_global_vars = {}
  for k, v in global_vars.items():
    expanded_global_vars[k] = _load_variable_values(v)

  jobs = []

  for exp_name_template, exp_data in experiments.items():
    exp_command = exp_data.get("command", global_command)
    exp_vars = exp_data.get("variables", {})
    expanded_exp_vars = {}
    for k, v in exp_vars.items():
      expanded_exp_vars[k] = _load_variable_values(v)

    merged_exp_vars = dict(expanded_global_vars)
    merged_exp_vars.update(expanded_exp_vars)

    tags = exp_data.get("tags", {})
    if tags:
      for tag_name, tag_data in tags.items():
        tag_vars = tag_data.get("variables", {})
        expanded_tag_vars = {}
        for k, v in tag_vars.items():
          expanded_tag_vars[k] = _load_variable_values(v)
        merged_vars = dict(merged_exp_vars)
        merged_vars.update(expanded_tag_vars)
        tag_command = tag_data.get("command", exp_command)

        # Only use variables referenced in exp_name, command, or tag_command
        used_vars = _extract_used_vars(exp_name_template, tag_command)
        filtered_vars = {k: v for k, v in merged_vars.items() if k in used_vars}

        if not filtered_vars:
          continue  # No variables used, skip job generation

        keys, values = zip(*filtered_vars.items()) if filtered_vars else ([], [])
        for combination in itertools.product(*values):
          var_dict = dict(zip(keys, combination))
          exp_name = _substitute(exp_name_template, var_dict)
          command = _substitute(tag_command, var_dict)
          # print('='*40)
          # print(f'{exp_name=}')
          # print(f'{command=}')
          # print(f'{tag_name=}')
          # print('='*40)
          job = launch_job(exp_name, command, tag=tag_name)
          jobs.append(job)
    else:
      used_vars = _extract_used_vars(exp_name_template, exp_command)
      filtered_vars = {k: v for k, v in merged_exp_vars.items() if k in used_vars}

      if not filtered_vars:
        continue  # No variables used, skip job generation

      keys, values = zip(*filtered_vars.items()) if filtered_vars else ([], [])
      for combination in itertools.product(*values):
        var_dict = dict(zip(keys, combination))
        exp_name = _substitute(exp_name_template, var_dict)
        command = _substitute(exp_command, var_dict)
        # print('='*40)
        # print(f'{exp_name=}')
        # print(f'{command=}')
        # print('='*40)
        job = launch_job(exp_name, command)
        jobs.append(job)

  return jobs