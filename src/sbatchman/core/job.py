import yaml
from pathlib import Path
from typing import Any, Optional
from dataclasses import dataclass, asdict
import shlex

from sbatchman.config.project_config import get_archive_dir, get_experiments_dir, get_project_configs_file_path
from sbatchman.core.status import Status
from sbatchman.exceptions import ConfigurationError, ConfigurationNotFoundError
from sbatchman.schedulers.pbs import PbsConfig
from sbatchman.schedulers.slurm import SlurmConfig
from sbatchman.schedulers.local import LocalConfig
from sbatchman.schedulers.base import BaseConfig
import subprocess

@dataclass
class Job:
  config_name: str
  cluster_name: str
  timestamp: str
  exp_dir: str
  command: str
  status: str
  scheduler: str
  tag: str
  job_id: int
  exitcode: Optional[int] = None
  preprocess: Optional[str] = None
  postprocess: Optional[str] = None 
  archive_name: Optional[str] = None

  def get_job_config(self) -> BaseConfig:
    """
    Returns the configuration of the job. It will specialize the class to either SlurmConfig, LocalConfig or PbsConfig
    """
  
    configs_file_path = get_project_configs_file_path()

    if not configs_file_path.exists():
      raise ConfigurationNotFoundError(f"Configuration '{configs_file_path}' for cluster '{self.cluster_name}' not found at '{configs_file_path}'.")
    
    configs = yaml.safe_load(open(configs_file_path, 'r'))
    if self.cluster_name not in configs:
      raise ConfigurationError(f"Could not find cluster '{self.cluster_name}' in configurations.yaml file ({configs_file_path})")
    
    scheduler = configs[self.cluster_name]['scheduler']
    configs = configs[self.cluster_name]['configs']
    if self.config_name not in configs:
      raise ConfigurationError(f"Could not find configuration '{self.config_name}' in configurations.yaml file ({configs_file_path})")
    
    config_dict = configs[self.config_name]
    config_dict['name'] = self.config_name
    config_dict['cluster_name'] = self.cluster_name
    if 'scheduler' in config_dict:
      del config_dict['scheduler']

    if scheduler == 'slurm':
      return SlurmConfig(**config_dict)
    elif scheduler == 'pbs':
      return PbsConfig(**config_dict)
    elif scheduler == 'local':
      return LocalConfig(**config_dict)
    else:
      raise ConfigurationError(f"No class found for scheduler '{scheduler}'. Supported schedulers are: slurm, pbs, local.")

  def parse_command_args(self) -> tuple[None, None, None] | tuple[str, list[Any], dict[Any, Any]]:
    """
    Parses the command string if it is a simple CLI command (no pipes, redirections, or shell operators).
    Returns (executable, args_dict, positional_args) where args_dict maps argument names to values,
    and positional_args is a list of positional arguments (not associated with any flag).
    """
    if any(op in self.command for op in ['|', '>', '<', ';', '&&', '||']):
      return None, None, None

    tokens = shlex.split(self.command)
    if not tokens:
      return None, None, None

    executable = tokens[0]
    args_dict = {}
    positional_args = []
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
        else:
          positional_args.append(token)
    return executable, positional_args, args_dict

  def get_job_base_path(self) -> Path:
    if self.archive_name:
      return get_archive_dir() / self.archive_name / self.exp_dir
    else:
      return get_experiments_dir() / self.exp_dir

  def get_stdout_path(self) -> Path:
    return self.get_job_base_path() / "stdout.log"

  def get_stdout(self) -> Optional[str]:
    """
    Returns the contents of the stdout log file for this job, or None if not found.
    """
    stdout_path = self.get_stdout_path()
    if stdout_path.exists():
      with open(stdout_path, "r") as f:
        return f.read()
    return None

  def get_stderr_path(self) -> Path:
    return self.get_job_base_path() / "stderr.log"

  def get_stderr(self) -> Optional[str]:
    """
    Returns the contents of the stderr log file for this job, or None if not found.
    """
    stderr_path = self.get_stderr_path()
    if stderr_path.exists():
      with open(stderr_path, "r") as f:
        return f.read()
    return None

  def get_metadata_path(self) -> Path:
    """
    Returns the path to the metadata.yaml file for this job.
    If the job is archived, it will return the path in the archive directory.
    Otherwise, it returns the path in the active experiments directory.
    """
    return self.get_job_base_path() / "metadata.yaml"

  def write_metadata(self):
    """Saves the current job state to its metadata.yaml file."""
    path = self.get_metadata_path()
    
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w") as f:
      job_dict = asdict(self)
      # Convert Path objects to strings for clean YAML representation
      for key, value in job_dict.items():
        if isinstance(value, Path) or isinstance(value, Status):
          job_dict[key] = str(value)
      yaml.dump(job_dict, f, default_flow_style=False)

  def write_job_id(self):
    """
    Updates the job_id in the metadata.yaml file.
    This is used to update the job_id after the job has been submitted.
    """
    path = self.get_metadata_path()

    if path.exists():
      subprocess.run(["sed", "-i", f"s/^job_id: [0-9]*/job_id: {int(self.job_id)}/", str(path)], check=True)
      # with open(path, 'rw') as f:
      #   lines = f.readlines()
      #   for line in lines:
      #     if line.strip().startswith('job_id:'):
      #       f.write(f"job_id: {int(self.job_id)}\n")
      #     else:
      #       f.write(line)
      
  def write_job_status(self):
    """
    Updates the status in the metadata.yaml file.
    """
    path = self.get_metadata_path()

    if path.exists():
      subprocess.run(["sed", "-i", f"/^status:/c\\status: {str(self.status)}", str(path)], check=True)