from pathlib import Path
from typing import List, Optional

from sbatchman.core import config_manager
import sbatchman.core.jobs as jobs
import sbatchman.core.launcher as launcher
import sbatchman.config.project_config as project_config

from .core.launcher import Job
from .schedulers.local import LocalConfig
from .schedulers.pbs import PbsConfig
from .schedulers.slurm import SlurmConfig
from .config.global_config import get_cluster_name


def init_project(path: Path):
  """Initializes a new SbatchMan project directory.

  This creates the necessary directory structure and configuration files
  for SbatchMan to operate within the specified path.

  Args:
    path: The root directory for the new SbatchMan project.
  """
  project_config.init_project(path)

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
) -> Path:
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
  return config.save_config(overwrite)

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
) -> Path:
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
  return config.save_config(overwrite)

def create_local_config(
  name: str,
  cluster_name: Optional[str] = None,
  env: Optional[List[str]] = None,
  modules: Optional[List[str]] = None,
  overwrite: bool = False,
) -> Path:
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
  return config.save_config(overwrite)

def launch_job(config_name: str, tag: str, command: str) -> Job:
  """Launches a single job with the specified configuration.

  Args:
    config_name: The name of the configuration to use for the job.
    tag: A tag to categorize the job.
    command: The command to execute.

  Returns:
    A Job object representing the launched job.
  """
  return launcher.launch_job(
    config_name=config_name,
    command=command,
    tag=tag
  )

def launch_jobs_from_file(jobs_file: Path) -> List[Job]:
  """Launches multiple jobs from a YAML batch file.

  Args:
    jobs_file: The path to the YAML file defining the jobs to launch.

  Returns:
    A list of Job objects for each job that was launched.
  """
  return launcher.launch_jobs_from_file(jobs_file)

def list_jobs(
  cluster_name: Optional[str] = None,
  config_name: Optional[str] = None,
  tag: Optional[str] = None,
  include_archived: bool = False
) -> List[Job]:
  """Lists active and optionally archived jobs, with optional filtering.

  Args:
    cluster_name: If provided, only list jobs from this cluster.
    config_name: If provided, only list jobs with this configuration name.
    tag: If provided, only list jobs with this tag.
    include_archived: If True, include jobs from all archives in the result.

  Returns:
    A list of Job objects matching the filter criteria.
  """
  return jobs.list_jobs(
    cluster_name=cluster_name,
    config_name=config_name,
    tag=tag,
    include_archived=include_archived
  )

def archive_jobs(archive_name: str, overwrite: bool = False, cluster_name: Optional[str] = None, config_name: Optional[str] = None, tag: Optional[str] = None) -> List[Job]:
  """Archives jobs matching the filter criteria.

  Moves job directories from the active experiments folder to a specified
  archive folder.

  Args:
    archive_name: The name of the archive to move jobs into.
    overwrite: If True, overwrite jobs in the archive if they already exist.
    cluster_name: If provided, only archive jobs from this cluster.
    config_name: If provided, only archive jobs with this configuration name.
    tag: If provided, only archive jobs with this tag.

  Returns:
    A list of Job objects that were successfully archived.
  """
  return jobs.archive_jobs(
    archive_name=archive_name,
    overwrite=overwrite,
    cluster_name=cluster_name,
    config_name=config_name,
    tag=tag
  )

def create_configs_from_file(file_path: Path, overwrite: bool = False):
  """Creates multiple job configurations from a YAML file.

  Args:
    file_path: The path to the YAML configuration file.
    overwrite: If True, overwrite existing configurations.
  """
  config_manager.create_configs_from_file(file_path, overwrite)