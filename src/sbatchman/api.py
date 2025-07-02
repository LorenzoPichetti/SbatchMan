from pathlib import Path
from typing import List, Optional

import sbatchman.core.jobs as jobs
import sbatchman.core.launcher as launcher
import sbatchman.config.project_config as project_config

from .core.launcher import Job
from .schedulers.local import LocalConfig
from .schedulers.pbs import PbsConfig
from .schedulers.slurm import SlurmConfig
from .config.global_config import get_cluster_name


def init_project(path: Path):
  """Initializes a new SbatchMan root directory."""
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
  """Creates a SLURM configuration file."""
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
  """Creates a PBS configuration file."""
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
  """Creates a configuration file for local execution."""
  config = LocalConfig(name=name, cluster_name=cluster_name if cluster_name else get_cluster_name(), env=env, modules=modules)
  return config.save_config(overwrite)

def launch_job(config_name: str, tag: str, command: str) -> Job:
  return launcher.launch_job(
    config_name=config_name,
    command=command,
    tag=tag
  )

def launch_jobs_from_file(jobs_file: Path) -> List[Job]:
  return launcher.launch_jobs_from_file(jobs_file)

def list_jobs(
  cluster_name: Optional[str] = None,
  config_name: Optional[str] = None,
  tag: Optional[str] = None,
  include_archived: bool = False
) -> List[Job]:
  """
  Lists active and optionally archived jobs, with optional filtering.
  """
  return jobs.list_jobs(
    cluster_name=cluster_name,
    config_name=config_name,
    tag=tag,
    include_archived=include_archived
  )

def archive_jobs(archive_name: str, overwrite: bool = False, cluster_name: Optional[str] = None, config_name: Optional[str] = None, tag: Optional[str] = None) -> List[Job]:
  """
  Archives jobs matching the filter criteria.
  """
  return jobs.archive_jobs(
    archive_name=archive_name,
    overwrite=overwrite,
    cluster_name=cluster_name,
    config_name=config_name,
    tag=tag
  )
