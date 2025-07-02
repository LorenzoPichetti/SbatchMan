from pathlib import Path
import shutil
from typing import List, Optional

import yaml

from sbatchman.core.jobs import archive_jobs, list_jobs
from sbatchman.exceptions import ArchiveExistsError

from .core.launcher import Job, launch_job
from .config.project_config import get_archive_dir, get_experiments_dir, init_project
from .schedulers.local import LocalConfig
from .schedulers.pbs import PbsConfig
from .schedulers.slurm import SlurmConfig

class SbatchManAPI:
  """A library-friendly API for SbatchMan operations."""

  def init_project(self, path: Path):
    """Initializes a new SbatchMan root directory and ensures global config exists."""
    init_project(path)

  def create_slurm_config(
    self,
    name: str,
    hostname: str,
    partition: Optional[str] = None,
    nodes: Optional[str] = None,
    ntasks: Optional[str] = None,
    cpus_per_task: Optional[int] = None,
    mem: Optional[str] = None,
    time: Optional[str] = None,
    gpus: Optional[int] = None,
    env: Optional[List[str]] = None,
    modules: Optional[List[str]] = None,
    overwrite: bool = False,
  ) -> Path:
    """Creates a SLURM configuration file."""
    config = SlurmConfig(
      name=name, hostname=hostname, partition=partition, nodes=nodes, ntasks=ntasks,
      cpus_per_task=cpus_per_task, mem=mem, time=time, gpus=gpus, env=env, modules=modules
    )
    return config.save_config(overwrite)

  def create_pbs_config(
    self,
    name: str,
    hostname: str,
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
      name=name, hostname=hostname, queue=queue, cpus=cpus, mem=mem, walltime=walltime, env=env, modules=modules
    )
    return config.save_config(overwrite)

  def create_local_config(
    self,
    name: str,
    hostname: str,
    env: Optional[List[str]] = None,
    modules: Optional[List[str]] = None,
    overwrite: bool = False,
  ) -> Path:
    """Creates a configuration file for local execution."""
    config = LocalConfig(name=name, hostname=hostname, env=env, modules=modules)
    return config.save_config(overwrite)

  def launch_job(self, hostname: Optional[str], config_name: str, tag: str, command: str) -> Job:
    return launch_job(
      config_name=config_name,
      command=command,
      hostname=hostname,
      tag=tag
    )
  
  def list_jobs(
    self,
    hostname: Optional[str] = None,
    config_name: Optional[str] = None,
    tag: Optional[str] = None,
    include_archived: bool = False
  ) -> List[Job]:
    """
    Lists active and optionally archived jobs, with optional filtering.
    """
    return list_jobs(
      hostname=hostname,
      config_name=config_name,
      tag=tag,
      include_archived=include_archived
    )
  
  def archive_jobs(self, archive_name: str, overwrite: bool = False, hostname: Optional[str] = None, config_name: Optional[str] = None, tag: Optional[str] = None) -> List[Job]:
    """
    Archives jobs matching the filter criteria.
    """
    return archive_jobs(
      archive_name=archive_name,
      overwrite=overwrite,
      hostname=hostname,
      config_name=config_name,
      tag=tag
    )
