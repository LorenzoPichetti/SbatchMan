import importlib.metadata

from .config.global_config import get_cluster_name, get_max_queued_jobs, set_max_queued_jobs
from .config.project_config import init_project
from .core.config_manager import create_configs_from_file, create_local_config, create_slurm_config, create_pbs_config
from .core.launcher import launch_job, launch_jobs_from_file, job_submit
from .core.jobs_manager import jobs_list, jobs_df, archive_jobs, delete_jobs, update_jobs_status, count_active_jobs
from .schedulers.slurm import SlurmConfig
from .schedulers.pbs import PbsConfig
from .schedulers.local import LocalConfig

from .exceptions import SbatchManError, ProjectNotInitializedError, ProjectExistsError
from .core.launcher import Job
from .core.status import Status

__version__ = '0.0.0'
try:
  __version__ = importlib.metadata.version("sbatchman")
except:
  pass

__all__ = [
  "SbatchManError",
  "ProjectNotInitializedError",
  "ProjectExistsError",
  
  "get_cluster_name",
  "get_max_queued_jobs",
  "set_max_queued_jobs",

  "Job",
  "Status",

  "init_project",

  "SlurmConfig",
  "PbsConfig",
  "LocalConfig",

  "create_local_config",
  "create_slurm_config",
  "create_pbs_config",
  "create_configs_from_file",
  "launch_jobs_from_file",

  "launch_job",
  "job_submit",

  "jobs_list",
  "jobs_df",
  "count_active_jobs",

  "archive_jobs",
  "delete_jobs",
  "update_jobs_status",
]