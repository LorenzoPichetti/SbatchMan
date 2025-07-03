"""A utility to create, launch, and monitor code experiments."""

__version__ = "0.1.0"

from .api import init_project, \
    create_local_config, create_slurm_config, create_pbs_config, \
    launch_job, jobs_list, jobs_df, \
    archive_jobs
from .exceptions import SbatchManError, ProjectNotInitializedError, ProjectExistsError

__all__ = [
    "SbatchManError",
    "ProjectNotInitializedError",
    "ProjectExistsError",

    "init_project",

    "create_local_config",
    "create_slurm_config",
    "create_pbs_config",

    "launch_job",

    "jobs_list",
    "jobs_df",

    "archive_jobs",
]