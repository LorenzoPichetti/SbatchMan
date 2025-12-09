import shutil
import fnmatch
from typing import List, Optional

import yaml

from sbatchman.config.global_config import get_cluster_name
from sbatchman.config.project_config import get_archive_dir, get_experiments_dir
from sbatchman.core.job import Job
from sbatchman.core.status import TERMINAL_STATES, Status
from sbatchman.exceptions import ArchiveExistsError
import pandas as pd
from dataclasses import asdict

JOBS_CACHE = {}

def job_exists(
  command: str,
  config_name: str,
  cluster_name: str,
  tag: str,
  preprocess: Optional[str],
  postprocess: Optional[str]
) -> bool:
  """
  Checks if an identical active job already exists.
  """
  global JOBS_CACHE
  cache_key = (cluster_name, config_name, tag)

  if cache_key not in JOBS_CACHE:
    JOBS_CACHE[cache_key] = []
    exp_dir = get_experiments_dir()
    
    # Construct a specific glob pattern to only scan relevant directories.
    # We use the directory structure to narrow down the search significantly.
    # Structure: cluster/config/tag/timestamp/metadata.yaml
    glob_pattern = f"{cluster_name}/{config_name}/{tag}/*/metadata.yaml"

    # Iterate through files and populate cache
    for metadata_path in exp_dir.glob(glob_pattern):
      try:
        with open(metadata_path, 'r') as f:
          job_dict = yaml.safe_load(f)
        
        if job_dict:
          JOBS_CACHE[cache_key].append(job_dict)
      except Exception:
        continue

    # Also check archived jobs
    archive_root = get_archive_dir()
    # Archive structure: archive_name/cluster_name/config_name/tag/timestamp
    # We use a wildcard for archive_name
    archive_glob_pattern = f"*/{cluster_name}/{config_name}/{tag}/*/metadata.yaml"
    
    for metadata_path in archive_root.glob(archive_glob_pattern):
      try:
        with open(metadata_path, 'r') as f:
          job_dict = yaml.safe_load(f)
        
        if job_dict:
          JOBS_CACHE[cache_key].append(job_dict)
      except Exception:
        continue

  # Check against cache
  for job_dict in JOBS_CACHE[cache_key]:
      if (
        job_dict.get('command') == command and
        job_dict.get('preprocess') == preprocess and
        job_dict.get('postprocess') == postprocess
      ):
        return True
      
  return False

def register_job(job: Job):
  """
  Registers a new job in the cache to avoid disk reads on subsequent checks.
  """
  global JOBS_CACHE
  cache_key = (job.cluster_name, job.config_name, job.tag)
  if cache_key in JOBS_CACHE:
    JOBS_CACHE[cache_key].append(asdict(job))

def jobs_list(
  cluster_name: Optional[str] = None,
  config_name: Optional[str] = None,
  tag: Optional[str] = None,
  status: Optional[List[Status]] = None,
  archive_name: Optional[str] = None,
  from_active: bool = True,
  from_archived: bool = False,
  update_jobs: bool = True
) -> List[Job]:
  """
  Lists active and/or archived jobs, with optional filtering. Updates the status of active jobs by default.
  Args:
    cluster_name: Filter by cluster name.
    config_name: Filter by configuration name.
    tag: Filter by tag.
    status: Filter by a set of Status.
    archive_name: If provided, only include jobs from this archive.
    from_active: If True, include active jobs.
    from_archived: If True, include archived jobs.
    update_jobs: If True, update the status of active jobs before listing.
  Returns:
    A list of Job objects matching the filter criteria.
  Raises:
    ArchiveExistsError: If an archive with the specified name already exists and overwrite is False.
  """
  jobs = []
  exp_dir = get_experiments_dir()

  if update_jobs:
    update_jobs_status()
  
  # Scan active jobs
  if from_active:
    exp_dir = get_experiments_dir()
    
    # Construct a more specific glob pattern if filters are available
    # Structure: cluster/config/tag/timestamp/metadata.yaml
    # We use fixed depth to avoid scanning subdirectories (which is very slow)
    parts = [
        cluster_name or "*",
        config_name or "*",
        tag or "*",
        "*", # timestamp
        "metadata.yaml"
    ]
    glob_pattern = "/".join(parts)

    for metadata_path in exp_dir.glob(glob_pattern):
      # Apply filters based on path structure BEFORE reading file
      # Active: .../cluster/config/tag/timestamp/metadata.yaml (parts[-5] is cluster)
      if cluster_name and not fnmatch.fnmatch(metadata_path.parts[-5], cluster_name):
          continue
      if config_name and not fnmatch.fnmatch(metadata_path.parts[-4], config_name):
          continue
      if tag and not fnmatch.fnmatch(metadata_path.parts[-3], tag):
          continue

      try:
        with open(metadata_path, 'r') as f:
          job_dict = yaml.safe_load(f)
          if job_dict:
            jobs.append(Job(**job_dict))
      except Exception:
        continue

  # Scan archived jobs
  if from_archived:
    archive_root = get_archive_dir()
    
    # Construct a more specific glob pattern if filters are available
    # Archive structure: archive_name/cluster_name/config_name/tag/timestamp/metadata.yaml
    parts = [
        archive_name or "*",
        cluster_name or "*",
        config_name or "*",
        tag or "*",
        "*", # timestamp
        "metadata.yaml"
    ]
    glob_pattern = "/".join(parts)

    for metadata_path in archive_root.glob(glob_pattern):
      # Apply filters based on path structure BEFORE reading file
      # Archive: .../archive_name/cluster/config/tag/timestamp/metadata.yaml (parts[-6] is archive_name)
      if cluster_name and not fnmatch.fnmatch(metadata_path.parts[-5], cluster_name):
          continue
      if config_name and not fnmatch.fnmatch(metadata_path.parts[-4], config_name):
          continue
      if tag and not fnmatch.fnmatch(metadata_path.parts[-3], tag):
          continue
      if archive_name and not fnmatch.fnmatch(metadata_path.parts[-6], archive_name):
          continue

      try:
        with open(metadata_path, 'r') as f:
          job_dict = yaml.safe_load(f)
          if job_dict:
            jobs.append(Job(**job_dict))
      except Exception:
        continue
  
  if status:
    status = [s.value if isinstance(s, Status) else str(s) for s in status]
    jobs = [j for j in jobs if str(j.status) in status]
    
  return jobs

def jobs_df(
  cluster_name: Optional[str] = None,
  config_name: Optional[str] = None,
  tag: Optional[str] = None,
  include_archived: bool = False
) -> pd.DataFrame:
  """
  Returns a pandas DataFrame of jobs, with optional filtering.
  Args:
    cluster_name: Filter by cluster name.
    config_name: Filter by configuration name.
    tag: Filter by tag.
    include_archived: If True, include archived jobs in the DataFrame.
  Returns:
    A pandas DataFrame containing job metadata.
  """
  jobs = jobs_list(
    cluster_name=cluster_name,
    config_name=config_name,
    tag=tag,
    from_archived=include_archived
  )
  jobs_dicts = [job.__dict__ for job in jobs]
  return pd.DataFrame(jobs_dicts)
  
def archive_jobs(archive_name: str, overwrite: bool = False, cluster_name: Optional[str] = None, config_name: Optional[str] = None, tag: Optional[str] = None, status: Optional[List[Status]] = None) -> List[Job]:
  """
  Archives jobs matching the filter criteria.
  """
  archive_path = get_archive_dir() / archive_name
  if archive_path.exists():
    if overwrite:
      shutil.rmtree(archive_path)
    else:
      raise ArchiveExistsError(
        f"Archive '{archive_name}' already exists. Use --overwrite to replace it."
      )
  
  jobs_to_archive = jobs_list(from_archived=False, cluster_name=cluster_name, config_name=config_name, tag=tag, status=status)

  exp_dir_root = get_experiments_dir()
  
  for job in jobs_to_archive:
    source_job_dir = exp_dir_root / job.exp_dir
    if not source_job_dir.exists():
      continue

    # Update metadata before moving
    job.archive_name = archive_name
    
    # Move to archive
    dest_job_dir = archive_path / job.exp_dir
    dest_job_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(source_job_dir), str(dest_job_dir))

    # Rewrite metadata in new location
    job.write_metadata()

  return jobs_to_archive

def delete_jobs(
  cluster_name: Optional[str] = None,
  config_name: Optional[str] = None,
  tag: Optional[str] = None,
  archive_name: Optional[str] = None,
  archived: bool = False,
  not_archived: bool = False,
  status: Optional[List[Status]] = None,
) -> int:
  """
  Deletes jobs matching the filter criteria.

  Args:
    cluster_name: Filter by cluster name.
    config_name: Filter by configuration name.
    tag: Filter by tag.
    archive_name: If provided, only delete jobs from this archive.
    archived: If True, delete only archived jobs.
    not_archived: If True, delete only active jobs.
    status: Filter jobs by status.

  Returns:
    The number of deleted jobs.
  """
  jobs_to_delete = jobs_list(
    cluster_name=cluster_name,
    config_name=config_name,
    tag=tag,
    archive_name=archive_name,
    from_active=not_archived,
    from_archived=archived,
    status=status,
  )

  if not jobs_to_delete:
    return 0

  exp_dir_root = get_experiments_dir()
  archive_root = get_archive_dir()

  for job in jobs_to_delete:
    if job.archive_name:
      job_dir = archive_root / job.archive_name / job.exp_dir
    else:
      job_dir = exp_dir_root / job.exp_dir
    

    if job_dir.exists():
      shutil.rmtree(job_dir)
    
    # Recursively delete empty parent directories
    parent_dir = job_dir.parent
    stop_dir = None
    if job.archive_name:
      stop_dir = archive_root
    else:
      stop_dir = exp_dir_root

    try:
      while parent_dir.is_dir() and not any(parent_dir.iterdir()) and parent_dir != stop_dir:
        shutil.rmtree(parent_dir)
        parent_dir = parent_dir.parent
    except FileNotFoundError:
      # This can happen in concurrent scenarios, it's safe to ignore.
      pass

  return len(jobs_to_delete)

def update_jobs_status() -> int:
  """
  Updates the status of active jobs on the current cluster by querying the scheduler.
  
  Returns:
    The number of jobs whose status was updated.
  """
  current_cluster = get_cluster_name()
  active_jobs = jobs_list(cluster_name=current_cluster, from_active=True, from_archived=False, update_jobs=False)
  
  updated_count = 0

  for job in active_jobs:
    if job.status in TERMINAL_STATES:
      continue

    try:
      config = job.get_job_config()
      new_status = config.get_job_status(job.job_id).value
      
      if new_status == Status.UNKNOWN.value:
        continue

      if new_status != job.status:
        job.status = new_status
        job.write_metadata()
        updated_count += 1
    except Exception as e:
      # Ignore errors (e.g., config not found) and continue
      continue
          
  return updated_count