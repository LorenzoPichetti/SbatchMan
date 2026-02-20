from ast import Tuple
import shutil
import fnmatch
import os
from typing import List, Optional, Dict, Any, Tuple
import concurrent.futures
from pathlib import Path

import yaml
try:
    from yaml import CSafeLoader as SafeLoader
except ImportError:
    from yaml import SafeLoader

from sbatchman.config.global_config import get_cluster_name
from sbatchman.config.project_config import get_archive_dir, get_experiments_dir
from sbatchman.core.job import Job
from sbatchman.core.status import TERMINAL_STATES, Status
from sbatchman.exceptions import ArchiveExistsError
import pandas as pd
from dataclasses import asdict

JOBS_CACHE = {}

def clean_jobs_cache():
  global JOBS_CACHE
  JOBS_CACHE = {}

def job_exists(
  command: str,
  config_name: str,
  cluster_name: str,
  tag: str,
  preprocess: Optional[str],
  postprocess: Optional[str],
  ignore_archived: bool = False,
  ignore_conf_in_dup_check: bool = False,
  ignore_commands_in_dup_check: bool = False,
) -> Tuple[bool, str]:
  """
  Checks if an identical job already exists (active or archived).

  Duplicate logic can be customized via:
    - ignore_archived
    - ignore_conf_in_dup_check
    - ignore_commands_in_dup_check
  """
  global JOBS_CACHE
  from_archive = False

  # Cache key depends on whether config is part of the duplicate logic
  if ignore_conf_in_dup_check:
    cache_key = (cluster_name, tag)
  else:
    cache_key = (cluster_name, config_name, tag)

  if cache_key not in JOBS_CACHE:
    JOBS_CACHE[cache_key] = []
    exp_dir = get_experiments_dir()

    if ignore_conf_in_dup_check:
      # Ignore config level: cluster/*/tag/*/metadata.yaml
      glob_pattern = f"{cluster_name}/*/{tag}/*/metadata.yaml"
    else:
      glob_pattern = f"{cluster_name}/{config_name}/{tag}/*/metadata.yaml"

    # Scan active experiments
    for metadata_path in exp_dir.glob(glob_pattern):
      try:
        with open(metadata_path, 'r') as f:
          job_dict = yaml.safe_load(f)
        if job_dict:
          JOBS_CACHE[cache_key].append(job_dict)
      except Exception:
        continue

    if not ignore_archived:
      archive_root = get_archive_dir()

      if ignore_conf_in_dup_check:
        archive_glob_pattern = f"*/{cluster_name}/*/{tag}/*/metadata.yaml"
      else:
        archive_glob_pattern = (
          f"*/{cluster_name}/{config_name}/{tag}/*/metadata.yaml"
        )

      for metadata_path in archive_root.glob(archive_glob_pattern):
        try:
          with open(metadata_path, 'r') as f:
            job_dict = yaml.safe_load(f)
          if job_dict:
            from_archive = True
            JOBS_CACHE[cache_key].append(job_dict)
        except Exception:
          continue

  # Duplicate check
  for job_dict in JOBS_CACHE[cache_key]:

    # If we ignore command-level comparison, tag (+ optional config rule) is enough
    if ignore_commands_in_dup_check:
      return True, 'archive' if from_archive else 'active'

    # Otherwise perform full comparison
    if (
      job_dict.get('command') == command and
      job_dict.get('preprocess') == preprocess and
      job_dict.get('postprocess') == postprocess
    ):
      return True, 'archive' if from_archive else 'active'

  return False, ''


def register_job(job: Job):
  """
  Registers a new job in the cache to avoid disk reads on subsequent checks.
  """
  global JOBS_CACHE
  cache_key = (job.cluster_name, job.config_name, job.tag)
  if cache_key in JOBS_CACHE:
    JOBS_CACHE[cache_key].append(asdict(job))

def _load_job_metadata(metadata_path: Path, variables: Optional[Dict[str, Any]] = None) -> Optional[Job]:
  try:
    with open(metadata_path, 'r') as f:
      job_dict = yaml.load(f, Loader=SafeLoader)
      if job_dict:
        if variables:
          job_vars = job_dict.get('variables') or {}
          match = True
          for k, v in variables.items():
            if str(job_vars.get(k)) != str(v):
              match = False
              break
          if not match:
            return None
        return Job(**job_dict)
  except Exception:
    return None
  return None

def _get_matching_subdirs(parent: Path, pattern: Optional[str]) -> List[Path]:
    """
    Helper to get subdirectories matching a pattern (exact or wildcard).
    Uses os.scandir for performance to avoid unnecessary stat calls.
    """
    if not parent.exists():
        return []
    
    results = []
    try:
        if not pattern:
            # No pattern means "all directories"
            with os.scandir(parent) as it:
                for entry in it:
                    if entry.is_dir():
                        results.append(Path(entry.path))
        
        elif set('*?[').intersection(pattern):
            # Pattern contains wildcards
            with os.scandir(parent) as it:
                for entry in it:
                    if entry.is_dir() and fnmatch.fnmatch(entry.name, pattern):
                        results.append(Path(entry.path))
        else:
            # Exact match optimization
            target = parent / pattern
            if target.exists() and target.is_dir():
                results.append(target)
    except OSError:
        pass
        
    return results

def jobs_list(
  cluster_name: Optional[str] = None,
  config_name: Optional[str] = None,
  tag: Optional[str] = None,
  status: Optional[List[Status]] = None,
  archive_name: Optional[str] = None,
  from_active: bool = True,
  from_archived: bool = False,
  update_jobs: bool = True,
  variables: Optional[Dict[str, Any]] = None
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
    variables: Filter by variable values.
  Returns:
    A list of Job objects matching the filter criteria.
  Raises:
    ArchiveExistsError: If an archive with the specified name already exists and overwrite is False.
  """
  jobs = []
  exp_dir = get_experiments_dir()

  if update_jobs:
    update_jobs_status()
  
  paths_to_process = []

  # Scan active jobs
  if from_active:
    exp_dir = get_experiments_dir()
    
    # Construct a more specific glob pattern if filters are available
    # Structure: cluster/config/tag/timestamp/metadata.yaml
    # We use fixed depth to avoid scanning subdirectories (which is very slow)
    # parts = [
    #     cluster_name or "*",
    #     config_name or "*",
    #     tag or "*",
    #     "*", # timestamp
    #     "metadata.yaml"
    # ]
    # glob_pattern = "/".join(parts)

    # for metadata_path in exp_dir.glob(glob_pattern):
    #   # Apply filters based on path structure BEFORE reading file
    #   # Active: .../cluster/config/tag/timestamp/metadata.yaml (parts[-5] is cluster)
    #   if cluster_name and not fnmatch.fnmatch(metadata_path.parts[-5], cluster_name):
    #       continue
    #   if config_name and not fnmatch.fnmatch(metadata_path.parts[-4], config_name):
    #       continue
    #   if tag and not fnmatch.fnmatch(metadata_path.parts[-3], tag):
    #       continue
    #   paths_to_process.append(metadata_path)
    
    # Optimized scanning: iterate directory levels manually to avoid full glob scan
    # Level 1: Cluster
    clusters = _get_matching_subdirs(exp_dir, cluster_name)
    for cluster_dir in clusters:
        
        # Level 2: Config
        configs = _get_matching_subdirs(cluster_dir, config_name)
        for config_dir in configs:

            # Level 3: Tag
            tags = _get_matching_subdirs(config_dir, tag)
            for tag_dir in tags:

                # Level 4: Timestamp (always scan all timestamps)
                try:
                    with os.scandir(tag_dir) as it:
                        for entry in it:
                            if entry.is_dir():
                                # Optimistically assume metadata.yaml exists to avoid stat() call
                                paths_to_process.append(Path(entry.path) / "metadata.yaml")
                except OSError:
                    continue

  # Scan archived jobs
  if from_archived:
    archive_root = get_archive_dir()
    
    # Construct a more specific glob pattern if filters are available
    # Archive structure: archive_name/cluster_name/config_name/tag/timestamp/metadata.yaml
    # parts = [
    #     archive_name or "*",
    #     cluster_name or "*",
    #     config_name or "*",
    #     tag or "*",
    #     "*", # timestamp
    #     "metadata.yaml"
    # ]
    # glob_pattern = "/".join(parts)

    # for metadata_path in archive_root.glob(glob_pattern):
    #   # Apply filters based on path structure BEFORE reading file
    #   # Archive: .../archive_name/cluster/config/tag/timestamp/metadata.yaml (parts[-6] is archive_name)
    #   if cluster_name and not fnmatch.fnmatch(metadata_path.parts[-5], cluster_name):
    #       continue
    #   if config_name and not fnmatch.fnmatch(metadata_path.parts[-4], config_name):
    #       continue
    #   if tag and not fnmatch.fnmatch(metadata_path.parts[-3], tag):
    #       continue
    #   if archive_name and not fnmatch.fnmatch(metadata_path.parts[-6], archive_name):
    #       continue
    #   paths_to_process.append(metadata_path)

    # Optimized scanning for archives
    archives = _get_matching_subdirs(archive_root, archive_name)
    for archive_dir in archives:

        # Level 1: Cluster
        clusters = _get_matching_subdirs(archive_dir, cluster_name)
        for cluster_dir in clusters:
            
            # Level 2: Config
            configs = _get_matching_subdirs(cluster_dir, config_name)
            for config_dir in configs:

                # Level 3: Tag
                tags = _get_matching_subdirs(config_dir, tag)
                for tag_dir in tags:

                    # Level 4: Timestamp
                    try:
                        with os.scandir(tag_dir) as it:
                            for entry in it:
                                if entry.is_dir():
                                    # Optimistically assume metadata.yaml exists to avoid stat() call
                                    paths_to_process.append(Path(entry.path) / "metadata.yaml")
                    except OSError:
                        continue

  # Use a higher number of workers for I/O bound tasks
  max_workers = min(100, len(paths_to_process) + 1)
  with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
      future_to_path = {executor.submit(_load_job_metadata, p, variables): p for p in paths_to_process}
      for future in concurrent.futures.as_completed(future_to_path):
          job = future.result()
          if job:
              jobs.append(job)
  
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
  variables: Optional[Dict[str, Any]] = None
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
    variables: Filter jobs by variable values.

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
    update_jobs=False,
    variables=variables
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

def _update_single_job_status(job: Job) -> bool:
    """
    Helper function to update a single job's status.
    Returns True if the status was updated, False otherwise.
    """
    if job.status in TERMINAL_STATES:
      return False

    try:
      config = job.get_job_config()
      new_status = config.get_job_status(job.job_id).value
      
      if new_status == Status.UNKNOWN.value:
        return False

      if new_status != job.status:
        job.status = new_status
        job.write_metadata()
        return True
    except Exception:
      # Ignore errors (e.g., config not found) and continue
      return False
    return False

def update_jobs_status() -> int:
  """
  Updates the status of active jobs on the current cluster by querying the scheduler.
  
  Returns:
    The number of jobs whose status was updated.
  """
  current_cluster = get_cluster_name()
  active_jobs = jobs_list(cluster_name=current_cluster, from_active=True, from_archived=False, update_jobs=False)
  
  updated_count = 0

  with concurrent.futures.ThreadPoolExecutor() as executor:
      futures = [executor.submit(_update_single_job_status, job) for job in active_jobs]
      for future in concurrent.futures.as_completed(futures):
          if future.result():
              updated_count += 1
          
  return updated_count


def count_active_jobs() -> int:
  """
  Counts the number of jobs that are currently queued or running by querying squeue.
  
  Returns:
    The number of jobs with QUEUED or RUNNING status.
  """
  import subprocess
  
  try:
    result = subprocess.run(
      ["squeue", "--me", "-h", "-t", "PENDING,RUNNING"],
      capture_output=True,
      text=True,
      timeout=30
    )
    if result.returncode == 0:
      lines = [line for line in result.stdout.strip().split('\n') if line.strip()]
      return len(lines)
  except Exception:
    pass
  
  return 0