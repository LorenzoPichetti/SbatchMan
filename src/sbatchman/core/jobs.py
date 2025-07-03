import shutil
from typing import List, Optional

import yaml

from sbatchman.config.project_config import get_archive_dir, get_experiments_dir
from sbatchman.core.launcher import Job
from sbatchman.exceptions import ArchiveExistsError
import pandas as pd

def jobs_list(
  cluster_name: Optional[str] = None,
  config_name: Optional[str] = None,
  tag: Optional[str] = None,
  archive_name: Optional[str] = None,
  from_active: bool = True,
  from_archived: bool = False,
) -> List[Job]:
  """
  Lists active and optionally archived jobs, with optional filtering.
  """
  jobs = []
  exp_dir = get_experiments_dir()
  
  # Scan active jobs
  if from_active:
    exp_dir = get_experiments_dir()
    for metadata_path in exp_dir.glob("**/metadata.yaml"):
      with open(metadata_path, 'r') as f:
        job_dict = yaml.safe_load(f)
        # Apply filters
        if cluster_name and not metadata_path.parts[-5] == cluster_name:
          continue
        if config_name and not metadata_path.parts[-4] == config_name:
          continue
        if tag and not metadata_path.parts[-3] == tag:
          continue
        jobs.append(Job(**job_dict))

  # Scan archived jobs
  if from_archived:
    archive_root = get_archive_dir()
    for metadata_path in archive_root.glob("*/**/metadata.yaml"):
      with open(metadata_path, 'r') as f:
        job_dict = yaml.safe_load(f)
        # Apply filters
        if cluster_name and not metadata_path.parts[-5] == cluster_name:
          continue
        if config_name and not metadata_path.parts[-4] == config_name:
          continue
        if tag and not metadata_path.parts[-3] == tag:
          continue
        if archive_name and not metadata_path.parts[-6] == archive_name:
          continue
        jobs.append(Job(**job_dict))
  
  return jobs

def jobs_df(
  cluster_name: Optional[str] = None,
  config_name: Optional[str] = None,
  tag: Optional[str] = None,
  include_archived: bool = False
) -> pd.DataFrame:
  """
  Returns a pandas DataFrame of jobs, with optional filtering.
  """
  jobs = jobs_list(
    cluster_name=cluster_name,
    config_name=config_name,
    tag=tag,
    from_archived=include_archived
  )
  jobs_dicts = [job.__dict__ for job in jobs]
  return pd.DataFrame(jobs_dicts)
  
def archive_jobs(archive_name: str, overwrite: bool = False, cluster_name: Optional[str] = None, config_name: Optional[str] = None, tag: Optional[str] = None) -> List[Job]:
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
  
  jobs_to_archive = jobs_list(from_archived=False, cluster_name=cluster_name, config_name=config_name, tag=tag)

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
    with open(dest_job_dir / "metadata.yaml", "w") as f:
      yaml.safe_dump(job, f, default_flow_style=False)

  return jobs_to_archive

def delete_jobs(
  cluster_name: Optional[str] = None,
  config_name: Optional[str] = None,
  tag: Optional[str] = None,
  archive_name: Optional[str] = None,
  all_archived: bool = False,
  not_archived: bool = False,
) -> int:
  """
  Deletes jobs matching the filter criteria.

  Args:
    cluster_name: Filter by cluster name.
    config_name: Filter by configuration name.
    tag: Filter by tag.
    archive_name: If provided, only delete jobs from this archive.
    all_archived: If True, delete only archived jobs.
    not_archived: If True, delete only active jobs.

  Returns:
    The number of deleted jobs.
  """
  jobs_to_delete = jobs_list(
    cluster_name=cluster_name,
    config_name=config_name,
    tag=tag,
    archive_name=archive_name,
    from_active=not_archived,
    from_archived=all_archived
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

  return len(jobs_to_delete)