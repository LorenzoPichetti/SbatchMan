import shutil
from typing import List, Optional

import yaml

from sbatchman.config.project_config import get_archive_dir, get_experiments_dir
from sbatchman.core.launcher import Job
from sbatchman.exceptions import ArchiveExistsError

def list_jobs(
  hostname: Optional[str] = None,
  config_name: Optional[str] = None,
  tag: Optional[str] = None,
  include_archived: bool = False
) -> List[Job]:
  """
  Lists active and optionally archived jobs, with optional filtering.
  """
  jobs = []
  exp_dir = get_experiments_dir()
  
  # Scan active jobs
  for metadata_path in exp_dir.glob("**/metadata.yaml"):
    with open(metadata_path, 'r') as f:
      job_dict = yaml.safe_load(f)
      # Apply filters
      if hostname and not metadata_path.parts[-5] == hostname:
        continue
      if config_name and not metadata_path.parts[-4] == config_name:
        continue
      if tag and not metadata_path.parts[-3] == tag:
        continue
      jobs.append(Job(**job_dict))

  # Scan archived jobs
  if include_archived:
    archive_root = get_archive_dir()
    for metadata_path in archive_root.glob("*/**/metadata.yaml"):
      with open(metadata_path, 'r') as f:
        job_dict = yaml.safe_load(f)
        # Apply filters
        if hostname and not metadata_path.parts[-5] == hostname:
          continue
        if config_name and not metadata_path.parts[-4] == config_name:
          continue
        if tag and not metadata_path.parts[-3] == tag:
          continue
        jobs.append(Job(**job_dict))
  
  return jobs
  
def archive_jobs(archive_name: str, overwrite: bool = False, hostname: Optional[str] = None, config_name: Optional[str] = None, tag: Optional[str] = None) -> List[Job]:
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
  
  jobs_to_archive = list_jobs(include_archived=False, hostname=hostname, config_name=config_name, tag=tag)

  exp_dir_root = get_experiments_dir()
  
  for job in jobs_to_archive:
    source_job_dir = exp_dir_root / job["exp_dir"]
    if not source_job_dir.exists():
      continue

    # Update metadata before moving
    job["archive_name"] = archive_name
    
    # Move to archive
    dest_job_dir = archive_path / job["exp_dir"]
    dest_job_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(source_job_dir), str(dest_job_dir))

    # Rewrite metadata in new location
    with open(dest_job_dir / "metadata.yaml", "w") as f:
      yaml.safe_dump(job, f, default_flow_style=False)

  return jobs_to_archive
