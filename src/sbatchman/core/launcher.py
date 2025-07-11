import subprocess
import datetime
import itertools
import re
import yaml
from pathlib import Path
from typing import Any, Dict, List, Optional
from rich.console import Console

from sbatchman.core.job import Job, Status
from sbatchman.core.jobs_manager import job_exists
from sbatchman.exceptions import ConfigurationError, ClusterNameNotSetError, ConfigurationNotFoundError, JobExistsError, JobSubmitError
from sbatchman.config.global_config import get_cluster_name
from sbatchman.config.project_config import get_project_config_dir, get_scheduler_from_cluster_name

from sbatchman.config.project_config import get_experiments_dir
from sbatchman.schedulers.local import local_submit
from sbatchman.schedulers.pbs import pbs_submit
from sbatchman.schedulers.slurm import slurm_submit

console = Console()

def launch_job(
  config_name: str,
  command: str,
  cluster_name: Optional[str] = None,
  tag: str = "default",
  preprocess: Optional[str] = None,
  postprocess: Optional[str] = None,
  force: bool = False,
  previous_job_id: Optional[int] = None,
) -> Job:
  """
  Launches an experiment based on a configuration name.
  Args:
    config_name: The name of the configuration to use.
    command: The command to run for this job.
    cluster_name: Optional; if not provided, will use the global cluster name.
    tag: A tag for this experiment run, used in directory structure.
    preprocess: Optional; a command to run before the main command.
    postprocess: Optional; a command to run after the main command.
    previous_job_id: Optional; if this is set, the job will be only launched after the previous is done.
  Returns:
    A Job object representing the launched job.
  Raises:
    ConfigurationError: If the configuration is not found or if the cluster name is not set.
    ClusterNameNotSetError: If the cluster name is not set globally and not provided.
    ConfigurationNotFoundError: If the configuration file does not exist.
    JobSubmitError: If there is an error during job submission.
  """

  if not cluster_name:
    try:
      cluster_name = get_cluster_name()
    except ClusterNameNotSetError:
      raise ConfigurationError(
        "Cluster name not specified and not set globally. "
        "Please provide '--cluster-name' or use 'sbatchman set-cluster-name <cluster_name>' to set a global default."
      )

  scheduler = get_scheduler_from_cluster_name(cluster_name)

  config_path = get_project_config_dir() / cluster_name / f"{config_name}.sh"
  if not config_path.exists():
    raise ConfigurationNotFoundError(f"Configuration '{config_name}' for cluster '{cluster_name}' not found at '{config_path}'.")
  template_script = open(config_path, "r").read()


  if job_exists(command, config_name, cluster_name, tag, preprocess, postprocess) and not force:
    raise JobExistsError(
      f"An identical job already exists for config '{config_name}' with tag '{tag}'. "
      "Use '--force' to submit it anyway"
    )

  # Capture the Current Working Directory at the time of launch
  submission_cwd = Path.cwd()
    
  # 2. Create a unique, nested directory for this experiment run
  timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
  # Directory structure: <cluster_name>/<config_name>/<tag>/<timestamp>
  # Find a directory name that has not been used yet
  base_exp_dir_local = Path(cluster_name) / config_name / tag / timestamp
  exp_dir_local = base_exp_dir_local
  exp_dir = get_experiments_dir() / exp_dir_local
  counter = 1
  while exp_dir.exists():
    exp_dir_local = base_exp_dir_local.with_name(f"{base_exp_dir_local.name}_{counter}")
    exp_dir = get_experiments_dir() / exp_dir_local
    counter += 1
  exp_dir.mkdir(parents=True, exist_ok=False)

  # 3. Prepare the final runnable script
  # Replace placeholders for log and CWD
  final_script_content = template_script.replace(
    "{EXP_DIR}", str(exp_dir.resolve())
  ).replace(
    "{CWD}", str(submission_cwd.resolve())
  ).replace(
    "{PREPROCESS}", str(preprocess) if preprocess is not None else ''
  ).replace(
    "{CMD}", str(command)
  ).replace(
    "{POSTPROCESS}", str(postprocess) if postprocess is not None else ''
  )
  
  run_script_path = exp_dir / "run.sh"
  with open(run_script_path, "w") as f:
    f.write(final_script_content)
  run_script_path.chmod(0o755)

  job = Job(
    config_name=config_name,
    cluster_name=cluster_name,
    timestamp=timestamp,
    exp_dir=str(exp_dir_local),
    command=command,
    status=Status.SUBMITTING.value,
    scheduler=scheduler,
    job_id=0,
    tag=tag,
    archive_name=None,
    preprocess=preprocess,
    postprocess=postprocess,
  )

  job.write_metadata()

  try:
    # 5. Submit the job using the scheduler's own logic
    if scheduler == 'slurm':
      job.job_id = slurm_submit(run_script_path, exp_dir, previous_job_id)
    elif scheduler == 'pbs':
      job.job_id = pbs_submit(run_script_path, exp_dir, previous_job_id)
    elif scheduler == 'local':
      console.print(f"✅ Submitting job with command '[bold cyan]{job.command}[/bold cyan]'.")
      job.job_id = local_submit(run_script_path, exp_dir)
    else:
      raise ConfigurationError(f"No submission class found for scheduler '{scheduler}'. Supported schedulers are: slurm, pbs, local.")
    
    job.write_job_id()
  
  except (subprocess.CalledProcessError, ValueError, FileNotFoundError) as e:
    job.status = Status.FAILED_SUBMISSION.value
    job.write_metadata()
    raise
  finally:    
    return job


def _load_variable_values(var_value):
  # If var_value is a list, return as is
  if isinstance(var_value, list):
    return var_value
  # If var_value is a string and a file, read lines
  elif isinstance(var_value, str):
    path = Path(var_value)
    if path.is_file():
      with open(path, "r") as f:
        return [line.strip().replace('\n', '') for line in f if line.strip()]
    elif path.is_dir():
      # Return sorted list of file names in the directory
      return sorted([str(p.absolute()) for p in path.iterdir() if p.is_file()])
    else:
      raise JobSubmitError(
        f"Variable value '{var_value}' is not a list, file, or directory.\n"
        "YAML script semantics:\n"
        "- Variables can be lists, a path to a file (one value per line), or a path to a directory (all file absolute paths used as values).\n"
        "- The cartesian product of all variable values is used to generate jobs.\n"
        "- Experiments can define configuration names (possibly using variables) and tags.\n"
        "- 'command' and 'variables' can be redefined or extended in inner YAML tags.\n"
        "- The '{var_name}' syntax is substituted with the actual value of 'var_name'."
      )
  else:
    return [var_value]


def _merge_dicts(base, override):
  # Recursively merge two dictionaries
  result = dict(base)
  for k, v in override.items():
    if k in result and isinstance(result[k], dict) and isinstance(v, dict):
      result[k] = _merge_dicts(result[k], v)
    else:
      result[k] = v
  return result


def _substitute(template, variables):
  # Replace {var} in template with values from variables
  if not isinstance(template, str):
    return template
  return template.format(**variables)


def _extract_used_vars(*templates):
  """Extract variable names used in {var} format from given templates."""
  var_names = set()
  for template in templates:
    if isinstance(template, str):
      var_names.update(re.findall(r"{(\w+)}", template))
  return var_names

def launch_jobs_from_file(jobs_file_path: Path, force: bool = False) -> List[Job]:
  """  Launches jobs based on a YAML configuration file.
  Args:
    jobs_file_path: Path to the YAML file containing job definitions.
    force: If True, will overwrite existing jobs with the same configuration.
  Returns:
    A list of Job objects representing the launched jobs.
  Raises:
    ConfigurationError: If the jobs file is not found or has invalid syntax.
  """

  with open(jobs_file_path, "r") as f:
    config = yaml.safe_load(f)

  global_is_sequential = bool(config.get("sequential"))
  global_vars = config.get("variables", {})
  global_command = config.get("command", None)
  global_preprocess = config.get("preprocess", None)
  global_postprocess = config.get("postprocess", None)

  if global_is_sequential:
    console.print('[yellow]Jobs will be scheduled sequentially.[/yellow]')
  
  # Prepare global variable values (expand files if needed)
  expanded_global_vars = {k: _load_variable_values(v) for k, v in global_vars.items()}
  
  launched_jobs = []
  job_definitions = config.get("jobs", [])
  previous_job_id = None

  for job_def in job_definitions:
    job_config_template = job_def.get("config")
    if not job_config_template:
      continue # Skip job definition if it has no config

    job_command_template = job_def.get("command", global_command)
    job_preprocess_template = job_def.get("preprocess", global_preprocess)
    job_postprocess_template = job_def.get("postprocess", global_postprocess)
    job_vars = job_def.get("variables", {})

    expanded_job_vars = {k: _load_variable_values(v) for k, v in job_vars.items()}

    # Merge global and job-specific variables
    merged_job_vars = {**expanded_global_vars, **expanded_job_vars}

    config_jobs = job_def.get("config_jobs", [])
    if not config_jobs:
      # If no config_jobs, run with the job's own context
      previous_job_id = _launch_job_combinations(
        job_config_template,
        job_command_template,
        "default",
        job_preprocess_template,
        job_postprocess_template,
        merged_job_vars,
        launched_jobs,
        force,
        global_is_sequential,
        previous_job_id,
      )
    else:
      for entry in config_jobs:
        tag_name = entry.get("tag")
        if not tag_name:
          continue # Skip matrix entry if it has no tag

        entry_command_template = entry.get("command", job_command_template)
        entry_preprocess_template = entry.get("preprocess", job_preprocess_template)
        entry_postprocess_template = entry.get("postprocess", job_postprocess_template)
        entry_vars = entry.get("variables", {})
        expanded_entry_vars = {k: _load_variable_values(v) for k, v in entry_vars.items()}
        
        # Merge all variables: global -> job -> entry
        final_vars = {**merged_job_vars, **expanded_entry_vars}

        previous_job_id = _launch_job_combinations(
          job_config_template,
          entry_command_template,
          tag_name,
          entry_preprocess_template,
          entry_postprocess_template,
          final_vars,
          launched_jobs,
          force,
          global_is_sequential,
          previous_job_id,
        )

  return launched_jobs

def _launch_job_combinations(
  config_template: str,
  command_template: str,
  tag: str,
  preprocess_template: Optional[str],
  postprocess_template: Optional[str],
  variables: Dict[str, Any],
  launched_jobs: List[Job],
  force: bool = False,
  sequential: bool = False,
  previous_job_id: Optional[int] = None,
) -> Optional[int]:
    """
    Generates and launches jobs for all combinations of variables.

    Returns: the id of the last submitted job
    """
    if not command_template:
      return

    # Determine which variables are actually used in the templates
    used_vars = _extract_used_vars(config_template, command_template, tag, preprocess_template, postprocess_template)
    filtered_vars = {k: v for k, v in variables.items() if k in used_vars}

    if not filtered_vars:
      # If no variables are used, launch a single job
      config_name = _substitute(config_template, {})
      command = _substitute(command_template, {})
      job_tag = _substitute(tag, {})
      preprocess = _substitute(preprocess_template, {})
      postprocess = _substitute(postprocess_template, {})
      # print('='*40)
      # print(f'{config_name=}')
      # print(f'{preprocess=}')
      # print(f'{command=}')
      # print(f'{postprocess=}')
      # print(f'{job_tag=}')
      # print('='*40)
      try:
        job = launch_job(config_name, command, tag=job_tag, preprocess=preprocess, postprocess=postprocess, force=force, previous_job_id=(previous_job_id if sequential else None))
        launched_jobs.append(job)
        previous_job_id = job.job_id
      except JobExistsError as e:
        print(f"Skipping job: {e.message}")
      except JobSubmitError as e:
        print(f"Failed to submit job: {e.message}")
      return previous_job_id

    keys, values = zip(*filtered_vars.items())
    for combination in itertools.product(*values):
      var_dict = dict(zip(keys, combination))
      config_name = _substitute(config_template, var_dict)
      command = _substitute(command_template, var_dict)
      job_tag = _substitute(tag, var_dict)
      preprocess = _substitute(preprocess_template, var_dict)
      postprocess = _substitute(postprocess_template, var_dict)
      # print('='*40)
      # print(f'{config_name=}')
      # print(f'{preprocess=}')
      # print(f'{command=}')
      # print(f'{postprocess=}')
      # print(f'{job_tag=}')
      # print('='*40)
      try:
        job = launch_job(config_name, command, tag=job_tag, preprocess=preprocess, postprocess=postprocess, force=force, previous_job_id=(previous_job_id if sequential else None))
        launched_jobs.append(job)
        previous_job_id = job.job_id
      except JobExistsError as e:
        print(f"Skipping job: {e.message}")
      except JobSubmitError as e:
        print(f"Failed to submit job: {e.message}")
    
    return previous_job_id