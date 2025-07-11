from sbatchman.schedulers.base import BaseConfig
import typer
from typing import List, Optional
from rich.console import Console
from pathlib import Path

import sbatchman as sbtc
from sbatchman.config import global_config
from sbatchman.exceptions import ProjectNotInitializedError, SbatchManError
import importlib.metadata

from .tui.tui import run_tui

console = Console()
app = typer.Typer(help="A utility to create, launch, and monitor code experiments.")
configure_app = typer.Typer(help="Create a configuration for a scheduler.")
app.add_typer(configure_app, name="configure")

def _handle_not_initialized():
  """Prints a helpful message when SbatchMan root directory is not found and asks to create it."""
  console.print("[bold yellow]Warning:[/bold yellow] SbatchMan project not initialized in this directory or any parent directory.")
  init_choice = typer.confirm(
    "Would you like to create a project in the current directory?",
    default=True,
  )
  if init_choice:
    try:
      sbtc.init_project(Path.cwd())
      console.print("[green]✓[/green] SbatchMan project created successfully. Please re-run your previous command.")
    except SbatchManError as e:
      console.print(f"[bold red]Error:[/bold red] {e}")
      raise typer.Exit(code=1)
  else:
    console.print("Aborted. Please run 'sbatchman init' in your desired project root.")
    raise typer.Exit(code=1)

def _save_config_print(config: BaseConfig):
  console.print(f"✅ Configuration '[bold cyan]{config.name}[/bold cyan]' saved to {config.template_path}")

def version_callback(value: bool):
  if not value:
    return
  try:
    sbatchman_version = importlib.metadata.version("sbatchman")
    console.print(f"SbatchMan version: [bold cyan]{sbatchman_version}[/bold cyan]")
  except importlib.metadata.PackageNotFoundError:
    console.print("[bold red]Error:[/bold red] Could not determine the version. Is SbatchMan installed correctly?")
    raise typer.Exit(1)
  raise typer.Exit()

@app.callback()
def main_callback(
    ctx: typer.Context,
    version: Optional[bool] = typer.Option(None, "--version", callback=version_callback, is_eager=True, help="Show the version and exit."),
  ):
  """
  SbatchMan CLI main callback.
  Handles global exceptions.
  """
  try:
    # This is a placeholder for any pre-command logic.
    # The actual command execution happens after this.
    pass
  except SbatchManError as e:
    console.print(f"[bold red]Error:[/bold red] {e}")
    raise typer.Exit(code=1)

@app.command("set-cluster-name")
def set_cluster_name(
  new_cluster_name: str = typer.Argument(..., help="The new name for this machine.")
):
  """
  Sets the machine of the machine (changes the global cluster name used by SbatchMan).
  """
  try:
    global_config.set_cluster_name(new_cluster_name)
    console.print(f"[green]✓[/green] Cluster name changed to '[bold]{new_cluster_name}[/bold]'.")
  except SbatchManError as e:
    console.print(f"[bold red]Error:[/bold red] {e}")
    raise typer.Exit(code=1)

@app.command()
def init(
  path: Path = typer.Argument(Path("."), help="The directory where the SbatchMan project folder should be created."),
):
  """Initializes a SbatchMan project and sets up global configuration if needed."""
  try:
    sbtc.init_project(path)
    console.print(f"[green]✓[/green] SbatchMan project initialized successfully in {path / 'SbatchMan'}")
  except SbatchManError as e:
    console.print(f"[bold red]Error:[/bold red] {e}")
    raise typer.Exit(code=1)

@configure_app.command("slurm")
def configure_slurm(
  name: str = typer.Option(..., "--name", help="A unique name for this configuration."),
  cluster_name: Optional[str] = typer.Option(None, "--cluster-name", help="The name of the machine where this configuration will be used."),
  partition: Optional[str] = typer.Option(None, help="SLURM partition name."),
  nodes: Optional[str] = typer.Option(None, help="SLURM number of nodes."),
  ntasks: Optional[str] = typer.Option(None, help="SLURM number of tasks."),
  cpus_per_task: Optional[int] = typer.Option(None, help="Number of CPUs per task."),
  mem: Optional[str] = typer.Option(None, help="Memory requirement (e.g., 16G, 64G)."),
  account: Optional[str] = typer.Option(None, help="SLURM account"),
  time: Optional[str] = typer.Option(None, help="Walltime (e.g., 01-00:00:00)."),
  gpus: Optional[int] = typer.Option(None, help="Number of GPUs."),
  constraint: Optional[str] = typer.Option(None, help="SLURM constraint."),
  nodelist: Optional[str] = typer.Option(None, help="SLURM nodelist."),
  qos: Optional[str] = typer.Option(None, help="SLURM quality of service (qos)."),
  reservation: Optional[str] = typer.Option(None, help="SLURM reservation."),
  env: Optional[List[str]] = typer.Option(None, "--env", help="Environment variables to set (e.g., VAR=value). Can be used multiple times (e.g., --env VAR1=value1 --env VAR2=value2)."),
  module: Optional[List[str]] = typer.Option(None, "--module", help="Module to load. Can be used multiple times (e.g. --module GCC/13.3.0 --module CUDA/12.5.0)."),
  overwrite: bool = typer.Option(False, "--overwrite", help="Overwrite current configuration."),
):
  """Creates a SLURM configuration."""
  while True:
    try:
      config = sbtc.create_slurm_config(
        name=name, cluster_name=cluster_name,
        partition=partition, nodes=nodes, ntasks=ntasks, cpus_per_task=cpus_per_task, mem=mem, account=account,
        time=time, gpus=gpus, constraint=constraint, nodelist=nodelist, qos=qos, reservation=reservation,
        env=env, modules=module, overwrite=overwrite
      )
      _save_config_print(config)
      break
    except ProjectNotInitializedError:
      _handle_not_initialized()
    except SbatchManError as e:
      console.print(f"[bold red]Error:[/bold red] {e}")
      raise typer.Exit(code=1)

@configure_app.command("pbs")
def configure_pbs(
  name: str = typer.Option(..., "--name", help="A unique name for this configuration."),
  cluster_name: Optional[str] = typer.Option(None, "--cluster-name", help="The name of the machine where this configuration will be used."),
  queue: Optional[str] = typer.Option(None, help="PBS queue name."),
  cpus: Optional[int] = typer.Option(None, help="Number of CPUs."),
  mem: Optional[str] = typer.Option(None, help="Memory requirement (e.g., 16gb, 64gb)."),
  walltime: Optional[str] = typer.Option(None, help="Walltime (e.g., 01:00:00)."),
  env: Optional[List[str]] = typer.Option(None, "--env", help="Environment variables to set (e.g., VAR=value). Can be used multiple times (e.g., --env VAR1=value1 --env VAR2=value2)."),
  module: Optional[List[str]] = typer.Option(None, "--module", help="Module to load. Can be used multiple times (e.g. --module GCC/13.3.0 --module CUDA/12.5.0)."),
  overwrite: bool = typer.Option(False, "--overwrite", help="Overwrite current configuration."),
):
  """Creates a PBS configuration."""
  while True:
    try:
      config = sbtc.create_pbs_config(name=name, cluster_name=cluster_name, queue=queue, cpus=cpus, mem=mem, walltime=walltime, env=env, modules=module, overwrite=overwrite)
      _save_config_print(config)
      break
    except ProjectNotInitializedError:
      _handle_not_initialized()
    except SbatchManError as e:
      console.print(f"[bold red]Error:[/bold red] {e}")
      raise typer.Exit(code=1)

@configure_app.command("local")
def configure_local(
  name: str = typer.Option(..., "--name", help="A unique name for this configuration."),
  cluster_name: Optional[str] = typer.Option(None, "--cluster-name", help="The name of the machine where this configuration will be used."),
  env: Optional[List[str]] = typer.Option(None, "--env", help="Environment variables to set (e.g., VAR=value). Can be used multiple times (e.g., --env VAR1=value1 --env VAR2=value2)."),
  overwrite: bool = typer.Option(False, "--overwrite", help="Overwrite current configuration."),
):
  """Creates a configuration for local execution."""
  while True:
    try:
      config = sbtc.create_local_config(name=name, env=env, cluster_name=cluster_name, overwrite=overwrite)
      _save_config_print(config)
      break
    except ProjectNotInitializedError:
      _handle_not_initialized()
    except SbatchManError as e:
      console.print(f"[bold red]Error:[/bold red] {e}")
      raise typer.Exit(code=1)

@configure_app.callback(invoke_without_command=True)
def configure(
  ctx: typer.Context,
  file: Optional[Path] = typer.Option(
    None,
    "--file",
    exists=True,
    file_okay=True,
    dir_okay=False,
    readable=True,
    help="YAML file with multiple configurations.",
  ),
  overwrite: bool = typer.Option(False, "--overwrite", help="Overwrite current configurations when using --file."),
):
  """
  Create or update job configurations.
  You can either specify a scheduler (slurm, pbs, local) or provide a --file.
  """
  if file:
    try:
      for config in sbtc.create_configs_from_file(file, overwrite):
        _save_config_print(config)
      console.print(f"✅ Configurations from '[bold cyan]{file.name}[/bold cyan]' loaded successfully.")
    except SbatchManError as e:
      console.print(f"[bold red]Error:[/bold red] {e}")
      raise typer.Exit(1)
  elif ctx.invoked_subcommand is None:
    console.print(ctx.get_help())

@app.command("launch")
def launch(
  file: Optional[Path] = typer.Option(None, "--file", help="YAML file that describes a batch of experiments."),
  config: Optional[str] = typer.Option(None, "--config", help="Configuration name."),
  tag: str = typer.Option("default", "--tag", help="Tag for this experiment (default: 'default')."),
  command: Optional[str] = typer.Argument(None, help="The executable and its parameters, enclosed in quotes."),
  preprocess: Optional[str] = typer.Option(None, "--preprocess", help="Command to run before the main job (optional)."),
  postprocess: Optional[str] = typer.Option(None, "--postprocess", 
  help="Command to run after the main job (optional)."),
  force: bool = typer.Option(False, "--force", help="Force submission even if identical jobs already exist.")
):
  """Launches an experiment (or a batch of experiments) using a predefined configuration.

  You can specify --preprocess and/or --postprocess to run commands before/after the main job.
  """

  try:
    # Call the API/launcher
    if file:
      jobs = sbtc.launch_jobs_from_file(file, force=force)
      console.print(f"✅ Submitted successfully {len(jobs)} jobs.")
    elif config and tag and command:
        job = sbtc.launch_job(
          config_name=config,
          command=command,
          tag=tag,
          preprocess=preprocess,
          postprocess=postprocess,
          force=force
        )
        console.print(f"✅ Experiment for config '[bold cyan]{config}[/bold cyan]' submitted successfully.")
        console.print(f"   ┣━ Job ID: {job.job_id}")
        console.print(f"   ┗━ Exp. Dir: {job.exp_dir}")
    else:
      console.print(f"[bold red]You must provide exactly on of: --jobs_file or (--config_name and --command)[/bold red]")
      raise typer.Exit(1)
  except SbatchManError as e:
    console.print(f"[bold red]Error:[/bold red] {e}")
    raise typer.Exit(1)

@app.command("status")
def status(
  experiments_dir: Optional[Path] = typer.Argument(None, help="Path to the experiments directory to monitor. Defaults to auto-detected SbatchMan/experiments.", exists=True, file_okay=False, dir_okay=True, readable=True)
):
  """Shows the status of all experiments in an interactive TUI."""
  try:
    run_tui(experiments_dir)
  except SbatchManError as e:
    console.print(f"[bold red]Error:[/bold red] {e}")
    raise typer.Exit(1)

@app.command("archive")
def archive(
    archive_name: str = typer.Argument(..., help="The name of the archive to create."),
    overwrite: bool = typer.Option(False, "--overwrite", help="Overwrite existing archive with the same name."),
    cluster_name: Optional[str] = typer.Option(None, "--cluster-name", help="Archive jobs from this cluster."),
    config_name: Optional[str] = typer.Option(None, "--config", help="Archive jobs with this configuration name."),
    tag: Optional[str] = typer.Option(None, "--tag", help="Archive jobs with this tag."),
):
  """Archives jobs, moving them from the active experiments directory to an archive location."""
  try:
    archived_count = sbtc.archive_jobs(
      archive_name=archive_name,
      overwrite=overwrite,
      cluster_name=cluster_name,
      config_name=config_name,
      tag=tag
    )
    console.print(f"[green]✓[/green] Successfully archived {len(archived_count)} jobs.")
  except ProjectNotInitializedError:
    _handle_not_initialized()
  except SbatchManError as e:
    console.print(f"[bold red]Error:[/bold red] {e}")
    raise typer.Exit(1)

@app.command("delete-jobs")
def delete_jobs(
  cluster_name: Optional[str] = typer.Option(None, "--cluster-name", help="Delete jobs from this cluster."),
  config_name: Optional[str] = typer.Option(None, "--config", help="Delete jobs with this configuration name."),
  tag: Optional[str] = typer.Option(None, "--tag", help="Delete jobs with this tag."),
  archive_name: Optional[str] = typer.Option(None, "--archive", help="Delete jobs from this archive."),
  archived: bool = typer.Option(False, "--archived", help="Delete only archived jobs."), 
  not_archived: bool = typer.Option(False, "--not-archived", help="Delete only active jobs."),
  all: bool = typer.Option(False, "--all", help="Delete ALL jobs."),
):
  """Deletes jobs matching the specified criteria."""

  if all:
    archived = True
    not_archived = True

  if not archived and not not_archived:
    console.print("[bold red]You must specify at least one of: --archived or --not-archived[/bold red]")
    raise typer.Exit(1)
  
  try:
    deleted_count = sbtc.delete_jobs(
      cluster_name=cluster_name,
      config_name=config_name,
      tag=tag,
      archive_name=archive_name,
      archived=archived,
      not_archived=not_archived,
    )
    console.print(f"✅ Successfully deleted {deleted_count} jobs.")
  except ProjectNotInitializedError:
    _handle_not_initialized()
  except SbatchManError as e:
    console.print(f"[bold red]Error:[/bold red] {e}")
    raise typer.Exit(1)

@app.command("update-jobs-status")
def update_jobs_status(
):
  """Updates the status of all jobs in the experiments directory."""
  try:
    updated_count = sbtc.update_jobs_status()
    console.print(f"✅ Successfully updated status for {updated_count} jobs.")
  except ProjectNotInitializedError:
    _handle_not_initialized()
  except SbatchManError as e:
    console.print(f"[bold red]Error:[/bold red] {e}")
    raise typer.Exit(1)

if __name__ == "__main__":
  app()