import typer
from typing import List, Optional
from rich.console import Console
from pathlib import Path

import sbatchman.api as api
from sbatchman.config import global_config
from sbatchman.exceptions import ProjectNotInitializedError, SbatchManError

from .tui.tui import run_tui

app = typer.Typer(help="A utility to create, launch, and monitor code experiments.")
configure_app = typer.Typer(help="Create a configuration for a scheduler.")
app.add_typer(configure_app, name="configure")

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
      api.init_project(Path.cwd())
      console.print("[green]✓[/green] SbatchMan project created successfully. Please re-run your previous command.")
    except SbatchManError as e:
      console.print(f"[bold red]Error:[/bold red] {e}")
      raise typer.Exit(code=1)
  else:
    console.print("Aborted. Please run 'sbatchman init' in your desired project root.")
    raise typer.Exit(code=1)

def _save_config_print(name: str, config_path: Path):
  console.print(f"✅ Configuration '[bold cyan]{name}[/bold cyan]' saved to {config_path}")

@app.callback()
def main_callback(ctx: typer.Context):
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
    api.init_project(path)
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
  env: Optional[List[str]] = typer.Option(None, "--env", help="Environment variables to set (e.g., VAR=value). Can be used multiple times."),
  module: Optional[List[str]] = typer.Option(None, "--module", help="Module to load. Can be used multiple times."),
  overwrite: bool = typer.Option(False, "--overwrite", help="Overwrite current configuration."),
):
  """Creates a SLURM configuration."""
  while True:
    try:
      config_path = api.create_slurm_config(
        name=name, cluster_name=cluster_name,
        partition=partition, nodes=nodes, ntasks=ntasks, cpus_per_task=cpus_per_task, mem=mem, account=account,
        time=time, gpus=gpus, constraint=constraint, nodelist=nodelist, qos=qos, reservation=reservation,
        env=env, modules=module, overwrite=overwrite
      )
      _save_config_print(name, config_path)
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
  env: Optional[List[str]] = typer.Option(None, "--env", help="Environment variables to set (e.g., VAR=value)."),
  module: Optional[List[str]] = typer.Option(None, "--module", help="Module to load. Can be used multiple times."),
  overwrite: bool = typer.Option(False, "--overwrite", help="Overwrite current configuration."),
):
  """Creates a PBS configuration."""
  while True:
    try:
      config_path = api.create_pbs_config(name=name, cluster_name=cluster_name, queue=queue, cpus=cpus, mem=mem, walltime=walltime, env=env, modules=module, overwrite=overwrite)
      _save_config_print(name, config_path)
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
  env: Optional[List[str]] = typer.Option(None, "--env", help="Environment variables to set (e.g., VAR=value)."),
  module: Optional[List[str]] = typer.Option(None, "--module", help="Module to load. Can be used multiple times."),
  overwrite: bool = typer.Option(False, "--overwrite", help="Overwrite current configuration."),
):
  """Creates a configuration for local execution."""
  while True:
    try:
      config_path = api.create_local_config(name=name, env=env, modules=module, cluster_name=cluster_name, overwrite=overwrite)
      _save_config_print(name, config_path)
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
      api.create_configs_from_file(file, overwrite)
      console.print(f"✅ Configurations from '[bold cyan]{file.name}[/bold cyan]' loaded successfully.")
    except SbatchManError as e:
      console.print(f"[bold red]Error:[/bold red] {e}")
      raise typer.Exit(1)
  elif ctx.invoked_subcommand is None:
    console.print(ctx.get_help())

@app.command("launch")
def launch(
  jobs_file: Optional[Path] = typer.Option(None, "--jobs_file", help="YAML file that describes a batch of experiments."),
  config_name: Optional[str] = typer.Option(None, "--config_name", help="Configuration name."),
  tag: str = typer.Option("default", "--tag", help="Tag for this experiment (default: 'default')."),
  command: Optional[str] = typer.Argument(None, help="The executable and its parameters, enclosed in quotes."),
):
  """Launches an experiment (or a batch of experiments) using a predefined configuration."""

  try:
    # Call the API/launcher
    if jobs_file:
      jobs = api.launch_jobs_from_file(jobs_file)
      console.print(f"✅ Submitted successfully {len(jobs)} jobs.")
    elif config_name and tag and command:
        job = api.launch_job(
          config_name=config_name,
          command=command,
          tag=tag
        )
        console.print(f"✅ Experiment for config '[bold cyan]{config_name}[/bold cyan]' submitted successfully.")
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

if __name__ == "__main__":
  app()