import typer
from typing import List, Optional
from rich.console import Console
from pathlib import Path

from .config import get_config_dir, get_experiments_dir
from .schedulers.slurm import SlurmScheduler
from .schedulers.pbs import PbsScheduler
from .schedulers.local import LocalScheduler
from .launcher import launch_experiment
from .tui import run_tui

app = typer.Typer(help="A utility to create, launch, and monitor code experiments.")
configure_app = typer.Typer(help="Create a configuration for a scheduler.")
app.add_typer(configure_app, name="configure")

console = Console()

def _save_config(name: str, content: str, output_dir: Optional[Path]):
  if output_dir:
    output_dir.mkdir(parents=True, exist_ok=True)
    config_path = output_dir / f"{name}.sh"
  else:
    config_path = get_config_dir() / f"{name}.sh"
  
  with open(config_path, "w") as f:
    f.write(content)
  console.print(f"✅ Configuration '[bold cyan]{name}[/bold cyan]' saved to {config_path}")

@configure_app.command("slurm")
def configure_slurm(
  name: str = typer.Option(..., "--name", help="A unique name for this configuration."),
  output_dir: Optional[Path] = typer.Option(None, "--output-dir", help="Directory to save the config file. Defaults to auto-detected SbatchMan/configs.", file_okay=False, dir_okay=True, writable=True),
  partition: Optional[str] = typer.Option(None, help="SLURM partition name."),
  nodes: Optional[int] = typer.Option(None, help="SLURM number of nodes."),
  ntasks: Optional[int] = typer.Option(None, help="SLURM number of tasks."),
  cpus_per_task: Optional[int] = typer.Option(None, help="Number of CPUs per task."),
  mem: Optional[str] = typer.Option(None, help="Memory requirement (e.g., 16G, 64G)."),
  time: Optional[str] = typer.Option(None, help="Walltime (e.g., 01-00:00:00)."),
  gpus: Optional[int] = typer.Option(None, help="Number of GPUs."),
  env: Optional[List[str]] = typer.Option(None, "--env", help="Environment variables to set (e.g., VAR=value). Can be used multiple times."),
):
  """Creates a SLURM configuration."""
  scheduler = SlurmScheduler()
  script_content = scheduler.generate_script(
    name=name, partition=partition, nodes=nodes, ntasks=ntasks,
    cpus_per_task=cpus_per_task, mem=mem, time=time, gpus=gpus, env=env
  )
  _save_config(name, script_content, output_dir)

@configure_app.command("pbs")
def configure_pbs(
  name: str = typer.Option(..., "--name", help="A unique name for this configuration."),
  output_dir: Optional[Path] = typer.Option(None, "--output-dir", help="Directory to save the config file. Defaults to auto-detected SbatchMan/configs.", file_okay=False, dir_okay=True, writable=True),
  queue: Optional[str] = typer.Option(None, help="PBS queue name."),
  cpus: Optional[int] = typer.Option(None, help="Number of CPUs."),
  mem: Optional[str] = typer.Option(None, help="Memory requirement (e.g., 16gb, 64gb)."),
  walltime: Optional[str] = typer.Option(None, help="Walltime (e.g., 01:00:00)."),
  env: Optional[List[str]] = typer.Option(None, "--env", help="Environment variables to set (e.g., VAR=value)."),
):
  """Creates a PBS/Torque configuration."""
  scheduler = PbsScheduler()
  script_content = scheduler.generate_script(name=name, queue=queue, cpus=cpus, mem=mem, walltime=walltime, env=env)
  _save_config(name, script_content, output_dir)

@configure_app.command("local")
def configure_local(
  name: str = typer.Option(..., "--name", help="A unique name for this configuration."),
  output_dir: Optional[Path] = typer.Option(None, "--output-dir", help="Directory to save the config file. Defaults to auto-detected SbatchMan/configs.", file_okay=False, dir_okay=True, writable=True),
  env: Optional[List[str]] = typer.Option(None, "--env", help="Environment variables to set (e.g., VAR=value)."),
):
  """Creates a configuration for local execution."""
  scheduler = LocalScheduler()
  script_content = scheduler.generate_script(name=name, env=env)
  _save_config(name, script_content, output_dir)

@app.command("launch")
def launch(
  config: str = typer.Option(..., "--config", "-c", help="The name of the configuration or a direct path to a config file."),
  command: str = typer.Argument(..., help="The executable and its parameters, enclosed in quotes."),
):
  """Launches an experiment using a predefined configuration."""
  try:
    launch_experiment(config, command)
  except FileNotFoundError as e:
    console.print(f"❌ Error: {e}")
    raise typer.Exit(code=1)
  except Exception as e:
    console.print(f"❌ An unexpected error occurred: {e}")
    raise typer.Exit(code=1)

@app.command("status")
def status(
  experiments_dir: Optional[Path] = typer.Argument(None, help="Path to the experiments directory to monitor. Defaults to auto-detected SbatchMan/experiments.", exists=True, file_okay=False, dir_okay=True, readable=True)
):
  """Shows the status of all experiments in an interactive TUI."""
  run_tui(experiments_dir)

if __name__ == "__main__":
  app()