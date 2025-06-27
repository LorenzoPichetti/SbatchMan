# src/exp_kit/cli.py
import typer
from typing import List, Optional
from rich.console import Console

from .config import CONFIG_DIR
from .schedulers.slurm import SlurmScheduler
from .schedulers.pbs import PbsScheduler
from .schedulers.local import LocalScheduler
from .launcher import launch_experiment
from .tui import run_tui

app = typer.Typer(help="A utility to create, launch, and monitor code experiments.")
configure_app = typer.Typer(help="Create a configuration for a scheduler.")
app.add_typer(configure_app, name="configure")

console = Console()

@configure_app.command("slurm")
def configure_slurm(
  name: str = typer.Option(..., "--name", help="A unique name for this configuration."),
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
  config_path = CONFIG_DIR / f"{name}.sh"
  with open(config_path, "w") as f:
    f.write(script_content)
  console.print(f"✅ SLURM configuration '[bold cyan]{name}[/bold cyan]' saved to {config_path}")

@configure_app.command("pbs")
def configure_pbs(
  name: str = typer.Option(..., "--name", help="A unique name for this configuration."),
  queue: Optional[str] = typer.Option(None, help="PBS queue name."),
  cpus: Optional[int] = typer.Option(None, help="Number of CPUs."),
  mem: Optional[str] = typer.Option(None, help="Memory requirement (e.g., 16gb, 64gb)."),
  walltime: Optional[str] = typer.Option(None, help="Walltime (e.g., 01:00:00)."),
  env: Optional[List[str]] = typer.Option(None, "--env", help="Environment variables to set (e.g., VAR=value)."),
):
  """Creates a PBS/Torque configuration."""
  scheduler = PbsScheduler()
  script_content = scheduler.generate_script(
    name=name, queue=queue, cpus=cpus, mem=mem, walltime=walltime, env=env
  )
  config_path = CONFIG_DIR / f"{name}.sh"
  with open(config_path, "w") as f:
    f.write(script_content)
  console.print(f"✅ PBS configuration '[bold cyan]{name}[/bold cyan]' saved to {config_path}")

@configure_app.command("local")
def configure_local(
  name: str = typer.Option(..., "--name", help="A unique name for this configuration."),
  env: Optional[List[str]] = typer.Option(None, "--env", help="Environment variables to set (e.g., VAR=value)."),
):
  """Creates a configuration for local execution."""
  scheduler = LocalScheduler()
  script_content = scheduler.generate_script(name=name, env=env)
  config_path = CONFIG_DIR / f"{name}.sh"
  with open(config_path, "w") as f:
    f.write(script_content)
  console.print(f"✅ Local configuration '[bold cyan]{name}[/bold cyan]' saved to {config_path}")


@app.command("launch")
def launch(
  config_name: str = typer.Option(..., "--config-name", "-c", help="The name of the configuration to use."),
  comment: str = typer.Option("", "--comment", help="A short comment to identify the experiment."),
  command: str = typer.Argument(..., help="The executable and its parameters, enclosed in quotes."),
):
  """Launches an experiment using a predefined configuration."""
  try:
    launch_experiment(config_name, command, comment)
  except FileNotFoundError as e:
    console.print(f"❌ Error: {e}")
    raise typer.Exit(code=1)
  except Exception as e:
    console.print(f"❌ An unexpected error occurred: {e}")
    raise typer.Exit(code=1)

@app.command("status")
def status():
  """Shows the status of all experiments in an interactive TUI."""
  run_tui()

if __name__ == "__main__":
  app()