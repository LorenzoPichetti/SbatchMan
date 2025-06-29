from pathlib import Path
import sys
from typing import Optional
from rich.console import Console
from rich.prompt import Confirm

# The name of the root directory to search for.
PROJECT_ROOT_DIR_NAME = "SbatchMan"

_cached_sbatchman_home: Optional[Path] = None

def find_sbatchman_home() -> Path:
  """
  Searches for the project root directory (SbatchMan) upwards from the CWD.
  If not found, defaults to the user's home directory.
  """
  global _cached_sbatchman_home
  if _cached_sbatchman_home is not None:
      return _cached_sbatchman_home

  current_dir = Path.cwd()
  home_dir = Path.home()

  # Search upwards from CWD to home directory
  while current_dir != home_dir and current_dir.parent != current_dir:
    project_dir = current_dir / PROJECT_ROOT_DIR_NAME
    if project_dir.is_dir():
      _cached_sbatchman_home = project_dir
      return project_dir
    current_dir = current_dir.parent

  # Check home directory as the last stop
  home_project_dir = home_dir / PROJECT_ROOT_DIR_NAME
  if home_project_dir.is_dir():
    _cached_sbatchman_home = home_project_dir
    return home_project_dir
  
  # If not found anywhere, ask the user to initialize it here.
  console = Console()
  cwd = Path.cwd()
  if Confirm.ask(
      f"[bold yellow]SbatchMan root not found.[/bold yellow] Would you like to initialize it in the current directory ({cwd})?",
      default=False
  ):
      project_dir = cwd / PROJECT_ROOT_DIR_NAME
      project_dir.mkdir(exist_ok=True)
      _cached_sbatchman_home = project_dir
      console.print(f"✅ Initialized SbatchMan root at [cyan]{project_dir}[/cyan]")
      return project_dir
  else:
      console.print("[red]SbatchMan root not found. Aborting.[/red]")
      sys.exit(1)


def get_config_dir() -> Path:
  """Returns the path to the configuration directory."""
  path = find_sbatchman_home() / "configs"
  path.mkdir(parents=True, exist_ok=True)
  return path

def get_experiments_dir() -> Path:
  """Returns the path to the experiments directory."""
  path = find_sbatchman_home() / "experiments"
  path.mkdir(parents=True, exist_ok=True)
  return path