import subprocess
from typing import Tuple


def run_command(command: str) -> Tuple[int, str, str]:
  """
  Executes a shell command and returns its exit code, stdout, and stderr.
  """
  process = subprocess.run(
    command,
    shell=True,
    capture_output=True,
    text=True
  )
  return process.returncode, process.stdout.strip(), process.stderr.strip()