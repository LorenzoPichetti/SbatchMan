import os
from pathlib import Path

# The root directory for all exp-kit data
SBATCHMAN_HOME = Path(os.environ.get("EXP_KIT_HOME", "sbatchman"))
CONFIG_DIR = SBATCHMAN_HOME / ".configs"
EXPERIMENTS_DIR = SBATCHMAN_HOME / "experiments"

# Ensure base directories exist
CONFIG_DIR.mkdir(parents=True, exist_ok=True)
EXPERIMENTS_DIR.mkdir(parents=True, exist_ok=True)