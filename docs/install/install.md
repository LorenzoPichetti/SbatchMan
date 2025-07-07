# 🛠️ Installation

SbatchMan comes with a command-line interface (CLI) and a Python API. 

For your first installation, it is recommended to install and use `SbatchMan` directly on your cluster, as this ensures it runs on the same machine where your jobs will execute. The preferred installation method is using `pipx`.

### Virtual Environment Setup (recommended)

```bash
# If you don't already have one,
# create and use a venv
python3 -m venv .venv
source .venv/bin/activate

pip install pipx
pipx ensurepath
```

### Global Setup

If you don't have `pipx`, you can install it with:

```bash
python3 -m pip install --user pipx
python3 -m pipx ensurepath
```

You may need to restart your terminal for the changes to take effect.

## SbatchMan Installation

Once `pipx` is installed, you can install `sbatchman` from [PyPI](https://pypi.org/project/sbatchman/):
```bash
# Install CLI command
pipx install sbatchman

# Install python package
pip install sbatchman
# this will ensure that you can
# 'import sbatchman' in your python scripts
```

Great, you now have `SbatchMan` installed! You can check out the available commands by running:
```bash
sbatchman --help
```

> **! IMPORTANT !**  
> Before using SbatchMan, make sure to complete the initial setup. Please, read the [setup](setup.md) page.