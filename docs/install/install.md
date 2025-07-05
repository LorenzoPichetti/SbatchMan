# 🛠️ Installation

SbatchMan comes with a command-line interface (CLI) and a Python API. 

For your first installation, it is recommended to install and use `SbatchMan` directly on your cluster, as this ensures it runs on the same machine where your jobs will execute. The preferred installation method is using `pipx`.

If you don't have `pipx`, you can install it with:
```bash
python3 -m pip install --user pipx
python3 -m pipx ensurepath
```
You may need to restart your terminal for the changes to take effect.

Once `pipx` is installed, you can install `sbatchman` from [PyPI](https://pypi.org/project/sbatchman/):
```bash
pipx install sbatchman
```

Great, you now have `SbatchMan` installed! You can check out the available commands by running:
```bash
sbatchman --help
```

> **! IMPORTANT !**  
> Before using SbatchMan, make sure to complete the initial setup. Please, read the [setup](setup.md) page.