# 🛠️ Installation

The recommended way to install `SbatchMan` is with `pipx`.

If you don't have `pipx`, you can install it with:
```bash
python3 -m pip install --user pipx
python3 -m pipx ensurepath
```
You may need to restart your terminal for the changes to take effect.

Once `pipx` is installed, you can install `sbatchman` from PyPI:
```bash
pipx install sbatchman
```

Great! Now you have `SbatchMan` installed. 🎉🎉🎉

Before continuing, **assign a name to your cluster**. This name helps organize configurations and manage jobs. Pick something descriptive, such as `research_cluster`, `test_cluster`, or `gpu_cluster`. Set the cluster name with:

```bash
sbatchman set-cluster-name my_cluster
```