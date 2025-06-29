<p align="center">
    <img src="https://raw.githubusercontent.com/LorenzoPichetti/SbatchMan/refs/heads/rewrite/docs/sbatchman.png" alt="SbatchMan" width="124">
<p>

# SbatchMan
A utility to create, launch, and monitor code experiments on SLURM, PBS, or local machines.

## 🚀 Installation

The recommended way to install `SbatchMan` is with `pipx`. This will install the package and its dependencies in an isolated environment.

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

Now try running `sbatchman --help`!

For development, clone the repository and install the package in editable mode from the local repository:
```bash
# For developers
pip install -e .
```
This allows you to make changes to the code and have them reflected immediately without needing to reinstall.

## 🛠️ Usage

The tool is organized into three main commands: `configure`, `launch`, and `status`.

### ⚙️ Configure an Experiment
Before launching an experiment, you need to create a configuration for your target environment (SLURM, PBS, or local). The `configure` command helps you create and save these settings.

When you first run this command in a project directory, `SbatchMan` will ask for confirmation to create a `sbatchman` directory. This directory will store all your configurations and experiment results.

Check out the available options for each environment:
```bash
sbatchman configure slurm --help # Show options for SLURM
sbatchman configure pbs --help   # Show options for PBS
sbatchman configure local --help # Show options for local execution
```

The configuration file will be saved in the `sbatchman/configs` directory with the name you provided. You can use this name later to launch your experiment. If you need to change the configuration, you can simply run the `configure` command again with the same name, and it will overwrite the existing configuration.

#### SLURM Example:

```bash
sbatchman configure slurm --name my-gpu-job \
--partition gpu_queue \
--cpus-per-task 4 \
--mem "16G" \
--gpus 1 \
--time "01:00:00" \
--env "CUDA_VISIBLE_DEVICES=0"
```

#### PBS Example:
```bash
sbatchman configure pbs --name my-pbs-job \
--queue standard \
--cpus 4 \
--mem "16gb" \
--walltime "01:00:00"
```

#### Local Example:
```bash
sbatchman configure local --name my-local-job \
--env "MY_VAR=hello"
```

### 쏘 Launch an Experiment

Use the configuration name to launch your code.
```bash
sbatchman launch --config my-gpu-job \
"python my_project/train.py --learning-rate 0.001 --epochs 50"
```

The command to execute must be passed in quotes.

### 📊 Check Experiment Status

To monitor your experiments, launch the interactive interface:
```bash
sbatchman status
```

The TUI provides a real-time view of your jobs, categorized into queued, running, and finished states. You can navigate through the list of jobs and select one to view its live standard output and error logs.

**⌨️ Keybindings:**
- **Up/Down Arrows**: Navigate through jobs and tabs.
- **Enter**: View logs for the selected job.
- **q**: Go back to the previous view or quit the application.

![SbatchMan TUI](https://raw.githubusercontent.com/LorenzoPichetti/SbatchMan/refs/heads/rewrite/docs/tui.png)