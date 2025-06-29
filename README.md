<p align="center">
    <img src="https://raw.githubusercontent.com/LorenzoPichetti/SbatchMan/refs/heads/rewrite/docs/sbatchman.png" alt="SbatchMan" width="124">
<p>

# SbatchMan

A utility to create and launch code experiments on SLURM, PBS, or local machines.

## Installation

You can install `SbatchMan` using pip:

```bash
pip install .
```

Or for development:
```bash
pip install -e .
```

## Usage

The tool is organized into three main commands: configure, launch, and status.

### Configure an Experiment
First, create a configuration for a specific environment (SLURM, PBS, or local). Configurations are stored in `sbatchman/configs/`.

#### SLURM Example:

```
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

### Launch an Experiment

Use the configuration name to launch your code.
```bash
sbatchman launch --config-name my-gpu-job \
"python my_project/train.py --learning-rate 0.001 --epochs 50"
```

The command to execute must be passed as a single string.

### Check Experiment Status

Launch the interactive TUI to monitor your jobs.
```bash
sbatchman status
```
This TUI shows queued, running, and finished jobs. You can select a job to view its live stdout and stderr logs.

Keybindings in TUI:
- Up/Down/k/j: Navigate jobs
- Enter: View logs for the selected job
- b: Go back from log view to job list
- q: Quit the application