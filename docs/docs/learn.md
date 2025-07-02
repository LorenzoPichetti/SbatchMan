# Learn

Learn how to use SbatchMan in this step-by-step Tutorial.

It covers everything you need to know from the simplest experiment to the most complex use cases, including advanced features like using the Python library, managing job archives, and more.

You could consider this a book, a course, the official and recommended way to learn SbatchMan. 😎

# Installation
The recommended way to install `SbatchMan` is with `pipx`. For your first installation, we recommend working directly on the cluster where you will run your experiments. Later, we will see how to use `SbatchMan` also on your local machine to prepare experiments for the cluster.

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

Great! Now you have `SbatchMan` installed.

Before moving on, set the name that sbatchman will use to identify your cluster. This is important for managing configurations and experiments.

```bash
sbatchman set-cluster-name my_cluster
```

# Example Use Case: Training a Machine Learning Model

Let's walk through a simple use case: training a machine learning model on a remote cluster using SbatchMan.

## 1. Create a Configuration

First, set up your cluster configuration. For example, to create a configuration for a cluster named `my_cluster`:

```bash
sbatchman configure slurm --name my_config --partition gpu --time 02:00:00 --gpus 1 --cluster-name my_cluster
```

## 2. Prepare Your Experiment

Suppose you have a script called `train.py` in your project directory. Create an experiment configuration:

```bash
sbatchman experiment create --name mnist_training --script train.py --config my_config
```

## 3. Launch the Job

Submit your experiment to the cluster:

```bash
sbatchman run --experiment mnist_training
```

You can check the status of your jobs with:

```bash
sbatchman status
```

## 4. Collect Results

Once the job is finished, retrieve the results and logs:

```bash
sbatchman collect --experiment mnist_training
```

The outputs will be stored in the SbatchMan folder under your project directory.

This workflow covers the essential SbatchMan commands: configuring the cluster, creating an experiment, launching jobs, and collecting results.

# First steps
To get started, initialize SbatchMan in your project directory. This command sets up a `SbatchMan` folder, which will store all configuration files and job archives for your experiments.

```bash
sbatchman init
```
The next step is specifying the configuration for your cluster. This is done using the `configure` command, which allows you to set parameters like partition, time limit, and number of GPUs.

Example for a SLURM cluster:
```bash
sbatchman configure slurm --name my_config --partition my_partition --time 01:00:00 --gpus 1 --cluster-name my_cluster
```
Example for a PBS cluster:
```bash
sbatchman configure pbs --name my_config --queue my_queue --walltime 01:00:00 --gpus 1 --cluster-name my_cluster
```

The `--name` option is used to identify the configuration, while `--cluster-name` specifies the remote cluster where the jobs will run. 