# 🚀 Step-by-step Tutorial 

Learn how to use SbatchMan in this **step-by-step Tutorial**!

It covers everything you need to know to get you started with SbatchMan, from setting up your cluster configuration to launching jobs and collecting results.

## 💡 SbatchMan Core Concepts

- **Project**  
  A directory where SbatchMan will store all metadata, configurations, and job records.  
  When you run `sbatchman init`, a `SbatchMan` folder is created in the current working directory. This folder is a SbatchMan "project".

- **Configuration**  
  A named set of cluster/job parameters (like environment variables, partition, walltime, GPUs, etc.), that will be stored in `SbatchMan/configs/`.
    - Each configuration has a *name*.
    - Configurations are reusable for different jobs.
    - Each configuration has a corresponding *template shell script*.
    - All configurations details are stored into a YAML file.

- **Tag**  
  A label you assign to a batch of jobs. Tags help you organize, filter, and track jobs.  
  **Example**: you have two programs `A` and `B`. Both shall run under the same configuration called `ExampleConfig`. Later, you'd like to retrieve results for experiments on programs `A` and `B` separately. You can achieve this by simply assigning two different tags to the jobs you run. 

- **Job**  
  A single execution of a command on a cluster or your local machine, tracked by SbatchMan.  
  Each job is linked to a **configuration** and can have a **tag**.  
  For each job, SbatchMan stores the **status**, **stdout**, **stderr** etc.

---

## 📂 Internal File & Folder Structure

```
# this is your project folder
SbatchMan/
├── archive/                        # Archived jobs
├── configs/                        # All configurations and templates
│   ├── configurations.yaml         # Central registry of all configurations
│   ├── <cluster_name>/
│   │   ├── <configuration_name_1>.sh  # Configuration template script
│   │   └── <configuration_name_2>.sh
│   └── <another_cluster_name>/
│       ├── <configuration_name_1>.sh
│       └── <configuration_name_2>.sh
└── experiments/                    # All job runs and their results
    └── <cluster_name>/
        ├── <configuration_name>/
        │   └── <tag>/
        │       ├── <job_timestamp_1>/
        │       │   ├── metadata.yaml      # Job metadata (config, tags, etc.)
        │       │   ├── run.sh             # The actual script submitted
        │       │   ├── stderr.log         # Error output
        │       │   └── stdout.log         # Standard output
        │       ├── <job_timestamp_2>/
        │       │   └── ...
        │       └── ...
        └── ...
```

- `configs/` Contains all configuration files and template scripts, organized by cluster.
- `experiments/` Stores all job runs, grouped by configuration and tag. Each run has its own timestamped folder with logs and metadata.
- `archive/` Used for archiving completed or old jobs.

This structure makes it easy to manage, reproduce, and analyze your experiments across different clusters and configurations.

## 📚 Initialize SbatchMan

To initialize SbatchMan, run the following command in your project root directory:

```bash
sbatchman init
```

This command will create a `SbatchMan` directory for your project, which will contain all the necessary files and configurations for managing your jobs.

> **IMPORTANT NOTE:** whenever you call `sbatchman` command, SbatchMan will look for a project directory (`SbatchMan`) starting from the current working directory (CWD) and exploring parents directories up to the user home (e.g. in linux `$HOME`).  

## ⚙️ Create a Configuration

First, set up your cluster configuration(s). This is where you define the parameters for your cluster, such as environment variables, partition, time limit, and number of GPUs.  
For example, to create a configuration for a cluster named `my_gpu_cluster`:

```bash
sbatchman configure slurm \
  --name simple_gpu_config \
  --partition gpu \
  --time 02:00:00 \
  --gpus 1 \
  --module GCC/13.3.0 \
  --module CUDA/12.5.0 \
  --cluster-name my_gpu_slurm_cluster
```

For a PBS cluster, you can use a similar command:

```bash
sbatchman configure pbs \
  --name simple_gpu_config \
  --queue gpu \
  --walltime 02:00:00 \
  --gpus 1 \
  --cluster-name my_gpu_pbs_cluster
```

For local development:

```bash
sbatchman configure local \
  --name simple_local_config \
  --env VAR1=value1 --env VAR2=value2 \
  --cluster-name my_local_machine
```

You can check out all the available options for configuring your cluster by running:

```bash
sbatchman configure --help
sbatchman configure slurm --help
sbatchman configure pbs --help
```

If you need to change the configuration later, run the `configure` command again with the same `--name` option and the `--overwrite` flag. SbatchMan will replace the existing configuration with the new one.

> **IMPORTANT NOTE:** if you do not specify `--cluster-name`, SbatchMan will use the name you set via the `sbatchman set-cluster-name` (see the [Setup Page](../install/setup.md) for more details).

## 🚀 Launch Your Code

Suppose you have a script named `train.py` in your project directory. To submit this script as a job, use the `launch` command. For example: 

```bash
sbatchman launch \
  --config simple_gpu_config \
  --tag mnist_training \
  python train.py --epochs 10 --batch-size 32
```

> **NOTE:** depending on the cluster where you run this command, the configuration will change accordingly.

The `--tag` option lets you organize your jobs by assigning a label to them. Tags are useful for tracking different experiments or runs of the same job, allowing you to easily filter and manage your jobs later on. A common use case for tags is to differentiate between different configurations of the same experiment, such as changing parameters or datasets.

For example, if you change the training dataset or number of epochs, you can relaunch the job with a new tag:

```bash
sbatchman launch \
  --config simple_gpu_config \
  --tag mnist_training_20_epochs \
  python train.py --epochs 20 --batch-size 32
```

## 🖥️ Monitor Your Jobs

You can check the status of your jobs with:

```bash
sbatchman status
```

This command will show you all the submitted jobs and their detail through an interactive Terminal UI (TUI).

> To select and copy text hold the SHIFT or OPTION key.

## 🏆 Collect Results

Once the jobs are completed, you will find all their data into the project sub-directory `SbatchMan/experiments`.  
Of course, you won't need to parse them manually. To parse the results, you can use the Python library offered by SbatchMan.  
For example, you can read the logs and extract metrics like accuracy or loss:

```python
from sbatchman import Job, jobs_list
job = jobs_list(
  cluster_name="my_gpu_cluster",
  config_name="simple_gpu_config",
  tag="mnist_training_20_epochs"
)[0]
executable, args_dict = job.parse_command_args()
print(executable)
print(args_dict)
print(job.get_stdout())
```

Here you can see the power of tags: you can easily filter jobs by their tags, making it simple to find the results of specific experiments.

### Advanced querying
The `jobs_list` function returns a list of `Job` objects, which you can further filter or sort. For example, you can get all jobs with a specific tag:

```python
jobs = jobs_list() # This will list all jobs across all clusters
jobs_with_tag = [job for job in jobs if job.tag == "mnist_training_20_epochs"]
for job in jobs_with_tag:
  print(f"Job ID: {job.id}, Status: {job.status}, Log: {job.get_log()}")
```

For more details, refer to the [API](../api.md/#sbatchman.Job) page.

## 📦 Archiving Jobs
To archive jobs, you can use the `archive` command. This is useful for keeping your job history organized and manageable. For example, to archive all jobs with the tag `mnist_training`:
```bash
sbatchman archive \
  --archive-name mnist_training_archive_1 \
  --tag mnist_training \
  --cluster-name my_gpu_cluster \
  --config-name simple_gpu_config
```

Archived jobs will be moved to the `SbatchMan/archive` directory, and will not appear in the job list, unless you specify the `archived` option in the `jobs_list` function.

You can check out all the available options by running:

```bash
sbatchman archive --help
```

## 🎉 Conclusion
This is a basic example of how to use SbatchMan to manage your experiments on multiple remote clusters. You can extend this by adding more configurations, automating job submissions, or using the Python API to integrate SbatchMan into your existing workflows.