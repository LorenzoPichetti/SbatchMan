# 🚀 Step-by-step Tutorial 

Learn how to use SbatchMan in this **step-by-step Tutorial**!

It covers everything you need to know to get you started with SbatchMan, from setting up your cluster configuration to launching jobs and collecting results.

## 📚 Initialize SbatchMan
To initialize SbatchMan, run the following command in the project root directory:

```bash
sbatchman init
```

This command will create a `SbatchMan` directory in your project, which will contain all the necessary files and configurations for managing your jobs.

## ⚙️ Create a Configuration

First, set up your cluster configuration. This is where you define the parameters for your cluster, such as the partition, time limit, and number of GPUs. For example, to create a configuration for a cluster named `my_gpu_cluster`:

```bash
sbatchman configure slurm \
  --name simple_gpu_config \
  --partition gpu \
  --time 02:00:00 \
  --gpus 1 \
  --cluster-name my_gpu_cluster
```
For a PBS cluster, you can use a similar command:

```bash
sbatchman configure pbs \
  --name simple_gpu_config \
  --queue gpu \
  --walltime 02:00:00 \
  --gpus 1 \
  --cluster-name my_gpu_cluster
```

You can check out all the available options for configuring your cluster by running:

```bash
sbatchman configure --help
sbatchman configure slurm --help
sbatchman configure pbs --help
```

If you need to change the configuration later, run the `configure` command again with the same `--name` option. SbatchMan will replace the existing configuration with the new one.

## 🚀 Launch Your Code

Suppose you have a script named `train.py` in your project directory. To submit this script as a job, use the `launch` command. For example: 

```bash
sbatchman launch \
  --config simple_gpu_config \
  --tag mnist_training \
  --command "python train.py --epochs 10 --batch-size 32"
```

The `--tag` option lets you organize your jobs by assigning a label to them. Tags are useful for tracking different experiments or runs of the same job, allowing you to easily filter and manage your jobs later on. A common use case for tags is to differentiate between different configurations of the same experiment, such as changing parameters or datasets.

For example, if you change the training dataset or number of epochs, you can relaunch the job with a new tag:

```bash
sbatchman launch \
  --config simple_gpu_config \
  --tag <span style="color:red">mnist_training_20_epochs</span> \
  --command "python train.py <span style="color:red">--epochs 20</span> --batch-size 32"
```

## 🖥️ Monitor Your Jobs

You can check the status of your jobs with:

```bash
sbatchman status
```

This command will show you all the jobs submitted to the cluster, with a live view of their log files.

## 🏆 Collect Results

Once the job is finished, you will find the logs in the `results` directory. To parse the results, you can use the Python library offered by SbatchMan. For example, you can read the logs and extract metrics like accuracy or loss:

```python
from sbatchman import Job, list_jobs
job = list_jobs(
  cluster_name="my_gpu_cluster",
  config_name="simple_gpu_config",
  tag="mnist_training_20_epochs"
)[0]
print(job.get_log())
```

Here you can see the power of tags: you can easily filter jobs by their tags, making it simple to find the results of specific experiments.

### Advanced querying
The `list_jobs` function returns a list of `Job` objects, which you can further filter or sort. For example, you can get all jobs with a specific tag:

```python
jobs = list_jobs() # This will list all jobs across all clusters
jobs_with_tag = [job for job in jobs if job.tag == "mnist_training_20_epochs"]
for job in jobs_with_tag:
  print(f"Job ID: {job.id}, Status: {job.status}, Log: {job.get_log()}")
```

## 📦 Archiving Jobs
To archive jobs, you can use the `archive` command. This is useful for keeping your job history organized and manageable. For example, to archive all jobs with the tag `mnist_training`:
```bash
sbatchman archive \
  --archive-name mnist_training_archive_1 \
  --tag mnist_training \
  --cluster-name my_gpu_cluster \
  --config-name simple_gpu_config
```

Archived jobs will be moved to a specified directory, and will not appear in the job list, unless you specify the `archived` option in the `list_jobs` function.

## 🎉 Conclusion
This is a basic example of how to use SbatchMan to manage your experiments on a remote cluster. You can extend this by adding more configurations, automating job submissions, or using the Python API to integrate SbatchMan into your existing workflows.