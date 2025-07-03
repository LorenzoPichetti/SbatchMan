# Advanced Job Submission

`SbatchMan` supports launching jobs using a YAML file or through the Python API. This guide will walk you through both methods, allowing you to manage complex job configurations and launch them efficiently.

## Launching Jobs using a YAML File
This section explains how to use `SbatchMan` to launch jobs defined in a YAML file. This is particularly useful for managing complex experiments with multiple configurations and parameters.

### The Launch Command

To launch a batch of jobs, use the `launch` command with the `--file` option:

```bash
sbatchman launch --file experiments.yaml
```

### Batch File Structure

The batch submission file is a YAML file that defines global variables and a list of job templates. `SbatchMan` will generate a job for each unique combination of parameters.

#### Top-Level Keys

The file has two main top-level keys:

-   `variables`: Defines global variables applicable to all jobs.
-   `jobs`: A list of job templates.

#### The `jobs` Block

This is a list where each item defines a job template. Each template can have the following keys:

-   `config`: The name of the configuration to use. This can be dynamic, using variables (e.g., `gpu_partition_{gpu_number}`).
-   `command`: A command template for the job.
-   `preprocess`: An optional command to run before the main `command`.
-   `postprocess`: An optional command to run after the main `command`.
-   `variables`: A dictionary of variables that apply only to this job template.
-   `matrix`: A list of variations for this job template. Each variation will generate one or more jobs.

#### The `matrix` Block

Each entry in the `matrix` list defines a specific set of runs for a job template and must contain a `tag`. It can also contain:

-   `variables`: To define or override variables for this specific variation.
-   `command`: To provide a command that overrides the job template's command.
-   `preprocess`: To override the job template's preprocess command.
-   `postprocess`: To override the job template's postprocess command.

#### The `variables` Block

This section defines variables that will be used to generate different job configurations. The final set of jobs is the Cartesian product of all applicable variable values.

Variables can be defined in three ways:

1.  **As a list of values:**
  ```yaml
  variables:
    learning_rate: [0.01, 0.001]
    use_gpu: [True, False]
  ```
2.  **As a path to a file:** `SbatchMan` will treat each line in the file as a value for the variable.
  ```yaml
  variables:
    dataset_path: datasets.txt
  ```
3.  **As a path to a directory:** `SbatchMan` will treat each file in the directory as a value for the variable.
  ```yaml
  variables:
    dataset_path: datasets/
  ```

#### Example

Here is a complete example of a batch submission file:

#### Example

Here is a complete example of a batch submission file:

```yaml
# Global variables applicable to all jobs
variables:
  n: 1000
  m: 10000
  scale: ['small', 'large']

# A list of job definitions
jobs:
  - config: "bfs_{scale}_cpu"
    command: "python script.py --file {dataset} --n {n} --m {m}"
    preprocess: "echo 'Starting BFS job for {scale} scale graphs'"
    # A matrix defines the different jobs for this configuration
    matrix:
      - tag: "shortDiam"
        variables:
          dataset: "datasets/short_diam/{scale}.graph"

      - tag: "largeDiam"
        variables:
          dataset: "datasets/large_diam/{scale}.graph"
        postprocess: "echo 'Finished large diameter test.'"

      - tag: "atomics_test"
        # This command overrides the one defined in the parent job
        command: "python script.py --file datasets/short_diam/{scale}.graph --atomic"
```

## Launching Jobs with the Python API

You can launch jobs programmatically using the Python API. This is useful for integrating `SbatchMan` into larger workflows or scripts.

### Launching a Single Job

To launch a single job, use the `api.launch_job` function. You need to provide a configuration name and the command to execute.

```python
from sbatchman import api

try:
  # Launch a single job using the 'cpu_small' configuration
  job = api.launch_job(
    config_name="cpu_small",
    command="python my_script.py --data /path/to/data",
    cluster_name="baldo", # Optional: specify if config name is not unique
    tag="single_run_test" # Optional: to group related jobs
  )
  print(f"Successfully launched job {job.job_id} in {job.exp_dir}")

except Exception as e:
  print(f"An error occurred: {e}")
```

### Launching Multiple Jobs from a File

The `launch_jobs_from_file` function takes the path to a YAML file and launches all the jobs defined within it.

```python
from pathlib import Path
from sbatchman import api

# Path to your batch jobs file
jobs_file = Path("experiments.yaml")

try:
  # Launch the jobs
  launched_jobs = api.launch_jobs_from_file(jobs_file)
  print(f"Successfully launched {len(launched_jobs)} jobs.")
  for job in launched_jobs:
    print(f"  - Job ID: {job.job_id}, Config: {job.config_name}, Tag: {job.tag}")
except Exception as e:
    print(f"An error occurred: {e}")
```