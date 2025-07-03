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
    If `datasets/` contains:
    ```
    data1.csv
    data2.csv
    ```
    Then `dataset_path` will have two possible values: `datasets/data1.csv` and `datasets/data2.csv`.

### The `command`, `preprocess`, and `postprocess` Blocks

You can specify commands to run before and after your main job using the `preprocess` and `postprocess` keys. These can be set globally, per experiment, or per tag, and support variable substitution just like `command`.

- **`command`**: The main command to run for the job.
- **`preprocess`**: (Optional) Command to run before the main job.
- **`postprocess`**: (Optional) Command to run after the main job.

Example:

```yaml
command: python train.py --lr {learning_rate} --data {dataset_path}
preprocess: echo "Starting job with dataset {dataset_path}"
postprocess: echo "Finished job with dataset {dataset_path}"
```

You can override `preprocess` and `postprocess` at any level in the hierarchy, just like `command`.

### The `experiments` Block

This is the core section where you define your jobs. It's a dictionary where each key is an experiment name. The key feature is that each experiment can contain another `experiments` block, allowing you to create a nested hierarchy.

-   **Dynamic Names**: Experiment names can use placeholders (e.g., `exp_{dataset_name}`) to create distinct groups of jobs.
-   **Overrides**: Each experiment can have its own `command`, `preprocess`, `postprocess`, or `variables` block, which will override settings from higher levels in the hierarchy.
-   **Job Identification**: The top-level experiment name corresponds to the job's `config`, while the name of the innermost experiment that defines the job becomes its `tag`.

### Hierarchy and Overrides

`SbatchMan` uses a clear hierarchy for commands and variables:

**Innermost Experiment > ... > Outermost Experiment > Global**

-   A `command` defined at a lower level (e.g., a nested experiment) completely replaces any `command` defined at a higher level.
-   `variables` defined at a lower level are merged with and override variables of the same name from higher levels.

### Example

Here is a complete example of a batch submission file using the nested structure:

```yaml
variables:
  dataset: datasets/   # This is a directory; each file name will be used as a value
  n: [100, 200]
  m: [10, 20]
  mode: modes.txt      # This is a file; each line is a value

command: python run.py --input {dataset} --n {n} --m {m} --mode {mode}
preprocess: echo "Preparing dataset {dataset}"
postprocess: echo "Cleaning up after {dataset}"

jobs:
  - config: exp_{n}_{m}
    # Uses the global command and variables
    config_jobs:
      - tag: fast
        variables:
          mode: [fast]
      - tag: slow
        variables:
          mode: [slow]
  - config: custom_exp_{dataset}
    # Custom command and preprocess for config
    command: python custom.py --file {dataset} --n {n}
    preprocess: echo "Custom preprocess for config custom_exp_{dataset}"
    # Custom variables for config
    variables:
      n: [300, 400]
    config_jobs:
      - tag: test
        command: python custom.py --file {dataset} --n {n} --test
        variables:
          dataset: datasets/test/
```

#### What does this example do?

This YAML batch file demonstrates how to use SbatchMan to launch a large set of jobs with different parameters, including pre- and post-processing commands.

- **Variables**:  
  - `dataset`: All files in the `datasets/` directory will be used as values.
  - `n`: Takes values 100 and 200.
  - `m`: Takes values 10 and 20.
  - `mode`: All lines in `modes.txt` will be used as values.

- **Global commands**:  
  - `command`: The main command template for all jobs.
  - `preprocess`: Runs before each job, e.g., prints which dataset is being prepared.
  - `postprocess`: Runs after each job, e.g., prints cleanup info.

- **Experiments**:
  - `exp_{n}_{m}`: For each combination of `n` and `m`, creates an experiment.  
    - Has two tags:  
      - `fast`: Sets `mode` to `fast`.
      - `slow`: Sets `mode` to `slow`.
  - `custom_exp_{dataset}`: For each dataset, creates a custom experiment with its own command and a sweep over `n` (300 and 400).  
    - Has a `test` tag that overrides the command and uses datasets from `datasets/test/`.

#### What jobs will be run?

- For each combination of:
  - `n` in `[100, 200]`
  - `m` in `[10, 20]`
  - `dataset` in all files in `datasets/`
  - `mode` in `[fast]` (for the `fast` tag) or `[slow]` (for the `slow` tag)

  SbatchMan will generate jobs with:
  - Preprocess: `echo "Preparing dataset {dataset}"`
  - Command: `python run.py --input {dataset} --n {n} --m {m} --mode {mode}`
  - Postprocess: `echo "Cleaning up after {dataset}"`

- For each `custom_exp_{dataset}`:
  - For each `dataset` in `datasets/` and each `n` in `[300, 400]`, a job is created with:
    - Command: `python custom.py --file {dataset} --n {n}`
    - Preprocess and postprocess as above (unless overridden).
  - For the `test` tag, for each `dataset` in `datasets/test/`:
    - Command: `python custom.py --file {dataset} --n {n} --test`
    - Preprocess and postprocess as above (unless overridden).

**In summary:**  
This file will launch jobs for every combination of the specified variables, with optional pre- and post-processing commands run before and after each job. You can override these commands at any level (global, experiment, or tag) to customize setup and cleanup for each job.

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