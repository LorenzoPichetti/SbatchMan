# Advanced Job Submission

`SbatchMan` supports launching jobs using a YAML file or through the Python API. This guide will walk you through both methods, allowing you to manage complex job configurations and launch them efficiently.

## Launching Jobs using a YAML File
This section explains how to use `SbatchMan` to launch jobs defined in a YAML file. This is particularly useful for managing complex experiments with multiple configurations and parameters.

### The `launch` Command

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
-   `config_jobs`: A list of variations for this job template. Each variation will generate one or more jobs.

*`command`, `preprocess` and `postprocess` can be also set at top-level.*

> **IMPORTANT NOTE:** In general, higher-level variables and blocks can be overwritten by redeclaring them for more specific scopes.

#### The `config_jobs` Block

Each entry in the `config_jobs` list defines a specific set of runs for a configuration and must contain a `tag`. It can also contain:

-   `tag`: To define the tag to be assigned to generated jobs.
-   `variables`: To define or override variables for this specific variation.
-   `command`: To provide a command that overrides the job template's command.
-   `preprocess`: To provide a command that sets or overrides the preprocess command.
-   `postprocess`: To provide a command that sets or overrides the postprocess command.

#### The `variables` Block

This section defines variables that will be used to generate different job configurations. The final set of jobs is the Cartesian product of all applicable variable values.

Variables can be defined in three ways:

1.  **As a list of values:**
  ```yaml
  variables:
    learning_rate: [0.01, 0.001]
    use_gpu: [True, False]
  ```
2.  **As a path to a file:** SbatchMan will treat each line in the file as a value for the variable.
  ```yaml
  variables:
    dataset_path: datasets.txt
  ```
3.  **As a path to a directory:** SbatchMan will treat each file in the directory as a value for the variable.
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


### Hierarchy and Variable wildcards

- **Dynamic Names**: `conf`, `tag`, `command`, `preprocess`, `postprocess` names/values can use placeholders (e.g., `my_{nGPUs}gpu_config`) to automatically generate a list of distinct values.
- **Overrides**: Each experiment can have its own `command`, `preprocess`, `postprocess`, or `variables` block, which will override settings from higher levels in the hierarchy.

> **IMPORTANT NOTE:** the number of combinations that dynamic names that will be generated, depend on the variables-dependencies that the names/values have. More about this in the following example.


### Example

Here is a complete example of a YAML batch submission file:

```yaml
variables:
  dataset: datasets/   # This is a directory; each file name will be used as a value
  nGPUs: gpus.txt      # This is a file; each line is a value
  trials: [100, 200]   # List of explicit values
  flag: ['--flag1', '--flag2']

# Top-level commands
command: python run.py --input {dataset} --runs {trials} --gpus {nGPUs} {flag}
preprocess: echo "Preparing dataset {dataset}"
postprocess: echo "Cleaning up after {dataset}"

jobs:
  - config: my_{nGPUs}gpu_config # Dynamically generate the configuration name
    # Uses the global command and variables
    config_jobs:
      - tag: flag_{flag} # This will run with ['--flag1', '--flag2']

      - tag: custom_flag # This will run with only ['--flag3']
        variables:
          flag: ['--flag3'] # Overwrite top-level flag variable

  - config: other_config
    # Custom variables for other_config
    variables:
      trials: [300, 400] # Overwrite top-level trials variable

    # Custom command and preprocess for other_config
    command: python custom.py --file {dataset} --runs {trials}
    preprocess: echo "Custom preprocess for config custom_exp_{dataset}"
    # Keep top-level postprocessing
    
    config_jobs:
      - tag: custom_program
        variables:
          dataset: datasets/test/ # Datasets for the custom.py

      - tag: custom_program1
        variables:
          dataset: datasets/test1/
        # Overwrite command only for tag custom_program1
        command: python custom_1.py --file {dataset} --runs {trials}
```

#### What will this example run?

**Configurations: `my_{nGPUs}gpu_config`**

*Variables used:*

- `dataset`: from `datasets/` directory  
    - _Example files:_ `data1.csv`, `data2.csv` → 2 values
- `nGPUs`: from `gpus.txt`  
    - _Example lines:_ `1`, `2` → 2 values
- `trials`: `[100, 200]` → 2 values
- `flag`:  
    - For tag `flag_{flag}`: `['--flag1', '--flag2']` → 2 values  
    - For tag `custom_flag`: `['--flag3']` → 1 value

*Combinations:*

- Tag: `flag_{flag}` → `2 (datasets) × 2 (GPUs) × 2 (trials) × 2 (flags)` = **16 jobs**
- Tag: `custom_flag` → `2 (datasets) × 2 (GPUs) × 2 (trials) × 1 (flag)` = **8 jobs**

✅ Total from this template: **24 jobs**

---

**Configuration: `other_config`**

*Variables and overrides:*

- `trials`: `[300, 400]`
- Custom `command` and `preprocess`
- Custom `datasets` variable:
    - `custom_program`: from `datasets/test/`  
        - _Example files:_ `testA.csv`, `testB.csv` → 2 values
    - `custom_program1`: from `datasets/test1/`  
        - _Example files:_ `sample1.csv`, `sample2.csv`, `sample3.csv` → 3 values

*Combinations:*

- Tag: `custom_program`  → ` 2 (datasets) × 2 (GPUs) × 2 (flags) × 2 (trials)` = **16 jobs**
- Tag: `custom_program1` → `3 (datasets) × 2 (GPUs) × 2 (flags) × 2 (trials)` = **24 jobs**

✅ **Total from this template: 40 jobs**

✅ **Grand Total: 64 jobs**



## Launching Jobs with the Python API

You can launch jobs programmatically using the Python API. This is useful for integrating `SbatchMan` into larger workflows or scripts.

### Launching a Single Job

To launch a single job, use the `api.launch_job` function. You need to provide a configuration name and the command to execute.

```python
import sbatchman as sbm

try:
  # Launch a single job using the 'cpu_small' configuration
  job = sbm.launch_job(
    config_name="cpu_small",
    command="python my_script.py --data /path/to/data",
    tag="single_run_test"      # Optional: to group related jobs
    # Cluster name will be automatically detected from your SbatchMan configuration file
  )
  print(f"Successfully launched job {job.job_id} in {job.exp_dir}")
except Exception as e:
  print(f"An error occurred: {e}")
```

For more details, refer to the [API](../api.md/#sbatchman.launch_job) page.

### Launching Multiple Jobs from a File

The `launch_jobs_from_file` function takes the path to a YAML file and launches all the jobs defined within it.

```python
from pathlib import Path
import sbatchman as sbm

# Path to your batch jobs file
jobs_file = Path("experiments.yaml")

try:
  # Launch the jobs
  launched_jobs = sbm.launch_jobs_from_file(jobs_file)
  print(f"Successfully launched {len(launched_jobs)} jobs.")
  for job in launched_jobs:
    print(f"  - Job ID: {job.job_id}, Config: {job.config_name}, Tag: {job.tag}")
except Exception as e:
    print(f"An error occurred: {e}")
```

For more details, refer to the [API](../api.md/#sbatchman.launch_jobs_from_file) page.