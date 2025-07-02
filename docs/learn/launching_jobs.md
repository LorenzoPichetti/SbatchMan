# Advanced Job Submission

`SbatchMan` simplifies running large-scale experiments by allowing you to launch multiple jobs from a single YAML file. This approach is ideal for parameter sweeps and testing combinations of different parameters.

## The Launch Command

To launch a batch of jobs, use the `launch` command with the `--file` option:

```bash
sbatchman launch --file experiments.yaml
```

You can combine this with other `launch` options to filter which jobs from the file are submitted. For example, to only run jobs with a specific tag:

```bash
sbatchman launch -f experiments.yaml --include_tag shortDiam
```

## Batch File Structure

The batch submission file is a YAML file that defines variables, commands, and a nested structure of experiments. `SbatchMan` will generate a job for each unique combination of parameters defined at every level of the hierarchy.

### Top-Level Keys

The file has three main top-level keys:

-   `variables`: Defines global variables for all experiments.
-   `command`: A default command template for all jobs.
-   `experiments`: The main block defining the jobs to be run in a hierarchical structure.

### The `variables` Block

This section defines variables that will be used to generate different job configurations. The final set of jobs is the Cartesian product of all variable values.

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
    If `datasets.txt` contains:
    ```
    datasets/data1.csv
    datasets/data2.csv
    ```
    Then `dataset_path` will have two possible values: `datasets/data1.csv` and `datasets/data2.csv`.
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

### The `command` Block

This is a global command template. Placeholders in the format `{variable_name}` will be replaced by the values of the defined variables.

```yaml
command: python train.py --lr {learning_rate} --data {dataset_path}
```

### The `experiments` Block

This is the core section where you define your jobs. It's a dictionary where each key is an experiment name. The key feature is that each experiment can contain another `experiments` block, allowing you to create a nested hierarchy.

-   **Dynamic Names**: Experiment names can use placeholders (e.g., `exp_{dataset_name}`) to create distinct groups of jobs.
-   **Overrides**: Each experiment can have its own `command` or `variables` block, which will override settings from higher levels in the hierarchy.
-   **Job Identification**: The top-level experiment name corresponds to the job's `config`, while the name of the innermost experiment that defines the job becomes its `tag`.

### Hierarchy and Overrides

`SbatchMan` uses a clear hierarchy for commands and variables:

**Innermost Experiment > ... > Outermost Experiment > Global**

-   A `command` defined at a lower level (e.g., a nested experiment) completely replaces any `command` defined at a higher level.
-   `variables` defined at a lower level are merged with and override variables of the same name from higher levels.

### Example

Here is a complete example of a batch submission file using the nested structure:

```yaml
# filepath: experiments.yaml
variables:
  n: 1000
  m: 10000
  scale: ['small', 'large']

command: python script.py --file {dataset} --n {n} --m {m}

experiments:
  bfs_{scale}_cpu:
    # This experiment name is dynamic, based on the 'scale' variable.
    # It will generate two top-level experiments: 'bfs_small_cpu' and 'bfs_large_cpu'.
    experiments:
      shortDiam:
        variables:
          dataset: datasets/short_diam/{scale}.graph
      largeDiam:
        variables:
          dataset: datasets/large_diam/{scale}.graph
      atomics_test:
        # This command completely overrides the one from the parent and global scope.
        command: python script.py --file {dataset} --atomic
        variables:
          dataset: datasets/short_diam/{scale}.graph
```

When you run `sbatchman launch -f experiments.yaml`, `SbatchMan` will generate the following jobs:

-   **Config `bfs_small_cpu`**:
    -   **Tag `shortDiam`**: `python script.py --file datasets/short_diam/small.graph --n 1000 --m 10000`
    -   **Tag `largeDiam`**: `python script.py --file datasets/large_diam/small.graph --n 1000 --m 10000`
    -   **Tag `atomics_test`**: `python script.py --file datasets/short_diam/small.graph --atomic`
-   **Config `bfs_large_cpu`**:
    -   **Tag `shortDiam`**: `python script.py --file datasets/short_diam/large.graph --n 1000 --m 10000`
    -   **Tag `largeDiam`**: `python script.py --file datasets/large_diam/large.graph --n 1000 --m 10000`
    -   **Tag `atomics_test`**: `python script.py --file datasets/short_diam/large.graph --atomic`