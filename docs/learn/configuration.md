# Advanced configuration

This guide covers the different ways to create and manage configurations in `SbatchMan`. You can either use a YAML file for batch configurations or programmatically create configurations using the Python API.

## Configuring with a YAML File

For managing multiple configurations across different clusters, a YAML file is the most convenient method. This is especially useful for setting up complex projects or sharing configurations with your team.

### The `configure` Command

To add multiple configurations from a file, you can use the `configure` command with the `--file` option, pointing to your YAML configuration file.

```bash
sbatchman configure --file my_configs.yaml
```

This command will parse the file and create or replace the specified configurations.

### YAML File Structure

The configuration file is organized by cluster. Each top-level key represents a `cluster_name` (excluding `variables`).

#### Variables and Wildcards

You can declare a `variables` block at the top level, similar to the jobs YAML file.  
Variables can be:

- A list of values
- A path to a file (each line is a value)
- A path to a directory (each file is a value)

You can use `{var}` wildcards in configuration names and parameter values. All combinations (cartesian product) of variable values will be generated.

> **IMPORTANT NOTE:** relative paths will be relative to the directory where the `sbatchman configure` command is run. 

#### YAML Syntax

Each cluster block must contain a `scheduler` and a `configs` section. You can also specify a `default_conf` section to set default parameters for all configurations within that cluster.

Here is the general structure:

```yaml
variables:
  <var1>: [value1, value2]
  <var2>: path/to/file_or_dir

<cluster_name_1>:
  scheduler: <slurm|pbs|local>

  default_conf:
    <param_1>: <value_1>
    <param_2>: <value_2>

  configs:
    - name: <config_name_1>
      <param_3>: <value_3>
    - name: <config_name_2>
      <param_1>: <overridden_value_1>
      <param_4>: <value_4>


<cluster_name_2>:
  ...
```

-   **`variables`**: (Optional) Top-level block for variable expansion.
-   **`<cluster_name>`**: The name of the cluster (e.g., `my_gpu_cluster`).
-   **`scheduler`**: The scheduler used by the cluster (`slurm`, `pbs`, or `local`).
-   **`default_conf` (Optional)**: A dictionary of default parameters that apply to all configurations under this cluster.
-   **`configs`**: A list of configuration templates. Wildcards in `name` or parameter values will be expanded using the variables.

#### Example

Here is an example `my_configs.yaml` file defining configurations for two different clusters, `clusterA` (using SLURM) and `clusterB` (using PBS), with variables and wildcards:

```yaml
variables:
  partition: [cpu, gpu]
  ncpus: [4, 8]
  dataset: datasets/   # directory, each file is a value
  mem: ["8gb", "16gb"]

clusterA:
  scheduler: slurm
  default_conf:
    account: "example_default_account"
    modules:
      - "gcc/10.2.0"
      - "python/3.9.6"
  configs:
    - name: job_{partition}_{ncpus}
      partition: "{partition}"
      cpus_per_task: "{ncpus}"
      mem: "16G"
      time: "01-00:00:00"
      env:
        - "DATASET={dataset}"
        - "OMP_NUM_THREADS={ncpus}"

clusterB:
  scheduler: pbs
  configs:
    - name: "mem_job_{mem}"
      cpus: 2
      mem: "{mem}"
      walltime: "01:00:00"
```

For `clutserA`, this will generate a configuration for every combination of `partition`, `ncpus`, and each file in `datasets/`.  
For `clusterB`, this will generate a configuration for every value of `mem`.

### Available Configuration Options

#### Common

* `env`
* `modules`

#### SLURM

* `partition`
* `nodes`
* `ntasks`
* `cpus_per_task`
* `mem`
* `account`
* `time`
* `gpus`
* `constraint`
* `nodelist`
* `exclude`
* `qos`
* `reservation`
* `exclusive`

#### PBS

* `queue`
* `cpus`
* `mem`
* `walltime`

## Configuring with the Python API

`SbatchMan` also provides a Python API for creating configurations programmatically. This is useful for dynamic configuration generation within scripts or other tools.

### SLURM Configuration

To create a configuration for a SLURM cluster, use the `create_slurm_config` function.

```python
import sbatchman as sbm

# Create a basic SLURM config
sbm.create_slurm_config(
  name="my_slurm_conf",
  cluster_name="my_slurm_cluster",
  partition="gpu_queue",
  cpus_per_task=4,
  mem="16G",
  gpus=1,
  time="01-00:00:00",
  modules=["gcc/10.2.0", "cuda/11.4"],
  overwrite=True
)
```

For more details, refer to the [API](../api.md/#sbatchman.create_slurm_config) page.

### PBS Configuration

To create a configuration for a PBS cluster, use the `create_pbs_config` function.

```python
import sbatchman as sbm

# Create a basic PBS config
sbm.create_pbs_config(
  name="my_pbs_conf",
  cluster_name="my_pbs_cluster",
  queue="default_queue",
  cpus=2,
  mem="8gb",
  walltime="12:00:00",
  overwrite=True
)
```

For more details, refer to the [API](../api.md/#sbatchman.create_pbs_config) page.

### Local Configuration

For running jobs on your local machine, you can create a `local` configuration.

```python
import sbatchman as sbm

# Create a local config
sbm.create_local_config(
  name="my_local_conf",
  cluster_name="my-laptop",
  env=["MY_VAR=my_value"],
  overwrite=True
)
```

For more details, refer to the [API](../api.md/#sbatchman.create_local_config) page.