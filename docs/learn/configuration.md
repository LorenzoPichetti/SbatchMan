# Advanced Configuration

While `SbatchMan` allows you to create configurations one by one from the command line, you can also define multiple configurations for various clusters at once using a single YAML file. This is especially useful for setting up complex projects or sharing configurations with your team.

## The Configuration File

To add multiple configurations, you can use the `configure` command with the `--file` option, pointing to your YAML configuration file.

```bash
sbatchman configure --file my_configs.yaml
```

This command will parse the file and create or replace the specified configurations.

## YAML File Structure

The configuration file is organized by cluster. Each top-level key represents a `cluster_name`.

### Cluster Block

Each cluster block must contain a `scheduler` and a `configs` section. You can also specify a `default_conf` section to set default parameters for all configurations within that cluster.

Here is the general structure:

```yaml
<cluster_name_1>:
  scheduler: <slurm|pbs|local>
  default_conf:
    <param_1>: <value_1>
    <param_2>: <value_2>
  configs:
    <config_name_1>:
      <param_3>: <value_3>
    <config_name_2>:
      <param_1>: <overridden_value_1>
      <param_4>: <value_4>

<cluster_name_2>:
  scheduler: <slurm|pbs|local>
  configs:
    <config_name_3>:
      ...
```

-   **`<cluster_name>`**: The name of the cluster (e.g., `my_gpu_cluster`).
-   **`scheduler`**: The scheduler used by the cluster (`slurm`, `pbs`, or `local`).
-   **`default_conf` (Optional)**: A dictionary of default parameters that apply to all configurations under this cluster.
-   **`configs`**: A dictionary where each key is a unique configuration name. The values are the specific parameters for that configuration, which will override any defaults set in `default_conf`.

### Example Configuration File

Here is an example `my_configs.yaml` file defining configurations for two different clusters, `baldo` (using SLURM) and `hpc-unitn` (using PBS).

```yaml
baldo:
  scheduler: slurm
  default_conf:
    account: "default_account"
    modules:
      - "gcc/10.2.0"
      - "python/3.9.6"
  configs:
    cpu_job:
      partition: "cpu_queue"
      cpus_per_task: 4
      mem: "16G"
      time: "01-00:00:00"
    gpu_job:
      partition: "gpu_queue"
      cpus_per_task: 8
      mem: "32G"
      gpus: 1
      time: "02-00:00:00"

hpc-unitn:
  scheduler: pbs
  default_conf:
    queue: "default_queue"
  configs:
    small_mem_job:
      cpus: 2
      mem: "8gb"
      walltime: "01:00:00"
    large_mem_job:
      cpus: 4
      mem: "64gb"
      walltime: "12:00:00"
```

### Available Parameters

The parameters you can use depend on the scheduler specified for the cluster.

#### SLURM (`scheduler: slurm`)

-   `partition`
-   `nodes`
-   `ntasks`
-   `cpus_per_task`
-   `mem`
-   `account`
-   `time`
-   `gpus`
-   `constraint`
-   `nodelist`
-   `qos`
-   `reservation`
-   `env` (list of strings)
-   `modules` (list of strings)

#### PBS (`scheduler: pbs`)

-   `queue`
-   `cpus`
-   `mem`
-   `walltime`
-   `env` (list of strings)
-   `modules` (list of strings)

#### Local (`scheduler: local`)

-   `env` (list of strings)
-   `modules` (list of strings)