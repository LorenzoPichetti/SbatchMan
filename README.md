# SbatchMan
A framework for managing slurm scripts 

### Description:
This repository serves as a foundational environment setup for the [YourProjectName] project. It includes scripts and configurations to initialize the environment necessary for seamless development and execution within the project's ecosystem.

### Getting Started:

To utilize the resources provided by this repository, follow the steps outlined below:

1. **Clone Repository:**
    ```bash
    git clone https://github.com/LorenzoPichetti/SbatchMan.git
    ```

2. **Navigate to Repository:**
    ```bash
    cd SbatchMan
    ```

3. **Initialize Environment:**
    Execute the `initEnv.sh` script to set up the environment variables and directory structure required for the project:
    ```bash
    ./initEnv.sh
    ```

4. **Source Configuration File:**
    Source the generated `sourceFile.sh` to activate the environment variables in your current session:
    ```bash
    source sourceFile.sh
    ```

### Environment Variables:

- **SbM_HOME**: Represents the root directory of the project.
- **SbM_SOUT**: Directory for storing output files generated during project execution.
- **SbM_UTILS**: Contains utility scripts necessary for various project tasks.
- **SbM_SBATCH**: Directory for storing Slurm batch scripts.
- **SbM_EXPTABLE**: Path to the experiment table file.
- **SbM_METADATA_HOME**: Root directory for metadata storage.

### Generating Experiments:

To generate experiments use the `newExperiment.sh` script. Below is an explanation of its usage:

```bash
#!/bin/bash

./SbatchMan/newExperiment.sh -p medium -a my.slurm.account -t 02:00:00 -e Exp-2proc  -n 1 -c 2 -g 2 -b bin/myBin -d 1
./SbatchMan/newExperiment.sh -p medium -a my.slurm.account -t 02:00:00 -e Exp-4proc  -n 1 -c 4 -g 4 -b bin/myBin -d 1
./SbatchMan/newExperiment.sh -p medium -a my.slurm.account -t 02:00:00 -e Exp-8proc  -n 1 -c 8 -g 8 -b bin/myBin -d 1
./SbatchMan/newExperiment.sh -p medium -a my.slurm.account -t 02:00:00 -e Exp-16proc  -n 2 -c 8 -g 8 -b bin/myBin -d 1
```

#### `newExperiment.sh` Usage:

The `newExperiment.sh` script is used to generate experiments with specified parameters. Below is a brief description of its usage:

- **Mandatory Arguments**:
  - `-t <time in HH:MM:SS>`: Specify the SLURM max time (HH:MM:SS).
  - `-e <exp-name>`: Specify the experiment name.
  - `-b <binary>`: Specify the binary path.
  - `-n <nnodes>`: Specify the number of required SLURM nodes.
  - `-c <ntasks>`: Specify the number of required SLURM tasks.

- **Optional Arguments**:
  - `-g <ngpus>`: Specify the number of required GPUs.
  - `-p <partition_name>`: Specify the SLURM partition name.
  - `-a <slurm_account>`: Specify the SLURM account.
  - `-M <MPI-version>`: Specify the SLURM MPI version (--mpi=).
  - `-d <cpus-per-task>`: Specify the number of CPUs per task.
  - `-s <constraint>`: Specify the SLURM constraint.
  - `-m <memory>`: Specify the allocated memory.
  - `-q <qos>`: Specify the SLURM QoS.
  - `-S <qos>`: Specify a non-standard ServiceLevel (i.e. export NCCL_IB_SL).
  - `-w <nodelist>`: Specify a specific slurm nodelist
	- `-P <preprocessblock>`: Specify a bash preprocess block of code

Ensure you provide all mandatory arguments when generating experiments. You can refer to the example provided for guidance on how to structure your experiment generation commands.

```bash
./SbatchMan/newExperiment.sh -p <partition_name> -a <slurm_account> -M <MPI-version> -t <time in HH:MM:SS> -e <exp-name> -n <nnodes> -c <ntasks> -g <ngpus> -b <binary>
```

## `submit.sh`

### Description:
The `submit.sh` script facilitates the submission of experiments to a SLURM-based HPC system. It automates the process of selecting the appropriate SLURM batch script and managing experiment metadata.

### Usage:
```bash
./submit.sh [--verbose] [--expname <expname>] --binary <binary> <binary_arguments>
```

### Options:

- `--verbose`: Use verbose submission process.
- `--expname <expname>`: Specifies the name of the experiment.
- `--binary <binary>`: Specifies the binary path.
- `<binary_arguments>`: Additional arguments required by the binary.

### Script Logic:

1. **Argument Validation:**
   - The script first checks if the correct number of arguments has been provided. If not, it displays usage instructions and exits with an error.
   
2. **Parsing Arguments:**
   - The script parses the command-line arguments using a `while` loop and a `case` statement. It identifies the `--binary` and `--expname` flags and assigns their corresponding values to variables.

3. **Finding SLURM Batch Script:**
   - The script searches for the SLURM batch script associated with the specified binary in the experiment table (`SbM_EXPTABLE`). It checks if the binary is listed in the table and retrieves the corresponding SLURM batch script.

4. **Generating Token:**
   - A unique token is generated for the experiment based on its parameters using the `genToken.sh` script. This token serves as an identifier for the experiment.

5. **Creating Metadata Directory:**
   - The script creates a directory to store experiment metadata. The directory path is constructed based on the hostname and experiment name.

6. **Initializing Metadata Files:**
   - Metadata files (`finished.txt`, `notFinished.txt`, `submitted.txt`, `notSubmitted.txt`, `launched.txt`, and `notLaunched.txt`) are initialized for the experiment. These files track the status of the experiment throughout its lifecycle.

7. **Submitting Experiment:**
   - Before submitting the experiment, the script checks if it is already in the SLURM queue. If not, it submits the experiment to the SLURM queue using the appropriate batch script. The experiment ID and status are recorded in the metadata files.

### Example Usage:
```bash
./submit.sh --expname AxBC-3d-2 --binary src/axbc2d -p medium -a flavio.vella -M pmix -t 02:00:00 -n 1 -c 2 -g 2 -d 1
```

### Notes:

- **Configuration Requirements:**
  - Before using `submit.sh`, ensure that the `SbM_EXPTABLE` file is correctly configured with experiment details, including binary paths and associated SLURM batch scripts.

- **Testing Mode:**
  - To test the script without submitting an actual experiment, use the `--test` flag. This flag prints the submission command without executing it.

- **Metadata Files:**
  - The script creates and manages several metadata files to track the status of experiments (`finished.txt`, `notFinished.txt`, `submitted.txt`, `notSubmitted.txt`, `launched.txt`, and `notLaunched.txt`). These files are located in the experiment's metadata directory.

- **Troubleshooting:**
  - If an error occurs during submission, check the metadata files and SLURM queue for relevant information. The metadata files provide insights into the experiment's status and history.

- **Customization:**
  - You can customize the behavior of `submit.sh` by modifying the script logic or adding additional features to meet specific requirements.

- **Documentation:**
  - Refer to the script documentation and comments for detailed information on its usage, options, and internal logic.


## Utils Scripts

To run these scripts, run: `utils/<script_name>.sh`

### `inQueue`

Prints a list of experiments currently in queue.

### `whoRuns`

Prints a list of experiments currently running.

### `archiveExperiments`

Passing an `experiment name`, it will move its `sout` and `metadata` folders into the corresponding `.old` folders, appending the current timestamp.

### `overallTable.sh`

Generates a `overallTable.csv` summarizing the status of the experiments.


## Python Scripts

First, make sure Python virtual enviromnent is set:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### `common`

Contains base data structures and functions to parse experiments results.

> NOTE: to include `common.py` make sure to load it into Python path:
```python
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '<PATH_TO_SBATCHMAN>')))
from SbatchMan.scripts.common import *
```

### `heatmap`

An example of how you can create an heatmap plot containing the "status" of the experiments.

By default the x-axis and y-axis of the heatmap are set to the 1st and 2nd CLI args, respectively.

> **Just copy and customize the script**

### `gather_results_from_sout.py`

An example of how to parse the STDOUT files from the experiments you have just ran.

This currently just prints the experiment details together with their output.

> **Just copy and customize the script**

> To ensure jobs are done before performing results analysis you could use a script like `gather_results_from_sout.sh`



## Complete Usage Example


First, setup environment variables:

```bash
./initEnv.sh && source sourceFile.sh
```

Then, go to the `example` directory and build the targets:

```bash
cd example
make all
```

Now, create the different SLURM configurations for the expermiments. The `setup_sbatch.sh` script contains an example, run it:

```bash
./setup_sbatch.sh
```

This will populate the `sbatchscripts` directory with SLURM script templates, one for each different configuration.

Finally, run a set of experiments using both setup SLURM configurations using the `run_experiments.sh` script:

```bash
./run_experiments.sh
```

This will populate the `sout/<hostname>/<SLURM_config>` folders with the experiments output files. Also, the `metadata/<hostname>/<SLURM_config>` will contain txt files with a resume about the final status of the jobs (e.g., finished, notFinished, launched, notLauched).

> **_NOTE:_**  If you want to reset the current state of the experiments, run: `rm -r sout/<hostname>/* metadata/<hostname>/* sbatchscripts/*`

## Results Analysis

This repository provides some scripts to facilitate the results analysis. Once the jobs are done, run:

```bash
utils/overallTable.sh # From the root folder
```

This will generate a CSV file containing a list of all issued experiments toghether with their final status. The folder `scripts` contains a Python script `heatmap.py` that will generate a heatmap for each "expname", resuming what ran succesfully, timeouts and errors.

> **_Python Dependencies:_**  `pandas numpy matplotlib`