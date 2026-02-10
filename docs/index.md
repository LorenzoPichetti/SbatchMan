# Welcome to SbatchMan!
<p align="center" style="padding: 0; margin: 0;">
  <img src="images/SbatchManLogo.png" alt="SbatchMan Logo" style="width: 6cm;"/>
</p>
SbatchMan simplifies the process of running and managing code experiments locally and on multiple remote clusters. It streamlines your workflow, allowing you to focus on your research rather than the complexities of job submission and cluster management.

To get started, check out the [installation guide](install/install.md) to set up SbatchMan on your system.  
Then, follow the [tutorial](learn/tutorial.md) to learn how to use SbatchMan effectively.  
If you prefer to learn by doing, check out this hands-on tutorial: [https://github.com/ThomasPasquali/SbatchManTutorial](https://github.com/ThomasPasquali/SbatchManTutorial)

!!! tip "The question is..."
    Have you ever found yourself developing programs, algorithms, benchmarks that need to be run with multiple combinations of resources, parameters, environments (potentially) on different systems or supercomputers?  
    Have you ever struggled managing hundreds of output files?  
    Are you tired to write programs that ensure that all results are nicely written to CSV files or structured folders?  
    If so, **you are in the right place**.

## Features
- **Flexible configurations**: Use YAML files to define multiple configurations (resources allocation, environment variables, modules etc.) for different clusters.
- **Job management**: Launch, monitor, and archive jobs with simple commands.
- **Powerful job launching**: Launch jobs with custom commands and configurations defined in a YAML file, automatically generating all combinations of variables.
- **Command-line interface**: Interact with SbatchMan through a user-friendly CLI.
- **Python API**: Integrate SbatchMan into your Python scripts to parse job results effortlessly.
- **Archive utility**: Keep track of your past experiments and their results.
- **User friendly**: All configurations and logs are stored locally within your project directory, making it easy to manually inspect the commands and results.

### Supported Workload Managers

- **SLURM**
- **PBS**
- SbatchMan also allows to create configurations and run jobs on your local machine.

!!! tip
    Curios about use cases and SbatchMan capabilities?  
    Check out this benchmarks collection based on SbatchMan: [https://github.com/HicrestLaboratory/HICREST-Benchmark-Collection](https://github.com/HicrestLaboratory/HICREST-Benchmark-Collection)

### User Interface

*The SbatchMan TUI provides an interactive way to view the status of your jobs directly from the terminal.*

![SbatchMan TUI Screenshot](images/tui.png)

