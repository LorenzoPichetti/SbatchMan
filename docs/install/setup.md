# ⚙️ Setup

Before continuing, **assign a name to your cluster**. This name helps organize configurations and manage jobs.  
Pick something descriptive, such as `research_cluster`, `test_cluster`, or `gpu_cluster`.  
Set the cluster name with:

```bash
sbatchman set-cluster-name <my_cluster_name>
```

This command will save the cluster name in your home configuration files. For example, on linux, at `~/.config/sbatchman/config.yaml`.

SbatchMan will automatically recognize the workload manager, here called *scheduler*, from your cluster configurations.

!!! note
    The name you assign to your cluster will be used to structure:  
    
    - Configurations
    - Job results