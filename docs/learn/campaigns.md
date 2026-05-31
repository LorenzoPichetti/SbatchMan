# Campaigns

Do you need to **run multiple applications/benchmarks together**?  
If so, this is what you need! 

**Key Features**
- Multi-cluster execution
- Multi-application execution
- Resume interrupted campaigns
- Automatic job monitoring
- Centralized logging
- Results collection

!!! note
    With `multi-cluster` means that SbatchMan will run the same campaign with different cluster names. It does NOT run the campaigns on remote systems via SSH. This is useful in case you cluster is a collection of machines with diverse characteristics.

**CLI Example**
```bash
sbatchman campaign campaign.yaml -c cluster-a -c cluster-b
```

## YAML Configuration Format

```yaml
apps:
  - name: my_app
    # Required
    # Unique application identifier
    # Type: string

    dir: ./path/to/application_wd
    # Required
    # Working directory of the application
    # Type: string (path)

    blocking: false
    # Optional
    # If true, waits for all jobs to finish before moving to next app
    # Type: boolean
    # Default: false

    cluster_whitelist:
      - cluster-a
      - cluster-b
    # Optional
    # Only execute on these clusters
    # Type: list[string]

    cluster_blacklist:
      - cluster-c
    # Optional
    # Skip execution on these clusters
    # Type: list[string]

    configs:
      - configs.yaml
      - compile_configs.yaml
    # Optional
    # SbatchMan configuration files
    # Relative to `dir`
    # Type: list[string] or string

    steps:
      - name: compile
        # Required
        # Unique step name within the application
        # Type: string

        script: "rm -rf bin; mkdir bin"
        # Optional (if not set, `jobs` is required)
        # Shell command executed before launching jobs
        # Type: string

        jobs: compile_jobs.yaml
        # Optional (if not set, `script` is required)
        # SbatchMan jobs YAML file
        # Relative to `dir`
        # Type: string

        on_fails: terminate
        # Optional
        # Failure handling policy
        # Values:
        #   - terminate : stop entire campaign
        #   - continue  : continue to next step
        #   - skip      : skip remaining steps and move to next app
        # Default: terminate

      - name: experiments
        jobs: jobs.yaml
        on_fails: continue

  - name: 
```

Check out this example: [https://github.com/ThomasPasquali/SbatchManTutorial/blob/main/campaign.yaml](https://github.com/ThomasPasquali/SbatchManTutorial/blob/main/campaign.yaml)

<!-- **Field Definitions:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | str | ✓ | Unique app identifier |
| `dir` | str | ✓ | Path to app directory (can be relative) |
| `blocking` | bool | | Wait for all steps before next app (default: false) |
| `cluster_whitelist` | List[str] | | Only run on these clusters (AND logic with CLI clusters) |
| `cluster_blacklist` | List[str] | | Skip these clusters (exclusion filter) |
| `configs` | List[str] \| str | | Paths to sbatchman config files, relative to `dir` |
| `steps[].name` | str | ✓ | Unique step identifier within app |
| `steps[].script` | str | | Bash script to execute before launching jobs |
| `steps[].jobs` | str | ✓ | Path to sbatchman jobs YAML file (relative to `dir`) |
| `steps[].on_fails` | str | | Behavior on failure: `terminate` (exit), `continue` (ignore), `skip` (next app) | -->

---

## Simplified Execution Flow

```python
for cluster in clusters
  # sbatchman set-cluster-name <cluster>

  for app in apps:
    # cd app working directory
    # sbatchman init
    # sbatchman configure -f <config files>

    for step in app.steps:
      # <step script>
      # sbatchman launch -f <step jobs>
```

Results structure
```
path/to/app1
├── ...
└── SbatchMan

path/to/app2
├── ...
└── SbatchMan
```

Each `SbatchMan` directory may contain results from multiple clusters.

!!! tip
    Multiple `SbatchMan` directories can be merged! This way you can access all jobs at once. To automate data collection check out the [Results](learn/results.md) page.