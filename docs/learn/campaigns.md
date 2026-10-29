# Campaigns

A campaign automates a sequence of ordinary SbatchMan work across applications and cluster names. It gives each application an isolated SbatchMan project, runs its steps in order, and records progress so a run can be monitored or resumed.

## Definition

```yaml
name: OpenMP benchmark campaign
parallel_clusters: true # Run selected clusters concurrently (default: false)
apps:
  - name: matrix
    dir: ./applications/matrix
    cluster_whitelist: [cpu-a]
    configs: [configs.yaml]
    steps:
      - name: compile
        script: make
      - name: small-runs
        jobs: runs.yaml
        jobs_args: ["-v", "dataset=small", "-t", "trial_*"]
      - name: large-runs
        jobs: runs.yaml
        jobs_args: ["-v", "dataset=large"]
```

The optional top-level `name` gives the campaign a human-readable label. It is saved in the journal and shown in the TUI alongside the unique campaign ID.

The YAML file may define multiple applications. `dir`, config files, job files, and scripts are resolved relative to the campaign YAML file or application directory as applicable: `dir` is relative to the YAML; `configs` and `jobs` paths are relative to the application directory. An application needs one or more steps. A step has a `script`, a `jobs` file, or both. Steps run in listed order; a jobs step waits for all submitted jobs to finish before the next step begins.

`cluster_whitelist` optionally limits an application to named clusters. With no whitelist, it runs on every selected cluster. A blacklist is unnecessary: select the clusters for the campaign, then use a whitelist when an application should run on only some of them.

Applications and steps remain ordered within each cluster. By default clusters are processed serially; set the top-level `parallel_clusters: true` to run the selected clusters concurrently. Jobs within a batch may also run concurrently according to their jobs YAML and scheduler. Each worker gets its own selected cluster and application project context. Scripts invoking SbatchMan inherit that scoped context.

## Runtime behavior

- **Pause is cooperative.** The TUI pause takes effect when a worker reaches a safe point: between apps or steps, or during job polling. A running script is not suspended. During polling, the worker notices pause on its next poll cycle, so it may take up to one polling interval to pause. With parallel clusters, workers share the pause request and each pauses when it reaches a safe point; other clusters can continue briefly while they finish their current operation.
- **Stop cancels orchestration.** Cancellation terminates a running campaign script and stops further campaign work as workers reach cancellation checks. It does not cancel jobs already submitted to the scheduler. The partial campaign journal is saved for later inspection or resume.
- **Step failure policy applies after a failed step.** `on_fails: terminate` (the default) stops that cluster's campaign flow; `continue` proceeds to the next step in the same app; `skip` skips the rest of that app on that cluster. A TUI user can instead choose Retry, Continue, Skip, or Stop when a step needs attention. Manual Continue marks the failed step completed; manual Skip marks it skipped.
- **Parallel clusters are independent flows.** Apps and steps keep their configured order within each cluster, but output and events from clusters may interleave. A failure stops that cluster's flow; other cluster workers may continue their work. Shared application project setup is serialized across clusters to avoid simultaneous config writes.
- **A campaign step waits for its submitted batch.** Jobs launched by a jobs step are polled until terminal status or the polling time limit. A campaign Stop does not cancel that batch. Jobs discovered outside the batch being monitored are reported for review rather than added to it.
- **Job-step duration comes from job runtimes.** Concurrent batches use the longest recorded job runtime; jobs marked `sequential: true` use the sum. Queue wait and campaign setup time are excluded. Script-only steps continue to use elapsed wall time.
- **Resume reuses completed steps selectively.** A step is reused only if its definition and referenced config/job inputs still match and all its recorded jobs are `COMPLETED`. Jobs in any other state, including cancelled jobs, are submitted again. A changed step runs again, while later completed steps can still be reused if their own definitions match; rerun downstream work when it depends on changed outputs.

## Batch arguments

`jobs_args` passes launch filters and duplicate-check options to a jobs step. It can be a YAML list of strings, a quoted command-like string, or a map with shared `default` arguments and per-cluster arguments:

```yaml
jobs_args: ["-v", "dataset=small", "-t", "trial_*"]

jobs_args:
  default: ["-v", "dataset=small", "-t", "trial_*"]
  cpu-b: ["-v", "dataset=large", "--force"]
```

The shared arguments apply to every cluster. A matching cluster entry is layered on top: tag filters are added, a cluster variable replaces the shared value for the same variable name, and flags such as `--force` or `-ia` are enabled if present in either list. Supported options are `-t/--tag`, `-v/--variable key=value`, `--force`, `-ia/--ignore-archived`, `-ic/--ignore-conf-in-dup-check`, and `-icomm/--ignore-commands-in-dup-check`. Repeat `-t` or `-v` to provide multiple filters. Campaign retries force submission automatically; `--force` is useful for a normal first attempt.

## Results and monitoring

By default campaign journals are written under `./campaign_results`. Each application's SbatchMan project, jobs, and logs are stored under that application's directory:

```text
campaign_results/
└── campaign_log_<timestamp>.json

<application-dir>/
└── SbatchMan/
```

Each application project is shared across that application's selected clusters. Campaign job commands run with the application directory as their working directory, and `SBATCHMAN_WD` points there. Use normal SbatchMan commands in `<application-dir>/SbatchMan` to inspect jobs and output. The journal is checkpointed as the campaign progresses and records step definitions, attempts, job IDs, statuses, and manual resolutions.

Export a journal and all of its application projects together:

```sh
sbatchman export-campaign campaign_results/campaign_log_<timestamp>.json
```

The command creates a `.tar.gz` beside the journal by default. Pass `--output bundle.tar.gz` to choose another destination. The archive contains `campaign_<id>/campaign.json` and `campaign_<id>/applications/<application-name>/SbatchMan/` for each recorded application project.

The TUI monitors step and job progress and offers Retry, Continue, Skip, and Stop when a step fails or unexpected jobs appear. Manual SbatchMan commands can be used while the campaign is active. Newly detected jobs are surfaced for review rather than silently adopted into the current batch. Continue records that the user resolved the step; Stop ends campaign orchestration but does not cancel scheduler jobs already submitted.

## Resume, edits, and partial runs

Resume a prior run from the TUI history. Resume and failed-app reruns update the selected journal in place, keeping the same campaign ID and history entry. Completed steps are reused only when their step definition and referenced application config/job files still match and every recorded job is `COMPLETED`; jobs in other states are submitted again. Changed or new steps run again; removed steps are omitted. Editing the YAML while a campaign is running does not change that in-memory run. Resume after editing to apply the new definition. Keep in mind that changing an earlier step may require rerunning later steps whose output depends on it.

The CLI can run only selected applications or steps:

```sh
sbatchman campaign campaign.yaml --application matrix --application solver
sbatchman campaign campaign.yaml --step compile --step small-runs
```

Selections retain campaign order. Step names are selected across the chosen applications; steps not selected are omitted. Include prerequisite steps yourself when needed (for example, select `compile` before a run step if the binary is not already available). CLI selections remain separate from a campaign's saved TUI selection.

When starting a new campaign in the TUI, you can select applications, their steps, and the campaign clusters each step should run on. Everything available is selected by default; an application's cluster whitelist limits the clusters offered for its steps. The selection is stored in that campaign's journal JSON. Open an existing campaign and choose **Edit selection** to change it; resume uses the saved selection.

Before a step retry, campaign resume, or failed-app rerun resubmits recorded jobs, the TUI lists the affected non-`COMPLETED` jobs and asks for confirmation. Failed-app reruns are limited to the affected steps and clusters. Completed jobs are reused and are never submitted again during recovery; existing job records and directories are retained.
