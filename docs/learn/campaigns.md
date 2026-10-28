# Campaigns

A campaign automates a sequence of ordinary SbatchMan work across applications and cluster names. It gives each application an isolated SbatchMan project, runs its steps in order, and records progress so a run can be monitored or resumed.

## Definition

```yaml
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

The YAML file may define multiple applications. `dir`, config files, job files, and scripts are resolved relative to the campaign YAML file or application directory as applicable: `dir` is relative to the YAML; `configs` and `jobs` paths are relative to the application directory. An application needs one or more steps. A step has a `script`, a `jobs` file, or both. Steps run in listed order; a jobs step waits for all submitted jobs to finish before the next step begins.

`cluster_whitelist` optionally limits an application to named clusters. With no whitelist, it runs on every selected cluster. A blacklist is unnecessary: select the clusters for the campaign, then use a whitelist when an application should run on only some of them.

Applications and clusters are processed serially. Jobs within one batch may run concurrently according to their jobs YAML and scheduler. The campaign scopes the selected cluster and project for each application; it does not change the user's global cluster selection. Scripts invoking SbatchMan inherit that scoped context.

## Batch arguments

`jobs_args` passes launch filters and duplicate-check options to a jobs step. It can be a YAML list of strings, as above, or a quoted command-like string. Supported options are `-t/--tag`, `-v/--variable key=value`, `--force`, `-ia/--ignore-archived`, `-ic/--ignore-conf-in-dup-check`, and `-icomm/--ignore-commands-in-dup-check`. Repeat `-t` or `-v` to provide multiple filters. Campaign retries force submission automatically; `--force` is useful for a normal first attempt.

## Results and monitoring

By default results are written under `./campaign_results`:

```text
campaign_results/
├── campaign_log_<timestamp>.json
└── applications/<application-name>/SbatchMan/
```

Each application project is shared across that application's selected clusters. Use normal SbatchMan commands in its project directory to inspect jobs and output. The journal is checkpointed as the campaign progresses and records step definitions, attempts, job IDs, statuses, and manual resolutions.

The TUI monitors step and job progress and offers Retry, Continue, Skip, and Stop when a step fails or unexpected jobs appear. Manual SbatchMan commands can be used while the campaign is active. Newly detected jobs are surfaced for review rather than silently adopted into the current batch. Continue records that the user resolved the step; Stop ends campaign orchestration but does not cancel scheduler jobs already submitted.

## Resume, edits, and partial runs

Resume a prior run from the TUI history. A resume creates a new journal and keeps the selected historical journal intact. Completed steps are reused only when their step definition and referenced application config/job files still match. Changed or new steps run again; removed steps are omitted. Editing the YAML while a campaign is running does not change that in-memory run. Resume after editing to apply the new definition. Keep in mind that changing an earlier step may require rerunning later steps whose output depends on it.

The CLI can run only selected applications or steps:

```sh
sbatchman campaign campaign.yaml --application matrix --application solver
sbatchman campaign campaign.yaml --step compile --step small-runs
```

Selections retain campaign order. Step names are selected across the chosen applications; steps not selected are omitted. Include prerequisite steps yourself when needed (for example, select `compile` before a run step if the binary is not already available). The CLI selection options are not currently part of the TUI history flow; choose a full resume there, or use the CLI to start a selected run.
