# Results Collection and Parsing

!!! tip
    First, if you need to collect results from multiple remote machines, check out the [Fetch](learn/fetch.md) page.

Once you have all you results in one `SbatchMan` project, you can use the `jobs_list` or `jobs_to_dataframe()` Python APIs to get your results.

### Example

```python
import sbatchman as sbm

jobs: List[sbm.Job] = sbm.jobs_list(status=[sbm.Status.COMPLETED])
# here you have ALL non-archived completed jobs
```

alternative

```python
def job_filter(job: sbm.Job) -> bool:
    return not job.get_stdout() and job.clutser_name != 'cluster-I-dont-want'

def extract_problem_size(job: sbm.Job) -> dict:
    exe, positional, kwargs = job.parse_command_args()
    return {
        "executable": exe,
        "size": kwargs.get("size"),
    }

def extract_flops(job: sbm.Job) -> dict:
    stdout = job.get_stdout()
    m = re.search(r"FLOPS:\s*([0-9.eE+-]+)", stdout)
    if not m:
        return {}
    return {
        "flops": float(m.group(1))
    }

df = sbm.jobs_to_dataframe(
    status=[sbm.Status.COMPLETED],
    job_filter=job_filter,
    extractors=[
        extract_problem_size,
        extract_flops,
    ],
    include_job_fields=True,
    include_job_variables=True,
)
```

The resulting `pandas.DataFrame` columns are the union of:
- User-defined: `executable`, `size`, `flops`
- YAML variables: all variables used in the jobs' wildcards `{var_name}`
- Metadata: `config_name`, `cluster_name`, `status`, `tag`, `job_id`, `exitcode`, `archive_name`, `sbm_queue_time_s`, `sbm_run_time_s`