# Results Collection and Parsing

!!! tip
    First, if you need to collect results from multiple remote machines, check out the [Fetch](learn/fetch.md) page.

Once you have all you results in one `SbatchMan` project, you can use the `jobs_list` or `jobs_to_dataframe()` Python APIs to get your results.

## Manual Results Parsing

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
- Metadata (fields): `config_name`, `cluster_name`, `status`, `tag`, `job_id`, `exitcode`, `archive_name`, `sbm_queue_time_s`, `sbm_run_time_s`


## Web-UI

SbatchMan provides and interactive web user interface to simplify data management and visualization.

See example in the [Tutorial Repository](https://github.com/ThomasPasquali/SbatchManTutorial/tree/main/campaign/program2)

To let the web ui understand your results, create (in your app directory) a `parser.py` file which provides a `def parse(job: sbm.Job) -> dict | None` function.

`parse` should return either:

- `None / {}` if the job produced no rows, or
- a `dict` mapping `table_name -> row(s)`, where each value is either
    - a single row: a dict of {column_name: value}, or
    - multiple rows: a list of such dicts.

This lets the user:

- choose table names freely (dict keys)
- emit any number of rows per job (list values)
- emit rows into multiple tables from one job (multiple dict keys)

Then, run

```bash
sbatchman visualize
```

And, in you browser, go to: `http://localhost:8765/`