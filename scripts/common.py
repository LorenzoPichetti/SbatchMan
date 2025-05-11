from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable

class STATUS(Enum):
    OK = -1
    ERROR = 1
    TIMEOUT = 0

@dataclass
class Experiment:
    name: str
    params: dict[str, Any]
    status: STATUS
    status_val: Any = field(default=None)

def parse_param(param_str: str) -> tuple[str, Any]:
    # THIS IS JUST A DEFAULT IMPLEMENTATION
    # IT ASSUMES param_str TO BE IN FORMAT pxx, 
    # where "p" is a single char identifying the parameter,
    # "xx" is a string identifying the value of the parameter
    # Change this accordingly to your needs
    return (param_str[0], param_str[1:])

def parse_results_csv(input_filename, cb_parse_param: Callable[[str], tuple[str, Any]]=parse_param) -> dict[str, list[Experiment]]:
    results = {}
    with open(input_filename, 'r') as input_file:
        for line in input_file:
            line = line.strip()
            if line.startswith('#'):
                continue
            parts = line.split(',')
            if len(parts) < 4:
                continue
            parts = [p.strip() for p in parts]

            expname = parts[0]
            params = [cb_parse_param(p) for p in parts[1:-1]]
            finished = int(parts[-1])
            
            if expname not in results:
                results[expname] = []
            results[expname].append(Experiment(expname, {p[0]:p[1] for p in params}, STATUS(finished)))

    return results

def summarize_results(results: dict[str, list[Experiment]]) -> None:
    for expname, experiments in results.items():
        total_experiments = len(experiments)
        status_counts = {status: 0 for status in STATUS}
        for experiment in experiments:
            status_counts[experiment.status] += 1

        print(f"Experiment: {expname}")
        print(f"  Total: {total_experiments}")
        for status, count in status_counts.items():
            print(f"  {status.name}: {count}")
        print()
