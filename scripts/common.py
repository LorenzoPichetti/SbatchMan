from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Union
from pathlib import Path

class STATUS(Enum):
    OK = 1
    ERROR = -1
    TIMEOUT = 0

@dataclass
class Experiment:
    id: int
    name: str
    params: dict[str, Any]
    status: STATUS
    # status_val: Any = field(default=None)

# def parse_param(param_str: str) -> tuple[str, Any]:
#     # THIS IS JUST A DEFAULT IMPLEMENTATION
#     # IT ASSUMES param_str TO BE IN FORMAT pxx, 
#     # where "p" is a single char identifying the parameter,
#     # "xx" is a string identifying the value of the parameter
#     # Change this accordingly to your needs
#     return (param_str[0], param_str[1:])

PAIR_SEP="£"     # Separator between flag and its parameter
TOKEN_SEP="££"   # Separator between token entries
def parse_token(token: str) -> dict[str, Any]:
    params = {}
    params_s = token.split(TOKEN_SEP)
    for p in params_s:
        if PAIR_SEP in p:
            k, v = p.split(PAIR_SEP)
            params[k] = v
        else:
            params[p] = True
    return params

def parse_results_csv(input_filename: Union[str, Path], cb_parse_token: Callable[[str], dict[str, Any]]=parse_token) -> dict[str, list[Experiment]]:
    results = {}
    with open(input_filename, 'r') as input_file:
        for line in input_file:
            line = line.strip()
            if line.startswith('#'):
                continue
            parts = line.split(',')
            if len(parts) < 4:
                print(f'Warning: wrong result format "{line}"')
                continue
            parts = [p.strip() for p in parts]

            id = int(parts[0])
            expname = parts[1]
            params = parse_token(parts[2])
            finished = int(parts[-1])
            
            if expname not in results:
                results[expname] = []
            results[expname].append(Experiment(id, expname, params, STATUS(finished)))

    return results

def summarize_results(results: dict[str, list[Experiment]]) -> str:
    res = []
    for expname, experiments in results.items():
        total_experiments = len(experiments)
        status_counts = {status: 0 for status in STATUS}
        for experiment in experiments:
            status_counts[experiment.status] += 1

        res.append(f"Experiment: {expname}")
        res.append(f"  Total: {total_experiments}")
        for status, count in status_counts.items():
            res.append(f"  {status.name}: {count}")
    return '\n'.join(res)
