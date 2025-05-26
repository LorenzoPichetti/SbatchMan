import csv
import re
import os
import argparse
import subprocess

import numpy as np
import pandas as pd

# import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap, BoundaryNorm
from pathlib import Path

from typing import Any, Callable

# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '<PATH_TO_SBATCHMAN>')))
# from SbatchMan.scripts.common import *
from common import Experiment, parse_results_csv, summarize_results

def generate_heatmaps(results: dict[str, list[Experiment]], 
                        x_axis_func: Callable[[dict[str, Any]], Any], 
                        y_axis_func: Callable[[dict[str, Any]], Any],
                        x_label=None,
                        y_label=None,
                        output_dir: str = ".") -> None: 
    for expname, experiments in results.items():
        generate_heatmap(expname, experiments, x_axis_func, y_axis_func, x_label, y_label, output_dir)

def generate_heatmap(expname: str,
                        experiments: list[Experiment],
                        x_axis_func: Callable[[dict[str, Any]], Any], 
                        y_axis_func: Callable[[dict[str, Any]], Any],
                        x_label=None,
                        y_label=None,
                        output_dir: str = ".") -> None:
    """
    Generate heatmaps for each experiment in the results dictionary.

    Args:
        expname (str): Experiment name.
        experiments (list[Experiment]): Experiment results.
        x_axis_func (callable): Function to determine x-axis values from experiment parameters.
        y_axis_func (callable): Function to determine y-axis values from experiment parameters.
        output_dir (str): Directory to save the heatmap images. Defaults to the current directory.
    """
    
    # Create a DataFrame for the heatmap
    data = []
    for experiment in experiments:
        x_value = x_axis_func(experiment.params)
        y_value = y_axis_func(experiment.params)
        data.append((x_value, y_value, experiment.status.value))

    df = pd.DataFrame(data, columns=['x', 'y', 'status'])

    # Pivot the DataFrame to create a 2D matrix for the heatmap
    heatmap_data = df.pivot(index='y', columns='x', values='status')
    heatmap_data.replace(np.nan, -2)

    # Create the heatmap
    plt.figure(figsize=(10, 8))
    cmap = ListedColormap(['red', 'yellow', 'green'])  # ERROR, TIMEOUT, OK
    bounds = [-1, 0, 1]
    norm = BoundaryNorm(bounds, 4)

    plt.imshow(heatmap_data, cmap=cmap, norm=norm, aspect='auto')
    cbar = plt.colorbar(ticks=[-0.5, 0, 0.5], label='Status')
    cbar.ax.set_yticklabels(['T/O', 'ERR', 'OK']) #'-'
    plt.title(f"Heatmap for {expname}")
    if x_label: plt.xlabel(x_label)
    if y_label: plt.xlabel(y_label)
    plt.xticks(ticks=range(len(heatmap_data.columns)), labels=heatmap_data.columns, rotation=45)
    plt.yticks(ticks=range(len(heatmap_data.index)), labels=heatmap_data.index)

    output_path = Path(output_dir, f'{expname}_heatmap.png')
    os.makedirs(output_dir, exist_ok=True)
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()
    print(f"Heatmap saved to '{output_path}'")


def main(args):
    SbM_HOME = os.environ.get('SbM_HOME')
    SbM_METADATA_HOME = os.environ.get('SbM_METADATA_HOME')
    if not SbM_HOME or not SbM_METADATA_HOME:
        print("Make sure that SbatchMan environment is set properly.")
        exit(1)

    exp_list = args.exp_list
    if len(exp_list) <= 0:
        exptable_path = Path(os.environ.get('SbM_EXPTABLE', 'DOES_NOT_EXIST'))
        if not exptable_path.exists():
            print("Could not find 'expTable.csv'. Make sure that SbatchMan environment is set properly.")
            exit(1)

        with open(exptable_path, 'r') as f:
            exp_file = f.read().split('\n')[1:]
            exp_list = [e.split(' ')[0] for e in exp_file if e.split(' ')[0]]

    print('Generating overall table...')
    os.system(f'{SbM_HOME}/utils/overallTable.sh --exp-list {" ".join(exp_list)}')

    hostname = subprocess.check_output([f'{SbM_HOME}/utils/hostname.sh']).decode().strip()
    results = parse_results_csv(Path(f'{SbM_METADATA_HOME}/{hostname}/overallTable.csv'))
    summarize_results(results)
    # print(results)

    for exp in exp_list:
        exp_results = results.get(exp)
        if exp_results:
            generate_heatmap(
                exp, exp_results,
                lambda p: p[list(p.keys())[0]],
                lambda p: p[list(p.keys())[1]],
                'X', 'Y',
                args.out_path
            )
        else:
            print(f'No results for experiment "{exp}"')
        

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SbatchMan - Heatmap - a utility that generates an heatmap for each experiment, resuming their state.")
    parser.add_argument("--exp-list", "-e", nargs="+", required=False, help="A list of 'experiments' to consider", default=[])
    parser.add_argument("--out-path", "-o", type=str, required=False, help="The path were plots will be written", default='.')
    args = parser.parse_args()

    main(args)