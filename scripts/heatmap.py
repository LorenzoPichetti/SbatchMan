import csv
import re

import numpy as np
import pandas as pd

# import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap, BoundaryNorm

from typing import Any, Callable

from common import Experiment, parse_results_csv, summarize_results

def generate_heatmaps(results: dict[str, list[Experiment]], 
                        x_axis_func: Callable[[dict[str, Any]], Any], 
                        y_axis_func: Callable[[dict[str, Any]], Any],
                        x_label=None,
                        y_label=None,
                        output_dir: str = ".") -> None:
    """
    Generate heatmaps for each experiment in the results dictionary.

    Args:
        results (dict[str, list[Experiment]]): Dictionary containing experiment data.
        x_axis_func (callable): Function to determine x-axis values from experiment parameters.
        y_axis_func (callable): Function to determine y-axis values from experiment parameters.
        output_dir (str): Directory to save the heatmap images. Defaults to the current directory.
    """
    for expname, experiments in results.items():
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

        output_path = f"{output_dir}/{expname}_heatmap.png"
        plt.tight_layout()
        plt.savefig(output_path)
        plt.close()
        print(f"Heatmap saved to '{output_path}'")

results = parse_results_csv('tmp.txt')
summarize_results(results)
generate_heatmaps(results, lambda p: p['n'], lambda p: p['r'], 'Scale', 'Seed')