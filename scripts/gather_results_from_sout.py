import os
import subprocess
import pathlib

# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '<PATH_TO_SBATCHMAN>')))
# from SbatchMan.scripts.common import *
from common import STATUS, parse_results_csv, summarize_results

SbM_HOME = os.environ.get('SbM_HOME')
if not SbM_HOME:
    print('SbatchMan Environment not set. Exiting...')
    exit(1)

HOST = subprocess.check_output([f"{SbM_HOME}/utils/hostname.sh"]).decode().strip()
METADATA_PATH = f'{SbM_HOME}/metadata/{HOST}'
SOUT_PATH = f'{SbM_HOME}/sout/{HOST}'

p = subprocess.Popen(['utils/overallTable.sh'], cwd=SbM_HOME, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
p.wait()

summary_file = pathlib.Path(f'{METADATA_PATH}/overallTable.csv')
results = parse_results_csv(summary_file)
print(summarize_results(results))

def parse_stdout(sout: str):
    return sout # Implement this

for category, category_res in results.items():
    category_runtimes = []
    for exp in category_res:
        if exp.status != STATUS.OK:
            print('Experiment failed: ', end='')
            print(exp)
            continue

        sout_filename = f'{SOUT_PATH}/{category}/{category}_{exp.id}.out'
        serr_filename = f'{SOUT_PATH}/{category}/{category}_{exp.id}.err' # You could do the same with this
        with open(sout_filename, 'r') as sout_file:
            print()
            print(exp)
            print('='*60)
            sout = sout_file.read()
            print(parse_stdout(sout))
            # Do whatever you want with your results...