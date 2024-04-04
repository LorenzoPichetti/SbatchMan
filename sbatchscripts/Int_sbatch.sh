#!/bin/bash

#SBATCH --job-name=Int
#SBATCH --output=/home/lorenzo.pichetti/vectorAddTest/SbatchMan/sout/marzola/Int/Int_%j.out
#SBATCH --error=/home/lorenzo.pichetti/vectorAddTest/SbatchMan/sout/marzola/Int/Int_%j.err

#SBATCH --partition=short
#SBATCH --account=flavio.vella
#SBATCH --time=00:05:00

#SBATCH --nodes=1
#SBATCH --gres=gpu:0
#SBATCH --tasks=1
#SBATCH --cpus-per-task=1

my_metadata_path=$1
my_token=$2

i=0
arguments=()
for a in $@
do
        if [[ "$i" -gt "1" ]]
        then
                arguments+=( $a )
        fi
        i=$(( $i +1 ))
done

echo " ------------ Int ------------ "
echo "         my_token: $my_token"
echo " my_metadata_path: $my_metadata_path"
echo "        arguments: ${arguments[*]}"

echo "${my_token}" >> "${my_metadata_path}/submitted.txt"

echo "srun bin/testInt ${arguments[*]}"
srun bin/testInt ${arguments[*]}

if [[ $? == 0 ]]
then
    echo "${my_token}" >> "${my_metadata_path}/finished.txt"
    acct_head=$(sacct -o JobID,JobName,Partition,State,Start,Elapsed,ExitCode | head -2)
    acct=$(sacct -o JobID,JobName,Partition,State,Start,Elapsed,ExitCode | grep "${SLURM_JOB_ID}")
    echo "${acct_head}" >> "${my_metadata_path}/finished_sacct.txt"
    echo "${acct}"      >> "${my_metadata_path}/finished_sacct.txt"
    echo "SLURM_JOB_ID: ${SLURM_JOB_ID}"
    echo "${acct_head}"
    echo "${acct}"
else
    echo "${my_token} not written in '${my_metadata_path}/finished.txt' since the exit code is different form 0 ($?)"
fi


echo "------------------------"
