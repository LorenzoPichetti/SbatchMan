#!/bin/bash

echo "I took $# input arguments"
i=0
for e in $@
do
	echo "$i: $e"
	i=$((i+1))
done

echo "First  argument: $1"
echo "Second argument: $2"
echo "Third  argument: $3"

sbatch_script=$1
sbatch_arguments=$2
my_token=$3
my_metadata_path=$4

if ! grep -q "${my_token}" "${my_metadata_path}/finished.txt"
then

	job_id=$(sbatch ${sbatch_script} ${my_metadata_path} ${my_token} ${sbatch_arguments})

	job_id=$(echo "$job_id" | awk '{print $4}')
	echo "${my_token}      ${job_id}"
       	echo "${my_token}      ${job_id}" >> "${my_metadata_path}/launched.txt"
	return ${job_id}

else

	echo "the experiment ${my_token} is already listed in ${my_metadata_path}/finished.txt, so the experiment is performed yet."
       	echo "${my_token}" >> "${my_metadata_path}/notSubmitted.txt"
	return 0

fi
