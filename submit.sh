#!/bin/bash

if [[ $# -lt 3 ]]
then
	echo "Usage: <my_metadata_path> <my_token> <sbatch_script> <sbatch_arguments>"
	exit 1
fi

my_metadata_path=$1
sbatch_arguments=()
sbatch_script=$3
my_token=$2

i=0
for a in $@
do
	if [[ "$i" -gt "2" ]]
	then
		sbatch_arguments+=( $a )
	fi
	i=$(( $i +1 ))
done

echo " sbatch_arguments: ${sbatch_arguments[*]}"
echo " my_metadata_path: ${my_metadata_path}"
echo "    sbatch_script: ${sbatch_script}"
echo "         my_token: ${my_token}"

if ! grep -q "${my_token}" "${my_metadata_path}/finished.txt"
then

	job_id=$(sbatch ${sbatch_script} ${my_metadata_path} ${my_token} ${sbatch_arguments[*]})

	job_id=$(echo "$job_id" | awk '{print $4}')
	echo "${my_token}      ${job_id}"
       	echo "${my_token}      ${job_id}" >> "${my_metadata_path}/launched.txt"
#	return ${job_id}

else

	echo "the experiment ${my_token} is already listed in ${my_metadata_path}/finished.txt, so the experiment is performed yet."
       	echo "${my_token}" >> "${my_metadata_path}/notSubmitted.txt"
	return 0

fi
