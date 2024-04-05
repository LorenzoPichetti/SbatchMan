#!/bin/bash

if [[ $# -lt 2 ]]
then
	echo "Usage: <my_token> <binary> <binary_arguments>"
	exit 1
fi

sbatch_arguments=()
binary=$2
my_token=$1

if ! [[ -f ${SbM_EXPTABLE} ]]
then
	echo "No exptable file was found (${SbM_EXPTABLE}), <write what to check or to do>"
	exit 1
fi


if grep -q "${binary}" "${SbM_EXPTABLE}"
then
	expname=$( grep ${binary} ${SbM_EXPTABLE} | awk '{ print $1 }' )
	sbatch_script=$( grep ${binary} ${SbM_EXPTABLE} | awk '{ print $3 }' )
else
	echo "Error: the binary ${binary} in not reported in the ExpTable (${SbM_EXPTABLE}), please, init the expariment with <...>"
	exit 1
fi

my_hostname=$( ${SbM_UTILS}/hostname.sh )
my_metadata_path="${SbM_METADATA_HOME}/${my_hostname}/${expname}"

mkdir -p "${my_metadata_path}" # ${SbM_METADATA_HOME}/${my_hostname} was already created for ExpTable

i=0
for a in $@
do
	if [[ "$i" -gt "1" ]]
	then
		sbatch_arguments+=( $a )
	fi
	i=$(( $i +1 ))
done

echo "          expname: ${expname}"
echo "         my_token: ${my_token}"
echo "    sbatch_script: ${sbatch_script}"
echo " sbatch_arguments: ${sbatch_arguments[*]}"
echo " my_metadata_path: ${my_metadata_path}"

mkdir -p "${my_metadata_path}"
if ! [[ -f "${my_metadata_path}/finished.txt" ]]
then
	echo "# Init file" > "${my_metadata_path}/finished.txt"
fi

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
