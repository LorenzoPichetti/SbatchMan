#!/bin/bash

mkdir -p "${SbM_HOME}/outputs"
my_hostname=$( ${SbM_UTILS}/hostname.sh )

compute_acct() {
    acct_head=$(sacct -o JobID,JobName,Partition,State,Start,Elapsed,TimelimitRaw,ExitCode | head -2)
    acct=$(sacct -o JobID,JobName,Partition,State,Start,Elapsed,TimelimitRaw,ExitCode | grep "${job_id}")
    echo "${acct_head}" >> ${1}
    echo "${acct}" >> ${1}

#     echo "primo input: $1 !!"
}

RED='\033[0;31m'
PUR='\033[0;35m'
GRE='\033[0;32m'
NC='\033[0m' # No Color


compute_singlefile() {
	file=$1
	filename=$( basename -- ${file} )

	type="${filename##*.}"
        job_id=$( echo "${file##*_}" | cut -d'.' -f 1 )

        if [[ "${type}" == "out" ]]
        then
        	token=$( grep "my_token" ${file} | awk '{ print $2 }' )
        else
        	tmpfile=$( echo ${file} | sed 's/.err/.out/g' )
        	token=$( grep "my_token" ${tmpfile} | awk '{ print $2 }' )
        fi

        newname="${token}_${job_id}.${type}"
        #echo "file:    ${file}"
        #echo "name:    ${filename}"
        #echo "token:   ${token}"
        #echo "job_id:  ${job_id}"
        #echo "newname: ${newname}"

        #continue #debug


	inQueue=$( ${SbM_HOME}/utils/inQueue.sh | awk '{ print $2 }' )
        if ${SbM_HOME}/utils/inQueue.sh | grep -q "${job_id}"
        then
        	isInQueue="1"
        	echo -e "${PUR}Warning${NC}: ${job_id} is in queue: nothing done"
        else
        	isInQueue="0"

#           echo "${SbM_METADATA_HOME}/${my_hostname}/${exp}/finished.txt"
        	if grep -q "${job_id}" "${SbM_METADATA_HOME}/${my_hostname}/${exp}/finished.txt"
        	then
                	echo "${job_id} finished correctly, stdout and stderr moved to finished_path"
                	mv "${file}" "${finished_path}/${newname}"

                	if [[ "${type}" == "out" ]]
                	then
                    		compute_acct "${finished_path}/${newname}"
                	fi
            	else
                	echo -e "${RED}Error${NC}: ${job_id} goes in error, moved to error_path"
                	mv "${file}" "${error_path}/${newname}"

                	if [[ "${type}" == "out" ]]
                	then
                    		compute_acct "${error_path}/${newname}"
                	fi
            	fi
        fi
}

# Initialize variables
njobs=1  # Default value for jobs if not specified

# Parse command-line options
while getopts "j:" opt; do
  case ${opt} in
    j )
      njobs=$OPTARG  # Set the jobs variable to the value provided with -j
      ;;
    \? )
      echo "Usage: cmd [-j number_of_jobs]" >&2
      exit 1
      ;;
  esac
done

# Shift to leave only non-option arguments
shift $((OPTIND -1))

# Use the jobs variable as needed
echo "Number of jobs specified: $jobs"



for exp_path in $( cat ${SbM_EXPTABLE} | grep -v "#" | awk '{ print $1 }' )
do
    #echo "exp_path: ${exp_path}"
    exp=$( basename -- ${exp_path} )

    sout_path="${SbM_HOME}/sout/${my_hostname}/${exp}"
    out_path="${SbM_HOME}/outputs/${my_hostname}/${exp}"
    error_path="${SbM_HOME}/outputs/${my_hostname}/${exp}/errors"
    finished_path="${SbM_HOME}/outputs/${my_hostname}/${exp}/finished"

    echo "-----------------------------------------------------------"
    echo "Experiment: ${exp}"
    echo -e "\tsout_path:     ${sout_path}"
    echo -e "\terror_path:    ${error_path}"
    echo -e "\tfinished_path: ${finished_path}\n"

    mkdir -p ${out_path}
    mkdir -p ${error_path}
    mkdir -p ${finished_path}


    if [[ "$( ls ${sout_path} | wc -l )" != "0" ]]
    then
	parallelizationi=1
        for loopfile in "${sout_path}"/*
        do
		((parallelizationi=parallelizationi%njobs)); ((parallelizationi++==0)) && wait
        	compute_singlefile ${loopfile} &
	done
	wait
    else
        echo -e "${PUR}Warning${NC}: ${sout_path} is empty"
    fi
done
