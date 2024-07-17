#!/bin/bash

SbM_submit_function() {

job_id="0"

if [[ $# -lt 1 ]]
then
	echo "Usage: [--verbose] [--expname <expname>] --binary <binary> <binary_arguments>"
	return 1
fi

RED='\033[0;31m'
PUR='\033[0;35m'
GRE='\033[0;32m'
NC='\033[0m' # No Color

_V=0
unset -v binary
unset -v expname
unset -v testflag
unset -v holdflag
unset -v dependency
unset -v nicepriority

while [[ $# -gt 0 ]]; do
  case "$1" in
    --binary)
      if [[ -n "$2" ]]; then
        binary="$2"
        [ $_V -eq 1 ] && echo "binary is set to ${binary}"
        shift
      else
        echo "Error: Binary requires a value."
        return 1
      fi
      break
      ;;
    --expname)
      if [[ -n "$2" ]]; then
        expname="$2"
        [ $_V -eq 1 ] && echo "expname is set to ${expname}"
        shift
      else
        echo "Error: Expname requires a value."
        return 1
      fi
      ;;
    --dependency)
      if [[ -n "$2" ]]; then
        dependency="$2"
        [ $_V -eq 1 ] && echo "dependency is set to ${dependency}"
        shift
      else
        echo "Error: Dependency requires a value."
        return 1
      fi
      ;;
    --nice)
      if [[ -n "$2" ]]; then
        nicepriority="$2"
        [ $_V -eq 1 ] && echo "nicepriority is set to ${nicepriority}"
        shift
      else
        echo "Error: nicepriority requires a value."
        return 1
      fi
      ;;
    --test)
	  testflag="1"
	  [ $_V -eq 1 ] && echo "testflag is now set"
# 	  shift
      ;;
    --hold)
          holdflag="1"
          [ $_V -eq 1 ] && echo "holdflag is now set"
	  echo "holdflag is now set" #debug
#         shift
      ;;
    --verbose)
	  _V=1
	  echo "verbose flag is now set"
# 	  shift
      ;;
    --help|*)
        echo "Unrecognized element: $1"
	echo "Usage: [--expname <expname>] --binary <binary> <binary_arguments>"
        return 1
      ;;
  esac
  shift
done

sbatch_arguments=()
# binary=$1

if ! [[ -f ${SbM_EXPTABLE} ]]
then
	echo "No exptable file was found (${SbM_EXPTABLE}), <write what to check or to do>"
	return 1
fi

if grep -q "${binary}" "${SbM_EXPTABLE}"
then
	noccurrency="$( grep -w "${binary}" "${SbM_EXPTABLE}" | wc -l )"
	if [[ "${noccurrency}" -eq "1" ]]
	then
		# TODO add check that (if expname specified) expnames (generated and inputed) are the same
		expname=$( grep -w ${binary} ${SbM_EXPTABLE} | awk '{ print $1 }' )
		sbatch_script=$( grep -w ${binary} ${SbM_EXPTABLE} | awk '{ print $3 }' )
	else
		if [ -z "${expname}" ]
		then
			echo -e "${RED}Error${NC}: You must specify an expname with --expname since binary ${binary} occurr ${noccurrency} times:" >&2
			echo "$( head -1 ${SbM_EXPTABLE} )"
			echo "$( grep ${binary} ${SbM_EXPTABLE})"
			return 1
		else
			if ! grep -q "${expname} ${binary}" "${SbM_EXPTABLE}"
			then
				echo -e "${RED}Error${NC}: no experiment with expname ${expname} is given for binary ${binary}:"
				echo "$( head -1 ${SbM_EXPTABLE} )"
				echo "$( grep ${binary} ${SbM_EXPTABLE})"
				return 1
			fi
			sbatch_script=$( grep -w ${expname} ${SbM_EXPTABLE} | awk '{ print $3 }' )
		fi 
	fi
else
	echo -e "${RED}Error${NC}: the binary ${binary} in not reported in the ExpTable (${SbM_EXPTABLE}), please, init the expariment with <...>"
	return 1
fi

my_hostname=$( ${SbM_UTILS}/hostname.sh )
my_metadata_path="${SbM_METADATA_HOME}/${my_hostname}/${expname}"

mkdir -p "${my_metadata_path}" # ${SbM_METADATA_HOME}/${my_hostname} was already created for ExpTable

SbM_submit_i=0
for a in $@
do
	if [[ "${SbM_submit_i}" -gt "0" ]]
	then
		sbatch_arguments+=( $a )
	fi
	SbM_submit_i=$(( ${SbM_submit_i} +1 ))
done

token_suffix=$( ${SbM_UTILS}/genToken.sh ${sbatch_arguments[*]} )
my_token="${expname}${token_suffix}"

sbatch_cmd="sbatch "
if [ ! -z "${holdflag}" ]
then
	sbatch_cmd+="--hold "
fi

if [ ! -z "${dependency}" ]
then
	sbatch_cmd+="--dependency=${dependency} "
fi

if [ ! -z "${nicepriority}" ]
then
	sbatch_cmd+="--nice=${nicepriority} "
fi


[ $_V -eq 1 ] && echo "          expname: ${expname}"
[ $_V -eq 1 ] && echo "         my_token: ${my_token}"
[ $_V -eq 1 ] && echo "       sbatch_cmd: ${sbatch_cmd}"
[ $_V -eq 1 ] && echo "    sbatch_script: ${sbatch_script}"
[ $_V -eq 1 ] && echo " sbatch_arguments: ${sbatch_arguments[*]}"
[ $_V -eq 1 ] && echo " my_metadata_path: ${my_metadata_path}"

current_date=$(date +%Y-%m-%d)  # Format: YYYY-MM-DD
current_time=$(date +%H:%M:%S)

if ! [[ -f "${my_metadata_path}/finished.txt" ]]
then
	echo "# ----- Init file ${current_date} ${current_time} -----" > "${my_metadata_path}/finished.txt"
fi

if ! [[ -f "${my_metadata_path}/notFinished.txt" ]]
then
	echo "# ----- Init file ${current_date} ${current_time} -----" > "${my_metadata_path}/notFinished.txt"
fi

if ! [[ -f "${my_metadata_path}/submitted.txt" ]]
then
        echo "# ----- Init file ${current_date} ${current_time} -----" > "${my_metadata_path}/submitted.txt"
fi

if ! [[ -f "${my_metadata_path}/notSubmitted.txt" ]]
then
        echo "# ----- Init file ${current_date} ${current_time} -----" > "${my_metadata_path}/notSubmitted.txt"
fi

if ! [[ -f "${my_metadata_path}/launched.txt" ]]
then
        echo "# ----- Init file ${current_date} ${current_time} -----" > "${my_metadata_path}/launched.txt"
fi

if ! [[ -f "${my_metadata_path}/notLaunched.txt" ]]
then
        echo "# ----- Init file ${current_date} ${current_time} -----" > "${my_metadata_path}/notLaunched.txt"
fi

if ! grep -q "${my_token}" "${my_metadata_path}/finished.txt"
then
	isinqueue=$( ${SbM_UTILS}/inQueue.sh | grep ${my_token} | wc -l )
	if [[ "${isinqueue}" -eq "0" ]]
	then
		mytimelimit=$( grep "#SBATCH --time=" ${sbatch_script} | awk -F'=' '{ print $2 }' )
		#echo "DEBUG mytimelimit: ${mytimelimit}"
		if [[ "$( echo ${mytimelimit} | grep -o ":" | wc -l )" -gt "0" ]]
		then
			myminuteslimit=$( ${SbM_UTILS}/hhmmss2mm.sh ${mytimelimit} )
		else
			myminuteslimit="${mytimelimit}"
		fi
		#echo "DEBUG myminuteslimit: ${myminuteslimit}"

		if [[ -f "${my_metadata_path}/timeLimit.txt" ]] && grep -q "${my_token}" "${my_metadata_path}/timeLimit.txt"
		then
			#echo "DEBUG mytoken: ${my_token} file: ${my_metadata_path}/timeLimit.txt"
			#grep "${my_token}" "${my_metadata_path}/timeLimit.txt" | sort -n -k 3 -r
			maxperf=$( grep "${my_token}" "${my_metadata_path}/timeLimit.txt" | sort -n -k 3 -r | head -1 | awk '{ print $3 }' )
			#echo "DEBUG maxperf: ${maxperf}"

			if [[ "${myminuteslimit}" -gt "${maxperf}" ]]
			then
				echo -e "${GRE}NOTE${NC}: the experiment ${my_token} was already lunched with ${RED}timelimit${NC} ${maxperf}, it will be lunched again with time limit ${myminuteslimit}."
			else
				echo -e "${PUR}Warning${NC}: the experiment ${my_token} was already lunched with longerequal ${RED}timelimit${NC} (${maxperf}), so the experiment was not submitted again."
				return 1
			fi
		fi

		if [ -z "${testflag}" ]
		then
			job_id=$( ${sbatch_cmd} ${sbatch_script} ${my_metadata_path} ${my_token} ${sbatch_arguments[*]} )
			job_id=$(echo "$job_id" | awk '{print $4}')
			echo -e "${GRE}Launched${NC}: ${my_token}      ${job_id}"
			echo "${my_token}      ${job_id}" >> "${my_metadata_path}/launched.txt"
		else
			echo -e "${PUR}Test mode${NC}: sbatch [sbatch commands] <sbatch_script> <my_metadata_path> <my_token> <bin_arguments>"
			echo -e "${PUR}Test mode${NC}: ${sbatch_cmd} ${sbatch_script} ${my_metadata_path} ${my_token} ${sbatch_arguments[*]}"
		fi
	else
		echo -e "${PUR}Warning${NC}: the experiment ${my_token} is already in queue, so the experiment was not submitted again."
		echo "${my_token}" >> "${my_metadata_path}/notLaunched.txt"
	fi

else

	echo -e "${PUR}Warning${NC}: the experiment ${my_token} is already listed in ${my_metadata_path}/finished.txt, so the experiment is performed yet."
	echo "${my_token}" >> "${my_metadata_path}/notLaunched.txt"

fi

return 0

}

#SbM_submit_function $@
#echo "Inside return: $?"
