#!/bin/bash

myhostname=$( ${SbM_UTILS}/hostname.sh )

RED='\033[0;31m'
PUR='\033[0;35m'
GRE='\033[0;32m'
NC='\033[0m' # No Color

unset -v explist_flag
unset -v

args=$*

while [[ $# -gt 0 ]]; do
  case "$1" in
    --all)
      echo "All flag provided"
      all_flag="1"
      break
      ;;
    --exp-list)
      echo "Exp-list flag provided"
      explist_flag="1"
      shift
      break
      ;;
    --help|*)
      echo "Usage: [ --all | --exp-list <exp1> <exp2> ... ] (default is --all)"
      exit 1
      ;;
  esac
  shift
done


exp_paths=()
if [[ -z ${explist_flag} ]]
then
        for exp_path in "${SbM_METADATA_HOME}/${myhostname}"/*
        do
                if [[ -d "${exp_path}" ]]
                then
                        exp_paths+=( ${exp_path} )
                fi
        done
else
        for provided_exp in $@
        do
                exp_path="${SbM_METADATA_HOME}/${myhostname}/${provided_exp}"
                if [[ -d "${exp_path}" ]]
                then
                        exp_paths+=( "${exp_path}" )
                else
                        echo -e "${RED}Error${NC}: provided expname ${provided_exp} does not correspond to a folder in ${SbM_METADATA_HOME}/${myhostname}/${provided_exp}"
                        exit 1
                fi
        done
fi

# ----- DEBUG -----
echo "exp_paths: ${exp_paths[*]}"
#echo "args: ${args}"
#exit 1
# -----------------

echo "---------------------------------------------------------------------"
echo "Generating SubmitTables..."
${SbM_UTILS}/genSubmitTables.sh ${args}
echo "---------------------------------------------------------------------"

tmpfile="tmpfile.txt"
tablename="${SbM_METADATA_HOME}/${myhostname}/overallTable.csv"
timelimittable="${SbM_METADATA_HOME}/${myhostname}/timelimitTable.csv"

current_date=$(date +%Y-%m-%d)  # Format: YYYY-MM-DD
current_time=$(date +%H:%M:%S)  # Format: HH:MM:SS

echo "# ------- Overall table of ${myhostname} -------"         >  "${tablename}"
echo "# Table generated: $current_date $current_time"           >> "${tablename}"
echo "# expname, parameter0, parameter1, ..., isfinished(0/1)"  >> "${tablename}"

echo "# ------- Timelimit table of ${myhostname} -------"       >  "${timelimittable}"
echo "# Table generated: $current_date $current_time"           >> "${timelimittable}"
echo "# expname, parameter0, parameter1, ..., isfinished(0/1)"  >> "${timelimittable}"

#for f in "${SbM_METADATA_HOME}/${myhostname}"/*/*SubmitTable.csv
for p in "${exp_paths[@]}"
do 
	f="${p}/"*SubmitTable.csv

	echo " ----- $f -----"
	expname=$(head -1 $f | awk '{ print $4 }' )
	echo "expname: ${expname}"
	grep -v "#" $f > ${tmpfile}

	my_timelimitfile="${SbM_METADATA_HOME}/${myhostname}/${expname}/timeLimit.txt"
	if ! [[ -f "${my_timelimitfile}" ]]
	then
		echo "# ----- Init file ${current_date} ${current_time} -----" > "${my_timelimitfile}"
	fi

	my_notfinishedfile="${SbM_METADATA_HOME}/${myhostname}/${expname}/notFinished.txt"
	if ! [[ -f "${my_notfinishedfile}" ]]
	then
		echo "# ----- Init file ${current_date} ${current_time} -----" > "${my_notfinishedfile}"
	fi
	
	while read line
	do 
		mytoken=$( echo ${line} | awk -F',' '{ print $1 }' )
		noprefix=${mytoken#"$expname"_}
		tmp=$( echo "${noprefix}" | awk -F'_' '{ print $2 }' )
		IFS='_' read -r -a arguments <<< "${noprefix}"
		finished=$( echo ${line} | awk -F',' '{ print $NF }' )

		string="${expname}, "
		for e in ${arguments[@]}
		do
			string+="$e, "
		done
		string+="${finished}"

		# ---------- debug ----------
                #echo "line: ${line}"
                #echo "noprefix: ${noprefix}"
                #echo "finished: ${finished}"
                #echo "arguments: ${arguments[*]}"
		#echo "string: ${string}"
		# ---------------------------

		echo "${string}" >> ${tablename}

		if [[ "${finished}" -eq "0" ]]
		then
			timelimit="0"

			jid_vec=()
			tmp=$( echo ${line} | awk -F',' '{ print $1 }' )
			#echo "tmp: ${tmp}"
			#grep "${tmp}" "${SbM_METADATA_HOME}/${myhostname}/${expname}/launched.txt"
			for jid in $( grep "${tmp}" "${SbM_METADATA_HOME}/${myhostname}/${expname}/launched.txt" | awk '{ print $2 }' )
			do
				jid_vec+=( "${jid}" )
			done
			#echo "jid_vec: ${jid_vec[*]}"

			for jid in ${jid_vec[@]}
			do
				if ! grep -q "${jid}" "${my_timelimitfile}" && ! grep -q "${jid}" "${my_notfinishedfile}"
				then
					echo "Search ${jid} in sacct..."
					tmpsacct=$( sacct -o Jobid,State,TimelimitRaw -j ${jid} | head -3 | tail -n 1 )
					state=$( echo ${tmpsacct} | awk '{ print $2 }' )
					timelimitraw=$( echo ${tmpsacct} | awk '{ print $3 }' )

					if [[ "${state}" == "TIMEOUT" ]]
					then
						#echo "debug: ${state} ${timelimitraw}"
						echo "${mytoken} ${jid} ${timelimitraw}" >> ${my_timelimitfile}
						if [[ "${timelimitraw}" -gt "${timelimit}" ]]
						then
							timelimit="${timelimitraw}"
						fi
					else
						echo "${mytoken} ${jid} ${state}" >> ${my_notfinishedfile}
					fi
				else
					if ! grep -q "${jid}" "${my_notfinishedfile}"
					then
						timelimitraw=$( grep "${jid}" "${my_timelimitfile}" | awk '{ print $3 }' )
						if [[ "${timelimitraw}" -gt "${timelimit}" ]]
						then
							timelimit="${timelimitraw}"
						fi
					fi
				fi
			done
		else
			timelimit="-1"
		fi
		
		if [[ "${timelimit}" != "-1" ]] && [[ "${timelimit}" != "0" ]]
		then
			echo "${line}"
			echo "TIME LIMIT: ${timelimit}"
		fi

		string="${expname}, "
                for e in ${arguments[@]}
                do
                        string+="$e, "
                done
                string+="${timelimit}"
		#echo "timelimitstring: ${string}"

		echo "${string}" >> ${timelimittable}

	done < ${tmpfile}
	rm ${tmpfile}

	#exit 1 # DEBUUG
done
