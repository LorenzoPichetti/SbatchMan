#!/bin/bash

myhostname=$( ${SbM_UTILS}/hostname.sh )

tmpfile="tmpfile.txt"
tablename="${SbM_METADATA_HOME}/${myhostname}/overallTable.csv"

current_date=$(date +%Y-%m-%d)  # Format: YYYY-MM-DD
current_time=$(date +%H:%M:%S)  # Format: HH:MM:SS

echo "# ------- Overall table of ${myhostname} -------"         >  "${tablename}"
echo "# Table generated: $current_date $current_time"           >> "${tablename}"
echo "# expname, parameter0, parameter1, ..., isfinished(0/1)"  >> "${tablename}"

for f in "${SbM_METADATA_HOME}/${myhostname}"/*/*SubmitTable.csv
do 
	echo " ----- $f -----"
	expname=$(head -1 $f | awk '{ print $4 }' )
	echo "expname: ${expname}"
	grep -v "#" $f > ${tmpfile}
	
	while read line
	do 
		tmp=$( echo ${line} | awk -F',' '{ print $1 }' )
		noprefix=${tmp#"$expname"_}
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
	done < ${tmpfile}
	rm ${tmpfile}

	#exit 1 # DEBUUG
done
