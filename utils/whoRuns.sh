myhost=$( ${SbM_UTILS}/hostname.sh )
myuser=$( ${SbM_UTILS}/whoami.sh )

for id in $(squeue -u ${myuser} -t R | awk '{ print $1 }')
do
	for exp_path in "${SbM_METADATA_HOME}/${myhost}"/*
        do
		if [[ -d "${exp_path}" ]]
	        then	
			if [ "$id" != "JOBID" ]
			then 
				me=$( cat "${exp_path}/launched.txt" | grep "$id" | wc -l )
				if [ "$1" == "1" ] && [ "$me" -gt "0" ]
				then
					echo "---------------------------------------------------------"
				fi
				cat "${exp_path}/launched.txt" | grep "$id"
				if [ "$1" == "1" ] && [ "$me" -gt "0" ]
				then
					echo "---------------------------------------------------------"
					sacct -o JobID,JobName,Partition,State,Elapsed,ExitCode | grep "$id"
					echo "---------------------------------------------------------"
				fi
			fi
		fi
	done
done
