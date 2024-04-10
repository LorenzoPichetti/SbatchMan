myhost=$( ${SbM_UTILS}/hostname.sh )

for exp_path in "${SbM_METADATA_HOME}/${myhost}"/*
do
	if [[ -d "${exp_path}" ]]
	then
		current_date=$(date +%Y-%m-%d)  # Format: YYYY-MM-DD
		current_time=$(date +%H:%M:%S)  # Format: HH:MM:SS
		exp=$( basename -- $exp_path )
		outfile="${exp_path}/${exp}SubmitTable.csv"

		echo "# ------- Experiment: ${exp} -------"                     >  "${outfile}"
		echo "# Table generated: $current_date $current_time"           >> "${outfile}"
		echo "# token, timeslaunched, timessubmitted, isfinished(0/1)"  >> "${outfile}"
		alllaunched=$( cat "${exp_path}/launched.txt" | awk '{ print $1 }' | sort | uniq )
		for i in ${alllaunched}
		do
			timeslaunched=$( cat "${exp_path}/launched.txt" | grep "$i" | wc -l )
			timessubmitted=$( cat "${exp_path}/submitted.txt" | grep "$i" | wc -l )
			finished=$( cat "${exp_path}/finished.txt" | grep "$i" | wc -l )
			echo "$i, ${timeslaunched}, ${timessubmitted}, ${finished}"  >> "${outfile}"
		done
		echo "Generated ${exp} SubmitTable in ${outfile}"
	fi
done
