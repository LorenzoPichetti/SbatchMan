myhost=$( ${SbM_UTILS}/hostname.sh )

RED='\033[0;31m'
PUR='\033[0;35m'
GRE='\033[0;32m'
NC='\033[0m' # No Color

unset -v explist_flag
unset -v

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
	for exp_path in "${SbM_METADATA_HOME}/${myhost}"/*
	do
		if [[ -d "${exp_path}" ]]
		then
			exp_paths+=( ${exp_path} )
		fi
	done
else
	for provided_exp in $@
        do
		exp_path="${SbM_METADATA_HOME}/${myhost}/${provided_exp}"
                if [[ -d "${exp_path}" ]]
		then
			exp_paths+=( "${exp_path}" )
		else
			echo -e "${RED}Error${NC}: provided expname ${provided_exp} does not correspond to a folder in ${SbM_METADATA_HOME}/${myhost}/${provided_exp}"
			exit 1
		fi
        done
fi

# ----- DEBUG -----
#echo "exp_paths: ${exp_paths[*]}"
#exit 1
# -----------------

for exp_path in "${exp_paths[@]}"
do
	# ----- DEBUG -----
	#echo "exp_path: ${exp_path}"
	#continue
	# -----------------

	if [[ -d "${exp_path}" ]]
	then
		current_date=$(date +%Y-%m-%d)  # Format: YYYY-MM-DD
		current_time=$(date +%H:%M:%S)  # Format: HH:MM:SS
		exp=$( basename -- $exp_path )
		outfile="${exp_path}/${exp}SubmitTable.csv"

		echo "# ------- Experiment: ${exp} -------"                     >  "${outfile}"
		echo "# Table generated: $current_date $current_time"           >> "${outfile}"
		echo "# id,token,timeslaunched,timessubmitted,isfinished(0/1)"  >> "${outfile}"
		mapfile -t tokens < <( grep -v '^#' "${exp_path}/launched.txt" | awk '{ print $1 }' ) # Read tokens into an array
		mapfile -t ids < <( grep -v '^#' "${exp_path}/launched.txt" | awk '{ print $2 }' )    # Read ids into an array
		
		for i in "${!tokens[@]}"
		do
			id=${ids[$i]}
			token=${tokens[$i]}
			echo -e "ID $id TOKEN $token"
			timeslaunched=$( cat "${exp_path}/launched.txt" | grep -w "$token" | wc -l )
			timessubmitted=$( cat "${exp_path}/submitted.txt" | grep -w "$token" | wc -l )
			finished=$( cat "${exp_path}/finished.txt" | grep -w "$token" | wc -l )
			echo "$id,$token,${timeslaunched},${timessubmitted},${finished}"  >> "${outfile}"
		done
		echo "Generated ${exp} SubmitTable in ${outfile}"
	fi
done
