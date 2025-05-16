#!/bin/bash

# === Token parsing configuration ===
PAIR_SEP="£"
TOKEN_SEP="££"

# FIXME does not work properly
token_parse() {
  local token="$1"
  local -n _out="$2"   # Output array passed by reference
  IFS="${TOKEN_SEP}" read -ra entries <<< "$token"
  _out=()
  for entry in "${entries[@]}"; do
    if [[ "$entry" == -*"${PAIR_SEP}"* ]]; then
      flag="${entry%%${PAIR_SEP}*}"
      param="${entry#${flag}${PAIR_SEP}}"
      _out+=( "$flag" "$param" )
    else
      _out+=( "$entry" "" )
    fi
  done
}

# token_args=()
# token_parse "--matA£mycielskian12.mtx££--matB£mycielskian12.mtx" token_args

# echo '======= TOKEN ============='
# for e in "${token_args[@]}"; do
# 	printf ",%s" "$e"
# done
# exit 0

myhostname=$( "${SbM_UTILS}/hostname.sh" )

RED='\033[0;31m'
PUR='\033[0;35m'
GRE='\033[0;32m'
NC='\033[0m'

unset -v explist_flag

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
if [[ -z ${explist_flag} ]]; then
  for exp_path in "${SbM_METADATA_HOME}/${myhostname}"/*; do
    [[ -d "${exp_path}" ]] && exp_paths+=( "${exp_path}" )
  done
else
  for provided_exp in "$@"; do
    exp_path="${SbM_METADATA_HOME}/${myhostname}/${provided_exp}"
    if [[ -d "${exp_path}" ]]; then
      exp_paths+=( "${exp_path}" )
    else
      echo -e "${RED}Error${NC}: provided expname '${provided_exp}' does not correspond to a folder in ${SbM_METADATA_HOME}/${myhostname}/"
      exit 1
    fi
  done
fi

# ----- DEBUG -----
echo "exp_paths: ${exp_paths[*]}"
# -----------------

echo "---------------------------------------------------------------------"
echo "Generating SubmitTables..."
${SbM_UTILS}/genSubmitTables.sh ${args}
echo "---------------------------------------------------------------------"

tmpfile="tmpfile.txt"
tablename="${SbM_METADATA_HOME}/${myhostname}/overallTable.csv"
timelimittable="${SbM_METADATA_HOME}/${myhostname}/timelimitTable.csv"

current_date=$(date +%Y-%m-%d)
current_time=$(date +%H:%M:%S)

echo "# ------- Overall table of ${myhostname} -------"         >  "${tablename}"
echo "# Table generated: $current_date $current_time"           >> "${tablename}"
echo "# id,expname,token,isfinished(0/1)"  >> "${tablename}"
# echo "# id,expname,parameter0,parameter1,...,isfinished(0/1)"  >> "${tablename}"

echo "# ------- Timelimit table of ${myhostname} -------"       >  "${timelimittable}"
echo "# Table generated: $current_date $current_time"           >> "${timelimittable}"
echo "# id,expname,parameter0,parameter1,...,timelimit"         >> "${timelimittable}"

for p in "${exp_paths[@]}"; do
  for f in "${p}"/*SubmitTable.csv; do
    [[ -f $f ]] || continue

	expname=$(head -1 "$f" | awk '{ print $4 }')
	echo "expname: ${expname}"
	grep -v "#" "$f" > "$tmpfile"

	my_timelimitfile="${SbM_METADATA_HOME}/${myhostname}/${expname}/timeLimit.txt"
	[[ -f "${my_timelimitfile}" ]] || echo "# Init file ${current_date} ${current_time}" > "${my_timelimitfile}"

	my_notfinishedfile="${SbM_METADATA_HOME}/${myhostname}/${expname}/notFinished.txt"
	[[ -f "${my_notfinishedfile}" ]] || echo "# Init file ${current_date} ${current_time}" > "${my_notfinishedfile}"

	while read -r line; do
		myid=$(echo "$line" | awk -F',' '{ print $1 }')
		mytoken=$(echo "$line" | awk -F',' '{ print $2 }')
		finished=$(echo "$line" | awk -F',' '{ print $NF }')

		# Remove experiment name from start of token
		noprefix=${mytoken#${expname}}

		# token_args=()
		# token_parse "$noprefix" token_args

		# echo '======= TOKEN ============='
		# echo $token_args

		# if [[ ${#token_args[@]} -eq 0 ]]; then
		# 	echo -e "${RED}Error:${NC} Failed to parse token '${noprefix}' from line: ${line}" >&2
		# 	continue
		# fi

		# Compose comma-separated line for overallTable
		{
		  printf "%s,%s" "$myid" "$expname"
		  printf ",%s" "$noprefix"
		#   for e in "${token_args[@]}"; do
		#   	printf ",%s" "$e"
		#   done
		  printf ",%s\n" "$finished"
		} >> "${tablename}"

		if [[ "${finished}" -eq "0" ]]; then
		timelimit="0"

		jid_vec=()
		while read -r jid; do
			jid_vec+=( "$jid" )
		done < <(grep "${mytoken}" "${SbM_METADATA_HOME}/${myhostname}/${expname}/launched.txt" | awk '{ print $3 }')

		for jid in "${jid_vec[@]}"; do
			if ! grep -q "${jid}" "${my_timelimitfile}" && ! grep -q "${jid}" "${my_notfinishedfile}"; then
			echo "Search ${jid} in sacct..."
			tmpsacct=$(sacct -o Jobid,State,TimelimitRaw -j "${jid}" | head -3 | tail -n 1)
			state=$(echo "${tmpsacct}" | awk '{ print $2 }')
			timelimitraw=$(echo "${tmpsacct}" | awk '{ print $3 }')

			if [[ "${state}" == "TIMEOUT" ]]; then
				echo "${mytoken} ${jid} ${timelimitraw}" >> "${my_timelimitfile}"
				(( timelimitraw > timelimit )) && timelimit="${timelimitraw}"
			else
				echo "${mytoken} ${jid} ${state}" >> "${my_notfinishedfile}"
			fi
			else
			if ! grep -q "${jid}" "${my_notfinishedfile}"; then
				timelimitraw=$(grep "${jid}" "${my_timelimitfile}" | awk '{ print $3 }')
				(( timelimitraw > timelimit )) && timelimit="${timelimitraw}"
			fi
			fi
		done
		else
		timelimit="-1"
		fi

		if [[ "${timelimit}" != "-1" && "${timelimit}" != "0" ]]; then
		echo "${line}"
		echo "TIME LIMIT: ${timelimit}"
		fi

		tstring="${expname},"
		for e in "${token_args[@]}"; do
			tstring+="${e},"
		done
		tstring+="${timelimit}"
		echo "${tstring}" >> "${timelimittable}"

	done < "$tmpfile"
  done

  rm -f "$tmpfile"
done

echo "Generated OverallTable in ${tablename}"
