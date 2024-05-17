flags=()
params=()
wichParam=()

RED='\033[0;31m'
PUR='\033[0;35m'
GRE='\033[0;32m'
NC='\033[0m' # No Color

flagId=-1
paramId=-1
for x in $@
do 
	if [[ "$x" == -* ]]
	then 
		let "flagId=flagId+1"
# 		echo "flag: $x ($flagId)"
		flags+=( "${x/?}" )
		wichParam[${flagId}]=-1
	else 
		let "paramId=paramId+1"
# 		echo "  param: $x ($paramId)"
		basename=$( basename -- $x )
		plainname=${basename%.*}
		name=$( echo "$plainname" | tr _ - )
		params+=( "$name" )

		if [[ "${flagId}" != "-1" ]] # portable for ordinal parameters
		then
			if [[ "${wichParam[${flagId}]}" == "-1" ]]
			then
				wichParam[${flagId}]=${paramId}
			else
				echo -e "${RED}Error${NC}: multiple parameter per flag are not alowed, flagId ${flagId} (${flags[${flagId}]}) already set param of id ${wichParam[${flagId}]} (${params[${wichParam[${flagId}]}]}), you can not set also parameter of id ${paramId} (${params[${paramId}]})"
				exit 1
			fi
		fi

	fi
done

# echo "inputs: "
# for i in ${!flags[@]}
# do
# 	if [[ "${wichParam[$i]}" != "-1" ]]
# 	then
# 		echo "${flags[$i]}: ${params[${wichParam[$i]}]} (wichParam: ${wichParam[$i]})"
# 	else
# 		echo "${flags[$i]}: No-parameter (wichParam: ${wichParam[$i]})"
# 	fi
# done

if [[ "${#flags[@]}" != "0" ]]
then

# 	if [[ "${#flags[@]}" != "${#params[@]}" ]]
# 	then
# 		echo -e "${RED}Error${NC}: the number of catched flags (${#flags[@]}) is different form the number of catched parameters (${#params[@]})"
# 		exit 1
# 	fi

	readarray -td '' flags_sorted < <(printf '%s\0' "${flags[@]}" | sort -z)

# 	params_sorted=()
# 	for e in ${flags_sorted[@]}
# 	do
# 		for i in ${!params[@]}
# 		do
# 				if [[ "${flags[$i]}" == "$e" ]]
# 			then
# 				params_sorted+=( "${params[$i]}" )
# 			fi
# 		done
# 	done

	outString=""
	for i in ${!flags_sorted[@]}
	do
# 		echo "Flag: ${flags_sorted[$i]}"
		for j in ${!flags[@]}
		do
			if [[ "${flags[$j]}" == "${flags_sorted[$i]}" ]]
			then
				origId=$j
# 				echo "    origId:        ${origId}"
# 				echo "    origWichParam: ${wichParam[${origId}]}"
# 				if [[ "${wichParam[${origId}]}" != "-1" ]]
# 				then
# 					echo "    param:         ${params[${wichParam[${origId}]}]}"
# 				else
# 					echo "    param:         No-param"
# 				fi
				break
			fi
		done

		if [[ "${wichParam[${origId}]}" != "-1" ]]
		then
			tmp="${flags_sorted[$i]}${params[${wichParam[${origId}]}]}"
		else
			tmp="${flags_sorted[$i]}"
		fi

# 		echo "    tmp: $tmp"
		outString+="_${tmp}"
	done

	else

	outString=""
	for e in ${params[@]}
	do
			outString+="_${e}"
	done

fi


echo "${outString}"
