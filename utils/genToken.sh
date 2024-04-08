flags=()                                                                                                                                         params=()

for x in $@
do 
	if [[ "$x" == -* ]]
	then 
		#echo "flag: $x"
		flags+=( "${x/?}" )
	else 
		#echo "  param: $x"
		params+=( "$x" )
	fi
done

if [[ "${#flags[@]}" != "0" ]]
then

	if [[ "${#flags[@]}" != "${#params[@]}" ]]
	then
		echo "ERROR: the number of catched flags (${#flags[@]}) is different form the number of catched parameters (${#params[@]})"
		exit 1
	fi

readarray -td '' flags_sorted < <(printf '%s\0' "${flags[@]}" | sort -z)

params_sorted=()
for e in ${flags_sorted[@]}
do
	for i in ${!params[@]}
	do
        	if [[ "${flags[$i]}" == "$e" ]]
		then
			params_sorted+=( "${params[$i]}" )
		fi
	done
done

outString=""
for i in ${!flags_sorted[@]}
do
	tmp="${flags_sorted[$i]}${params_sorted[$i]}"
#	echo "tmp=$tmp"
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
