flags=()                                                                                                                                         params=()
c="-c 5 -b abba -x 15 -a 27"

echo "input parameters: ${c}"

for x in ${c[@]}
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

echo "Flags:"
for e in ${flags[@]}
do 
	echo "  $e"
done

echo "Params:"
for e in ${params[@]}
do 
	echo "  $e"
done

echo "--------------------------"

readarray -td '' flags_sorted < <(printf '%s\0' "${flags[@]}" | sort -z)

echo "Flags sorted:"
for e in ${flags_sorted[@]}
do
	echo "  $e"
done

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

echo "Params sorted:"
for e in ${params_sorted[@]}
do
        echo "  $e"
done

echo "--------------------------"

outString=""
for i in ${!flags_sorted[@]}
do
	tmp="${flags_sorted[$i]}${params_sorted[$i]}"
	echo "tmp=$tmp"
	if [[ "$i" == "0" ]]
	then
		outString="$tmp"
	else
		outString+="_${tmp}"
	fi
done

echo "OutString: ${outString}"
