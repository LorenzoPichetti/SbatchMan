my_hostname=$(${SbM_UTILS}/hostname.sh)

if [[ $# -lt 1 ]]
then
	echo "Usage: $0 <expname>"
	return 1
fi

if [[ (-d "${SbM_HOME}/sout/$my_hostname/$1") || (-d "${SbM_HOME}/metadata/$my_hostname/$1") ]]; then
    echo -e "${PUR}Old experiment data found for $1${NC}"
    hidden_dir_sout="${SbM_HOME}/sout/$my_hostname/.old_sout"
    hidden_dir_meta="${SbM_HOME}/metadata/$my_hostname/.old_metadata"
    mkdir -p "$hidden_dir_sout" "$hidden_dir_meta"

    timestamp=$(date +%Y-%m-%d_%H-%M-%S)

    if [[ -d "${SbM_HOME}/sout/$my_hostname/$1" ]]; then
        mv "${SbM_HOME}/sout/$my_hostname/$1" "$hidden_dir_sout/${1}_$timestamp"
        echo -e "${GRE}Moved old 'sout' data to $hidden_dir_sout/${1}_$timestamp${NC}"
    fi
    if [[ -d "${SbM_HOME}/metadata/$my_hostname/$1" ]]; then
        mv "${SbM_HOME}/metadata/$my_hostname/$1" "$hidden_dir_meta/${1}_$timestamp"
        echo -e "${GRE}Moved old 'meta' data to $hidden_dir_meta/${1}_$timestamp${NC}"
    fi
fi