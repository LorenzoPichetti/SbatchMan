#!/bin/bash

RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${RED}Do you really want to permanently delete 'metadata/* sbatchscripts/* sout/* sourceFile.sh'? (y/N)${NC}"
read -r response
if [[ "$response" == "y" || "$response" == "Y" ]]; then
    my_hostname=$( ${SbM_UTILS}/hostname.sh )
    rm -r "$SbM_METADATA_HOME/$my_hostname" "$SbM_SBATCH" "$SbM_SOUT/$my_hostname" "$SbM_HOME/sourceFile.sh"
    echo "Please run: 'unset -v SbM_METADATA_HOME SbM_SBATCH SbM_SOUT SbM_HOME SbM_EXPTABLE SbM_UTILS'"
    echo "Environment cleared!"
fi
