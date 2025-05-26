#!/bin/bash

if [[ -z $SbM_UTILS ]]; then
    echo -e "${RED} Please setup SbatchMan environment and source 'sourceFile.sh'${NC}"
    exit 1
fi

queue=$(${SbM_UTILS}/inQueue.sh)
go=1

if [[ ! -z $queue ]]; then
    echo -e "-- QUEUE --\n${queue}"
    echo -e "${PUR}Queue is not empty. Do you want to proceed anyway? (y/N)${NC}"
    read -r response
    if [[ "$response" != "y" && "$response" != "Y" ]]; then
        exit
    fi
fi

running=$(${SbM_UTILS}/whoRuns.sh)

if [[ ! -z $running ]]; then
    echo -e "-- RUNNING --\n${running}"
    echo -e "${PUR}Jobs are still running. Do you want to proceed anyway? (y/N)${NC}"
    read -r response
    if [[ "$response" != "y" && "$response" != "Y" ]]; then
        exit
    fi
fi

echo -e "${GRE}Parsing results...${NC}"
python3 gather_results_from_sout.py