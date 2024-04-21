#!/bin/bash

# !! insert a check that it is launched form SbM home !!

SbM_HOME="$(pwd)"
SbM_SOUT="${SbM_HOME}/sout"
SbM_UTILS="${SbM_HOME}/utils"
SbM_METADATA="${SbM_HOME}/metadata"
SbM_SBATCH="${SbM_HOME}/sbatchscripts"

sourcename="${SbM_HOME}/sourceFile.sh"
my_hostname=$( ${SbM_UTILS}/hostname.sh )

mkdir -p ${SbM_HOME} ${SbM_SOUT} ${SbM_META} ${SbM_SBATCH}
mkdir -p "${SbM_METADATA}/${my_hostname}"
SbM_EXPTABLE="${SbM_METADATA}/${my_hostname}/expTable.csv"

echo "# Please, source this file each time you open a new session"     >  ${sourcename}
echo "SbM_HOME=${SbM_HOME} ; export SbM_HOME"                          >> ${sourcename}
echo "SbM_SOUT=${SbM_SOUT} ; export SbM_SOUT"                          >> ${sourcename}
echo "SbM_UTILS=${SbM_UTILS} ; export SbM_UTILS"                       >> ${sourcename}
echo "SbM_SBATCH=${SbM_SBATCH} ; export SbM_SBATCH"                    >> ${sourcename}
echo "SbM_EXPTABLE=${SbM_EXPTABLE} ; export SbM_EXPTABLE"              >> ${sourcename}
echo "SbM_METADATA_HOME=${SbM_METADATA} ; export SbM_METADATA_HOME"    >> ${sourcename}

echo "# Please, source this file each time you open a new session"
echo "SbM_HOME=${SbM_HOME}"                  
echo "SbM_SOUT=${SbM_SOUT}"                  
echo "SbM_UTILS=${SbM_UTILS}"
echo "SbM_SBATCH=${SbM_SBATCH}"           
echo "SbM_EXPTABLE=${SbM_EXPTABLE}"
echo "SbM_METADATA_HOME=${SbM_METADATA}"
