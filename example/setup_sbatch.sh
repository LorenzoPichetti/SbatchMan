#!/bin/bash

preprocess_block1=$(cat << 'EOF'

# --------------------- preprocess1 ---------------------
echo "--------------------- preprocess1 ---------------------"
echo "This is my preprocess:"

myhostnamefrompreprocess=$( hostname )
echo -e "\thostname from preprocess: ${myhostnamefrompreprocess}"

echo "other stuff..."
echo "------------------------------------------------------"
# ------------------------------------------------------


EOF
)

preprocess_block2=$(cat << 'EOF'

# --------------------- preprocess ---------------------
echo "--------------------- preprocess ---------------------"
echo "This is a different preprocess"
# ------------------------------------------------------

EOF
)

# These are some sample values that work on Baldo cluster
PARTITION=cpu
BIN_PATH=bin

# 1 node, 1 CPU, 0 GPU, no MPI
../newExperiment.sh -p $PARTITION -t 00:05:00 -e Int -n 1 -c 1 -g 0 -b "$BIN_PATH/testInt" -d 1 -P "${preprocess_block1}"
../newExperiment.sh -p $PARTITION -t 00:05:00 -e Float -n 1 -c 1 -g 0 -b "$BIN_PATH/testFloat" -d 1 -P "${preprocess_block2}"
../newExperiment.sh -p $PARTITION -t 00:05:00 -e Double1 -n 1 -c 1 -g 0 -b "$BIN_PATH/testDouble" -d 1

# 1 node, 2 CPUs, 0 GPU, no MPI
../newExperiment.sh -p $PARTITION -t 00:05:00 -e Double2 -n 1 -c 1 -g 0 -b "$BIN_PATH/testDouble" -d 2

# 1 node, 1 CPU, 5 seconds of walltime
../newExperiment.sh -p $PARTITION -t 00:00:05 -e IntWTime -n 1 -c 1 -g 0 -b "$BIN_PATH/testInt" -d 1