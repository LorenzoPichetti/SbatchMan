#!/bin/bash

source ../submit.sh

bins=( "bin/testInt" "bin/testFloat" "bin/testDouble" )

for ii in ${!bins[@]}
do
	for scale in $( seq 20 21 ) # Scale = { 2^20, 2^21 }
	do
		for seed in $( seq 0 1 ) # Seed = { 0, 1 }
		do
            bin=${bins[$ii]}
			echo "----- $bin Scale: ${scale} Seed: ${seed} -----"
			if [[ $bin != "bin/testDouble" ]]
			then
				export OMP_NUM_THREADS=1
				echo "SbM_submit_function --binary $bin -n ${scale} -r ${seed}"
				SbM_submit_function --verbose --binary $bin -n ${scale} -r ${seed}
				echo "jobid: ${job_id}"
			else
				export OMP_NUM_THREADS=1
				SbM_submit_function --verbose --expname Double1 --binary $bin -n ${scale} -r ${seed}
				echo "jobid: ${job_id}"
				export OMP_NUM_THREADS=2
				SbM_submit_function --verbose --expname Double2 --binary $bin -n ${scale} -r ${seed} -a
				echo "jobid: ${job_id}"
			fi
			echo "---------------------------------"
		done
	done
done