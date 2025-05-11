#!/bin/bash

source ../submit.sh

bins=( "bin/testInt" "bin/testFloat" "bin/testDouble" )

for ii in ${!bins[@]}
do
	for seed in $( seq 0 1 ) # seed in { 0, 1 }
	do
		for scale in $( seq 20 21 ) # scale in { 2^20, 2^21 }
		do
			bin=${bins[$ii]}
			echo "----- $bin Scale: ${scale} Seed: ${seed} -----"
			if [[ $bin == "bin/testDouble" ]]
			then
				export OMP_NUM_THREADS=1
				SbM_submit_function --verbose --expname Double1 --binary $bin -n ${scale} -r ${seed}
				echo "jobid: ${job_id}"
				export OMP_NUM_THREADS=2
				SbM_submit_function --verbose --expname Double2 --binary $bin -n ${scale} -r ${seed} -a
				echo "jobid: ${job_id}"
			elif [[ $bin == "bin/testInt" ]]
			then
				export OMP_NUM_THREADS=1
				SbM_submit_function --verbose --expname Int --binary $bin -n ${scale} -r ${seed}
			else
				export OMP_NUM_THREADS=1
				echo "SbM_submit_function --binary $bin -n ${scale} -r ${seed}"
				SbM_submit_function --verbose --binary $bin -n ${scale} -r ${seed}
				echo "jobid: ${job_id}"
			fi
			echo "---------------------------------"
		done
	done
done

export OMP_NUM_THREADS=1

# These will result in a runtime error
SbM_submit_function --verbose --expname Double1 --binary bin/testDouble -n 24 -r 1 -e
SbM_submit_function --verbose --expname IntWTime --binary bin/testInt -n 24 -r 1 -e

# These will result in a Timeout
SbM_submit_function --verbose --expname IntWTime --binary bin/testInt -n 40 -r 0
SbM_submit_function --verbose --expname IntWTime --binary bin/testInt -n 40 -r 1