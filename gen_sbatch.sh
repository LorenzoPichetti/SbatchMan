#!/bin/bash

if [ "$#" -lt 7 ]; then
    echo "Usage: $0 <partition_name> <slurm_account> <time in HH:MM:SS> <exp-name> <ntasks> <ngpus> <binary>"
    exit 1
fi

my_partition=$1
my_expname=$4
my_account=$2
my_ntasks=$5
my_binary=$7
my_ngpus=$6
my_time=$3

my_hostname=$( ${SbM_UTILS}/hostname.sh )

mkdir -p "${SbM_METADATA_HOME}/${my_hostname}"
exptable="${SbM_METADATA_HOME}/${my_hostname}/expTable.csv"
if ! [[ -f "${exptable}" ]]
then
        echo "# ExpName BinaryName SbatchName" > "${exptable}"
fi

# if grep ${my_binary} in ${exptable} write and abort
if grep -q "${my_binary}" "${exptable}"
then
	echo "WARNING: ${my_binary} is already contained in ${exptable} with a diferent name:"
	grep "${my_binary}" "${exptable}"
	echo "  If you want to change the experiment name, please, remove it manually form ${exptable} and menage manually the old metadata"
	# exit 1 need to check that the name is not the same to abort
fi

echo "my_partition: ${my_partition}"
echo " my_hostname: ${my_hostname}"
echo "  my_expname: ${my_expname}"
echo "  my_account: ${my_account}"
echo "   my_ntasks: ${my_ntasks}"
echo "   my_binary: ${my_binary}"
echo "    my_ngpus: ${my_ngpus}"
echo "     my_time: ${my_time}"

mkdir -p "${SbM_SOUT}"
mkdir -p "${SbM_SOUT}/${my_hostname}"
mkdir -p "${SbM_SOUT}/${my_hostname}/${my_expname}"

stencil_sbatch=$(cat << 'EOF'
#!/bin/bash

#SBATCH --job-name=<exp-name>
#SBATCH --output=<sout_path>/<hostname>/<exp-name>/<exp-name>_%j.out
#SBATCH --error=<sout_path>/<hostname>/<exp-name>/<exp-name>_%j.err

#SBATCH --partition=<partition>
#SBATCH --account=<account>
#SBATCH --time=<time>

#SBATCH --nodes=1
#SBATCH --gres=gpu:<ngpus>
#SBATCH --tasks=<ntasks>
#SBATCH --cpus-per-task=1

my_metadata_path=$1
my_token=$2

i=0
arguments=()
for a in $@
do
        if [[ "$i" -gt "1" ]]
        then
                arguments+=( $a )
        fi
        i=$(( $i +1 ))
done

echo " ------------ <exp-name> ------------ "
echo "         my_token: $my_token"
echo " my_metadata_path: $my_metadata_path"
echo "        arguments: ${arguments[*]}"

echo "${my_token}" >> "${my_metadata_path}/submitted.txt"

echo "srun <binary> ${arguments[*]}"
srun <binary> ${arguments[*]}

if [[ $? == 0 ]]
then
    echo "${my_token}" >> "${my_metadata_path}/finished.txt"
    acct_head=$(sacct -o JobID,JobName,Partition,State,Start,Elapsed,ExitCode | head -2)
    acct=$(sacct -o JobID,JobName,Partition,State,Start,Elapsed,ExitCode | grep "${SLURM_JOB_ID}")
    echo "${acct_head}" >> "${my_metadata_path}/finished_sacct.txt"
    echo "${acct}"      >> "${my_metadata_path}/finished_sacct.txt"
    echo "SLURM_JOB_ID: ${SLURM_JOB_ID}"
    echo "${acct_head}"
    echo "${acct}"
else
    echo "${my_token} not written in '${my_metadata_path}/finished.txt' since the exit code is different form 0 ($?)"
fi


echo "------------------------"

EOF
)

sbatch=$(echo "${stencil_sbatch}" | sed "s/<hostname>/${my_hostname}/g" | sed "s/<account>/${my_account}/g" | sed "s/<partition>/${my_partition}/g" | sed "s/<time>/${my_time}/g" | sed "s/<exp-name>/${my_expname}/g" | sed "s%<binary>%${my_binary}%g" | sed "s/<ntasks>/${my_ntasks}/g" | sed "s/<ngpus>/${my_ngpus}/g" | sed "s%<sout_path>%${SbM_SOUT}%g")


sbatch_name="${SbM_SBATCH}/${my_expname}_sbatch.sh"
echo "${sbatch}" > ${sbatch_name}
chmod +x "${sbatch_name}"

echo "${my_expname} ${my_binary} ${sbatch_name}" >> "${exptable}"

echo "Generated ${sbatch_name}"
