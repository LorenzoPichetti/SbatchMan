#!/bin/bash

if [ "$#" -lt 8 ]; then
    echo "Usage: $0 <partition_name> <slurm_account> <time in HH:MM:SS> <hostname> <exp-name> <ntasks> <ngpus> <binary>"
    exit 1
fi

my_partition=$1
my_hostname=$4
my_expname=$5
my_account=$2
my_ntasks=$6
my_binary=$8
my_ngpus=$7
my_time=$3

echo "my_partition: ${my_partition}"
echo " my_hostname: ${my_hostname}"
echo "  my_expname: ${my_expname}"
echo "  my_account: ${my_account}"
echo "   my_ntasks: ${my_ntasks}"
echo "   my_binary: ${my_binary}"
echo "    my_ngpus: ${my_ngpus}"
echo "     my_time: ${my_time}"

mkdir -p "sout"
mkdir -p "sout/${my_hostname}"
mkdir -p "sout/${my_hostname}/${my_expname}"

stencil_sbatch=$(cat << 'EOF'
#!/bin/bash

#SBATCH --job-name=<exp-name>
#SBATCH --output=sout/<hostname>/<exp-name>/<exp-name>_%j.out
#SBATCH --error=sout/<hostname>/<exp-name>/<exp-name>_%j.err

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
    acct=$(sacct -o JobID,JobName,Partition,State,Start,Elapsed,ExitCode | grep "${SLURM_JOB_ID})
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

sbatch=$(echo "${stencil_sbatch}" | sed "s/<hostname>/${my_hostname}/g" | sed "s/<account>/${my_account}/g" | sed "s/<partition>/${my_partition}/g" | sed "s/<time>/${my_time}/g" | sed "s/<exp-name>/${my_expname}/g" | sed "s%<binary>%${my_binary}%g" | sed "s/<ntasks>/${my_ntasks}/g" | sed "s/<ngpus>/${my_ngpus}/g")


sbatch_name="${my_expname}_sbatch.sh"
echo "${sbatch}" > ${sbatch_name}
chmod +x "${sbatch_name}"

echo "Generated ${sbatch_name}"
