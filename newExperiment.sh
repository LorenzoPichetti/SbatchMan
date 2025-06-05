#!/bin/bash

print_usage() {
    echo "Usage: $0 -p <partition_name> -a <slurm_account> -t <time in HH:MM:SS> -e <exp-name> -c <ntasks> -g <ngpus> -b <binary>"
    
	echo -e '\nMandatory arguments:'
	echo -e '\t-t <time in HH:MM:SS>:\tspecify the SLURM max time (HH:MM:SS)'
	echo -e '\t-e <exp-name>:\t\tspecify the experiment name'
	echo -e '\t-b <binary>:\t\tspecify the binary path'
	echo -e '\t-n <nnodes>:\t\tspecify the number of required SLURM nodes'
	echo -e '\t-c <ntasks>:\t\tspecify the number of required SLURM tasks'

	echo -e '\nOptional arguments:'
	echo -e '\t-g <ngpus>:\t\tspecify the number of required gpus'
	echo -e '\t-p <partition_name>:\tspecify the SLURM partition name'
	echo -e '\t-a <slurm_account>:\tspecify the SLURM account'
	echo -e '\t-S <qos>:\t\tspecify a non-standard ServiceLevel (i.e. export NCCL_IB_SL)'
	echo -e '\t-M <MPI-version>:\tspecify the slurm MPI version (--mpi=)'
	echo -e '\t-d <cpus-per-task>:\tspecify the number of cpu per task'
	echo -e '\t-s <constraint>:\tspecify the slurm constraint'
	echo -e '\t-m <memory>:\t\tspecify the alloc memory'
	echo -e '\t-q <qos>:\t\tspecify the slurm qos'
	echo -e '\t-r <reservation>:\tspecify the slurm reservation'
	echo -e '\t-w <nodelist>:\t\tspecify a specific slurm nodelist'
	echo -e '\t-P <preprocessblock>:\tspecify a bash preprocess block of code'
}

RED='\033[0;31m'
PUR='\033[0;35m'
GRE='\033[0;32m'
NC='\033[0m' # No Color

unset -v my_preprocessblock
unset -v my_constraint
unset -v my_partition
unset -v my_expname
unset -v my_nodelist
unset -v my_account
unset -v my_ntasks
unset -v my_binary
unset -v my_ngpus
unset -v my_time
unset -v my_MPI
unset -v my_qos
unset -v my_mem
unset -v my_cpt
unset -v my_sl
unset -v my_reservation

while getopts 'P:s:p:e:a:n:c:b:g:t:q:m:d:M:S:w:' flag; do
  case "${flag}" in
	P) my_preprocessblock="${OPTARG}" ;;
	s) my_constraint="${OPTARG}" ;;
	p) my_partition="${OPTARG}" ;;
	e) my_expname="${OPTARG}" ;;
	a) my_account="${OPTARG}" ;;
	n) my_nnodes="${OPTARG}" ;;
	w) my_nodelist="${OPTARG}" ;;
	c) my_ntasks="${OPTARG}" ;;
	b) my_binary="${OPTARG}" ;;
	g) my_ngpus="${OPTARG}" ;;
	t) my_time="${OPTARG}" ;;
	M) my_MPI="${OPTARG}" ;;
	q) my_qos="${OPTARG}" ;;
	m) my_mem="${OPTARG}" ;;
	d) my_cpt="${OPTARG}" ;;
	S) my_sl="${OPTARG}" ;;
	r) my_reservation="${OPTARG}" ;;
	*) print_usage
     	     exit 1 ;;
  esac
done

# if [ -z "$my_partition" ]
# then
#         echo 'You must specify a partition with -p' >&2
# 		print_usage
#         exit 1
# fi

if [ -z "$my_expname" ]
then
        echo 'You must specify the expname with -e' >&2
		print_usage
        exit 1
fi

# if [ -z "$my_account" ]
# then
#         echo 'You must specify a slurm account with -a' >&2
# 		print_usage
#         exit 1
# fi

if [ -z "$my_nnodes" ]
then
        echo 'You must specify number of slurm nodes with -n' >&2
                print_usage
        exit 1
fi

if [ -z "$my_ntasks" ]
then
        echo 'You must specify number of slurm tasks with -c' >&2
		print_usage
        exit 1
fi

if [ -z "$my_binary" ]
then
        echo 'You must specify the binary file with -b' >&2
		print_usage
        exit 1
fi

# if [ -z "$my_ngpus" ]
# then
#         echo 'You must specify the number of gpus with -g' >&2
# 		print_usage
#         exit 1
# fi

if [ -z "$my_time" ]
then
        echo 'You must specify the max time with -t HH:MM:SS' >&2
		print_usage
        exit 1
fi

my_hostname=$( ${SbM_UTILS}/hostname.sh )
mkdir -p "${SbM_METADATA_HOME}/${my_hostname}"

exptable="${SbM_EXPTABLE}"
if ! [[ -f "${exptable}" ]]
then
        echo "# ExpName BinaryName SbatchName Account Partition nNodes nTasks nGpus Time" > "${exptable}"
fi

if grep -q "${my_expname}" "${exptable}"
then
	echo -e "${RED}ERROR${NC}: ${my_expname} is already contained in ${exptable}:"
	grep "${my_expname}" "${exptable}"
	echo "  If you want to change the experiment parameters, please, remove it manually form ${exptable} and menage manually the old metadata; if you want to define a different experiment, please, choose a different name"
	exit 1
fi

# if grep ${my_binary} in ${exptable} write and abort
if grep -q "${my_binary}" "${exptable}"
then
	echo -e "${PUR}WARNING${NC}: ${my_binary} is already contained in ${exptable} with a diferent name:"
	grep "${my_binary}" "${exptable}"
	echo "  If you want to change the experiment name, please, remove it manually form ${exptable} and menage manually the old metadata"
	# exit 1 # need to check that all the arguments are the same to abort
fi

echo "my_partition: ${my_partition}"
echo "my_reservation: ${my_reservation}"
echo " my_hostname: ${my_hostname}"
echo "  my_expname: ${my_expname}"
echo "  my_account: ${my_account}"
echo "   my_nnodes: ${my_nnodes}"
echo "   my_ntasks: ${my_ntasks}"
echo "   my_binary: ${my_binary}"
echo "    my_ngpus: ${my_ngpus}"
echo "     my_time: ${my_time}"
echo "       my_SL: ${my_sl}"

mkdir -p "${SbM_SOUT}"
mkdir -p "${SbM_SOUT}/${my_hostname}"
mkdir -p "${SbM_SOUT}/${my_hostname}/${my_expname}"

stencil_sbatch_head=$(cat << 'EOF'
#!/bin/bash

#SBATCH --job-name=<exp-name>
#SBATCH --output=<sout_path>/<hostname>/<exp-name>/<exp-name>_%j.out
#SBATCH --error=<sout_path>/<hostname>/<exp-name>/<exp-name>_%j.err

<partition>
#SBATCH --time=<time>
<account>
<qos>
<reservation>

#SBATCH --nodes=<nnodes>
#SBATCH --tasks=<ntasks>
#SBATCH --cpus-per-task=<cpus-per-task>
<ngpus>
<constraint>
<memory>
<nodelist>

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

EOF
)


# ---- <preprocess_block> ----

stencil_sbatch_tail=$(cat << 'EOF'
<NCCL_SL>
export cmd="<binary> ${arguments[*]}"
echo "srun <Slurm_MPI> ${cmd}"
srun <Slurm_MPI> ${cmd}

if [[ $? == 0 ]]
then
    echo "${my_token} ${SLURM_JOB_ID}" >> "${my_metadata_path}/finished.txt"
    #acct_head=$(sacct -o JobID,JobName,Partition,State,Start,Elapsed,ExitCode | head -2)
    #acct=$(sacct -o JobID,JobName,Partition,State,Start,Elapsed,ExitCode | grep "${SLURM_JOB_ID}")
    #echo "${acct_head}" >> "${my_metadata_path}/finished_sacct.txt"
    #echo "${acct}"      >> "${my_metadata_path}/finished_sacct.txt"
    #echo "${acct_head}"
    #echo "${acct}"
else
    echo "------------------------ ERROR ------------------------"
    echo "${my_token} not written in '${my_metadata_path}/finished.txt' since the exit code is different form 0 ($?)"
    echo "${my_token} ${SLURM_JOB_ID}" >> "${my_metadata_path}/notFinished.txt"
fi


echo "------------------------"
echo "SLURM_JOB_ID: ${SLURM_JOB_ID}"
echo "SLURMD_NODENAME: ${SLURMD_NODENAME}"
echo "SLURM_JOB_PARTITION: ${SLURM_JOB_PARTITION}"
echo "SLURM_JOB_START_TIME: ${SLURM_JOB_START_TIME}"
echo "SLURM_JOB_END_TIME: ${SLURM_JOB_END_TIME}"
echo "------------------------"

EOF
)

tmpfile="mytmpfile4stencilsbatch.txt"
echo "${stencil_sbatch_head}" > ${tmpfile}

if [ ! -z "$my_preprocessblock" ]
then
        echo "${my_preprocessblock}" >> ${tmpfile}
fi

echo "${stencil_sbatch_tail}" >> ${tmpfile}
stencil_sbatch=$( cat ${tmpfile} )
rm ${tmpfile}

# ----------- DEBUG -----------
#echo "stencil_sbatch:"
#echo " -----------------------------"
#echo "${stencil_sbatch}"
#exit 1
# -----------------------------

# sbatch=$(echo "${stencil_sbatch}" | sed "s/<hostname>/${my_hostname}/g" | sed "s/<account>/${my_account}/g" | sed "s/<partition>/${my_partition}/g" | sed "s/<time>/${my_time}/g" | sed "s/<exp-name>/${my_expname}/g" | sed "s%<binary>%${my_binary}%g" | sed "s/<nnodes>/${my_nnodes}/g" | sed "s/<ntasks>/${my_ntasks}/g" | sed "s/<ngpus>/${my_ngpus}/g" | sed "s%<sout_path>%${SbM_SOUT}%g")
sbatch=$(echo "${stencil_sbatch}" | sed "s/<hostname>/${my_hostname}/g" | sed "s/<time>/${my_time}/g" | sed "s/<exp-name>/${my_expname}/g" | sed "s%<binary>%${my_binary}%g" | sed "s/<nnodes>/${my_nnodes}/g" | sed "s/<ntasks>/${my_ntasks}/g" | sed "s%<sout_path>%${SbM_SOUT}%g")

if [ -z "$my_reservation" ]
then
        tmp=$(echo "${sbatch}" | sed "s/<reservation>//g" )
        sbatch=$( echo "${tmp}" )
else
        tmp=$(echo "${sbatch}" | sed "s/<reservation>/#SBATCH --reservation=${my_reservation}/g" )
        sbatch=$( echo "${tmp}" )
fi

if [ -z "$my_account" ]
then
        tmp=$(echo "${sbatch}" | sed "s/<account>//g" )
        sbatch=$( echo "${tmp}" )
else
        tmp=$(echo "${sbatch}" | sed "s/<account>/#SBATCH --account=${my_account}/g" )
        sbatch=$( echo "${tmp}" )
fi

if [ -z "$my_partition" ]
then
        tmp=$(echo "${sbatch}" | sed "s/<partition>//g" )
        sbatch=$( echo "${tmp}" )
else
        tmp=$(echo "${sbatch}" | sed "s/<partition>/#SBATCH --partition=${my_partition}/g" )
        sbatch=$( echo "${tmp}" )
fi

if [ -z "$my_constraint" ]
then
        tmp=$(echo "${sbatch}" | sed "s/<constraint>//g" )
        sbatch=$( echo "${tmp}" )
else
        tmp=$(echo "${sbatch}" | sed "s/<constraint>/#SBATCH --constraint=${my_constraint}/g" )
        sbatch=$( echo "${tmp}" )
fi

if [ -z "$my_MPI" ]
then
	tmp=$(echo "${sbatch}" | sed "s/<Slurm_MPI>//g" )
	sbatch=$( echo "${tmp}" )
else
	tmp=$(echo "${sbatch}" | sed "s/<Slurm_MPI>/--mpi=${my_MPI}/g" )
	sbatch=$( echo "${tmp}" )
fi

if [ -z "$my_qos" ]
then
	tmp=$(echo "${sbatch}" | sed "s/<qos>//g" )
	sbatch=$( echo "${tmp}" )
else
	tmp=$(echo "${sbatch}" | sed "s/<qos>/#SBATCH --qos=${my_qos}/g" )
	sbatch=$( echo "${tmp}" )
fi

if [ -z "$my_ngpus" ]
then
	tmp=$(echo "${sbatch}" | sed "s/<ngpus>//g" )
	sbatch=$( echo "${tmp}" )
else
	tmp=$(echo "${sbatch}" | sed "s/<ngpus>/#SBATCH --gres=gpu:${my_ngpus}/g" )
	sbatch=$( echo "${tmp}" )
fi

if [ -z "$my_mem" ]
then
	tmp=$(echo "${sbatch}" | sed "s/<memory>//g" )
	sbatch=$( echo "${tmp}" )
else
	tmp=$(echo "${sbatch}" | sed "s/<memory>/#SBATCH --mem=${my_mem}/g" )
	sbatch=$( echo "${tmp}" )
fi

if [ -z "$my_cpt" ]
then
	tmp=$(echo "${sbatch}" | sed "s/<cpus-per-task>/1/g" )
	sbatch=$( echo "${tmp}" )
else
	tmp=$(echo "${sbatch}" | sed "s/<cpus-per-task>/${my_cpt}/g" )
	sbatch=$( echo "${tmp}" )
fi

if [ -z "$my_nodelist" ]
then
	tmp=$(echo "${sbatch}" | sed "s/<nodelist>//g" )
	sbatch=$( echo "${tmp}" )
else
	tmp=$(echo "${sbatch}" | sed "s/<nodelist>/#SBATCH --nodelist=${my_nodelist}/g" )
	sbatch=$( echo "${tmp}" )
fi

if [ -z "$my_sl" ]
then
	tmp=$(echo "${sbatch}" | sed "s/<NCCL_SL>//g" )
	sbatch=$( echo "${tmp}" )
else
	tmp=$(echo "${sbatch}" | sed "s/<NCCL_SL>/export NCCL_IB_SL=${my_sl}/g" )
	sbatch=$( echo "${tmp}" )
fi

sbatch_name="${SbM_SBATCH}/${my_expname}_sbatch.sh"
echo "${sbatch}" > ${sbatch_name}
chmod +x "${sbatch_name}"

echo "${my_expname} ${my_binary} ${sbatch_name} ${my_account} ${my_partition} ${my_nnodes} ${my_ntasks} ${my_ngpus} ${my_time}" >> "${exptable}"

echo -e "${GRE}Generated${NC} ${sbatch_name}"
