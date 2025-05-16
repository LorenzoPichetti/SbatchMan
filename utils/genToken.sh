#!/bin/bash

# === Configuration ===
PAIR_SEP="£"     # Separator between flag and its parameter
TOKEN_SEP="££"   # Separator between token entries

# === Data structures ===
declare -A flag_param   # map: flag => param (or "")
declare -a lone_params  # positional args without flags
declare -a flag_list    # list of flags for sorting

args=("$@")
i=0
while [[ $i -lt ${#args[@]} ]]; do
    arg="${args[$i]}"
    if [[ "$arg" == -* ]]; then
        flag="$arg"
        next="${args[$((i + 1))]}"
        if [[ -n "$next" && "$next" != -* ]]; then
            # Flag with parameter
            base=$(basename -- "$next")
            flag_param["$flag"]="$base"
            flag_list+=("$flag")
            ((i++))  # skip next as it's the param
        else
            # Flag with no param
            flag_param["$flag"]=""
            flag_list+=("$flag")
        fi
    else
        # Standalone positional parameter
        base=$(basename -- "$arg")
        lone_params+=("$base")
    fi
    ((i++))
done

# === Build the token ===
IFS=$'\n' sorted_flags=($(sort <<<"${flag_list[*]}"))
unset IFS

token=""
for flag in "${sorted_flags[@]}"; do
    val="${flag_param[$flag]}"
    if [[ -n "$val" ]]; then
        part="${flag}${PAIR_SEP}${val}"
    else
        part="${flag}"
    fi
    token+="${part}${TOKEN_SEP}"
done

for param in "${lone_params[@]}"; do
    token+="${param}${TOKEN_SEP}"
done

# Remove trailing separator
token="${token%${TOKEN_SEP}}"

echo "$token"
