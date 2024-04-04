#!/bin/bash

my_hostname=$( hostname )
if [[ ${my_hostname} == *.* ]]
then
        my_hostname=$( hostname -d | cut -d'.' -f1 )
else
        my_hostname=$( hostname )
fi
echo "${my_hostname}"
