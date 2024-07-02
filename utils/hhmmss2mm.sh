#!/bin/bash

# Input time
time="$1"

# Parse hours, minutes, and seconds
IFS=':' read -r hours minutes seconds <<< "$time"

#echo "debug: $hours $minutes $seconds"

# Convert hours and seconds to minutes
if [[ "$seconds" -gt "0" ]]
then
	seconds="60"
fi

total_minutes=$((hours * 60 + minutes + seconds / 60))

# Print the result
echo "$total_minutes"
