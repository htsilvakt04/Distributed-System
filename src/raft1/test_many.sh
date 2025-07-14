#!/bin/bash

# Check if at least 2 arguments are passed
if [ "$#" -lt 2 ]; then
  echo "Usage: $0 [count] [command...]"
  exit 1
fi

count=$1
shift

# Remaining arguments are the command
cmd="$@"

for ((i=1; i<=count; i++)); do
  echo "Run #$i: $cmd"
  $cmd
  status=$?
  echo "Exit code: $status"
  echo
done