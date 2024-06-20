#!/bin/bash

for item in $(hdfs dfs -ls /data/*_analysis/void/par* |  grep  "\-00000$")
do
  if [[ $item == *"part-00000" ]]; then
    dest=$(echo "$item" | sed "s/\//__/g")
    echo "copying $item from HDFS to $dest"
    hdfs dfs -get "$item" "$dest"
  fi
done
