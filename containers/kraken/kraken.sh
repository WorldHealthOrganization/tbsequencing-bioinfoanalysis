#/bin/bash

sample_id=$1
kraken_db_name=$2

kraken2 --db references/${kraken_db_name} --threads 1 --report ${sample_id}/${sample_id}-kraken.txt --paired ${sample_id}/*.f*q* > ${sample_id}/${sample_id}-kraken-full.txt