#/bin/bash

sample_id=$1
fastq_path=$2

mkdir -p references/${sample_id}/ && cd references/${sample_id}/ && wget -q ${fastq_path}