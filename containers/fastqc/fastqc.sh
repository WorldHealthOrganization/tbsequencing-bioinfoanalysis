#/bin/bash

sample_id=$1
fastq_id=$2
r1suffix=$3
r2suffix=$4

mkdir -p ${sample_id} && fastqc --outdir ${sample_id} ${sample_id}/${fastq_id}_${r1suffix} ${sample_id}/${fastq_id}_${r2suffix}