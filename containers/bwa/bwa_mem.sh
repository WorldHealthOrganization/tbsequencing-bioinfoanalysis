#/bin/bash

sample_id=$1
fastq_id=$2
reference_name=$3
r1suffix=$4
r2suffix=$5

mkdir -p ${sample_id} genotype locus-stats taxonomy-assignment global-stats deletion alignment && bwa mem -R '@RG\tID:1\tSM:WGS\tPL:ILLUMINA' -t 4 -o ${sample_id}/${fastq_id}.sam references/${reference_name}.fna ${sample_id}/${fastq_id}_${r1suffix} ${sample_id}/${fastq_id}_${r2suffix}