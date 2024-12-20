#/bin/bash

sample_id=$1
fastq_id=$2

samtools sort -@ 4 -o ${sample_id}/${fastq_id}.bam ${sample_id}/${fastq_id}.sam && samtools index ${sample_id}/${fastq_id}.bam