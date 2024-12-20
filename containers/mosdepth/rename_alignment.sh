#/bin/bash

sample_id=$1
fastq_id=$2

mv ${sample_id}/${fastq_id}.bam ${sample_id}/${sample_id}.bam && mv ${sample_id}/${fastq_id}.bam.bai ${sample_id}/${sample_id}.bam.bai