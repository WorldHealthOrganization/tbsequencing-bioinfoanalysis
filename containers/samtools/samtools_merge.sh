#/bin/bash

sample_id=$1

samtools merge ${sample_id}/${sample_id}.bam ${sample_id}/*.bam && samtools index ${sample_id}/${sample_id}.bam