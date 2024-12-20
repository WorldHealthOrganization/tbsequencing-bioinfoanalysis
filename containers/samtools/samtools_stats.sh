#/bin/bash

sample_id=$1

samtools stats --remove-dups ${sample_id}/${sample_id}.bam > ${sample_id}/${sample_id}-samtools-stats.txt