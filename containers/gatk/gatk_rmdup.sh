#/bin/bash

sample_id=$1
fastq_id=$2

gatk MarkDuplicates --REMOVE_DUPLICATES true --INPUT ${sample_id}/${fastq_id}.bam --OUTPUT ${sample_id}/${fastq_id}_rmdup.bam --METRICS_FILE ${sample_id}/${fastq_id}_rmdupmetrics.txt && mv ${sample_id}/${fastq_id}_rmdup.bam ${sample_id}/${fastq_id}.bam && gatk BuildBamIndex --INPUT ${sample_id}/${fastq_id}.bam --OUTPUT ${sample_id}/${fastq_id}.bam.bai