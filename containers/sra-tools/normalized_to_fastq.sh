#/bin/bash

sample_id=$1
fastq_id=$2

fasterq-dump --log-level info --split-files --outdir ${sample_id}/ --temp ${sample_id}/tmp-${fastq_id} --force ./${sample_id}/${fastq_id} && rm -rf ${sample_id}/tmp-${fastq_id}/