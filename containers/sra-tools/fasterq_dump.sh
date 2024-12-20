#/bin/bash

sample_id=$1
fastq_id=$2

rm -rf tmp-${fastq_id}/ && vdb-config --report-cloud-identity yes && prefetch --output-directory tmp-${fastq_id} ${fastq_id} && fasterq-dump --log-level info --split-files --outdir ${sample_id}/ --temp tmp-${fastq_id} --force tmp-${fastq_id}/${fastq_id} && rm -rf tmp-${fastq_id}/