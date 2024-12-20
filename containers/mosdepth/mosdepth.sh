#/bin/bash

sample_id=$1
sample_type=$2
refseq_assembly_accession=$3

cd ${sample_id} && mosdepth --thresholds 10,15,20,30 --by ../references/${refseq_assembly_accession}_${sample_type}.bed ${sample_id} ${sample_id}.bam