#!/bin/sh
# Cleanup normalized FASTQ files using seqkit
# Usage: cleanup_normalized_fastq.sh <sampleId> <fastqId>
set -euo pipefail

SAMPLE_ID="$1"
FASTQ_ID="$2"

# create a list of reads present in both files
seqkit pair -1 $SAMPLE_ID/${FASTQ_ID}_1.fastq -2 $SAMPLE_ID/${FASTQ_ID}_2.fastq 

# replace original files with paired reads only
mv $SAMPLE_ID/${FASTQ_ID}_1.paired.fastq $SAMPLE_ID/${FASTQ_ID}_1.fastq
mv $SAMPLE_ID/${FASTQ_ID}_2.paired.fastq $SAMPLE_ID/${FASTQ_ID}_2.fastq
