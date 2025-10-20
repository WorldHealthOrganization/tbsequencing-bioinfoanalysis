#!/bin/sh
# Cleanup normalized FASTQ files using seqkit
# Usage: cleanup_normalized_fastq.sh <sampleId> <fastqId>
set -euo pipefail

SAMPLE_ID="$1"
FASTQ_ID="$2"

cd "$SAMPLE_ID"

# create a list of reads present in both files
seqkit common $SAMPLE_ID/${FASTQ_ID}_1.fastq.gz $SAMPLE_ID/${FASTQ_ID}_2.fastq.gz | seqkit seq -n -i > $SAMPLE_ID/common_reads.txt

# filter R1 and R2 using the common reads list, compress to new files
seqkit grep -f $SAMPLE_ID/common_reads.txt $SAMPLE_ID/${FASTQ_ID}_1.fastq > $SAMPLE_ID/${FASTQ_ID}_cleaned_1.fastq
seqkit grep -f $SAMPLE_ID/common_reads.txt $SAMPLE_ID/${FASTQ_ID}_2.fastq > $SAMPLE_ID/${FASTQ_ID}_cleaned_2.fastq

mv $SAMPLE_ID/${FASTQ_ID}_cleaned_1.fastq $SAMPLE_ID/${FASTQ_ID}_1.fastq
mv $SAMPLE_ID/${FASTQ_ID}_cleaned_2.fastq $SAMPLE_ID/${FASTQ_ID}_2.fastq

rm $SAMPLE_ID/common_reads.txt
