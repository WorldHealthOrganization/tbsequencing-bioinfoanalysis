FROM quay.io/biocontainers/seqkit:0.7.1--0

ARG VERSION=0.7.1--0

LABEL container.base.image="quay.io/biocontainers/seqkit:0.7.1--0"
LABEL software.name="seqkit"
LABEL software.version=${VERSION}
LABEL software.description="seqkit is a toolkit for FASTA/Q file manipulation"
LABEL tags="Genomics"


ADD cleanup_normalized_fastq.sh /scripts/

WORKDIR /scratch
