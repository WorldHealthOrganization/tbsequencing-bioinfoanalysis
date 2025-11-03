FROM quay.io/biocontainers/seqkit:2.10.1--he881be0_0

ARG VERSION=2.10.1--he881be0_0

LABEL container.base.image="quay.io/biocontainers/seqkit:2.10.1--he881be0_0"
LABEL software.name="seqkit"
LABEL software.version=${VERSION}
LABEL software.description="seqkit is a toolkit for FASTA/Q file manipulation"
LABEL tags="Genomics"


ADD cleanup_normalized_fastq.sh /scripts/

WORKDIR /scratch
