FROM quay.io/biocontainers/bwa:0.7.17--he4a0461_11

ARG VERSION=0.7.17

LABEL container.base.image="quay.io/biocontainer/bwa:0.7.17"
LABEL software.name="bwa"
LABEL software.version=${VERSION}
LABEL software.description="Utilities for the Sequence Alignment/Map (SAM/BAM/CRAM) formats"
LABEL software.website="http://www.htslib.org"
LABEL software.documentation="http://www.htslib.org/doc/samtools.html"
LABEL software.license="MIT/Expat"
LABEL tags="Genomics"

ADD bwa_mem.sh /scripts/

RUN sed -i "s/\r//g" /scripts/bwa_mem.sh

WORKDIR /scratch