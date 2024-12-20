FROM quay.io/biocontainers/samtools:1.17--hd87286a_1

ARG VERSION=1.9

# Metadata
LABEL software.name="SAMtools"
LABEL software.version=${VERSION}
LABEL software.description="Utilities for the Sequence Alignment/Map (SAM/BAM/CRAM) formats"
LABEL software.website="http://www.htslib.org"
LABEL software.documentation="http://www.htslib.org/doc/samtools.html"
LABEL software.license="MIT/Expat"
LABEL tags="Genomics"

ADD clean_all.sh /scripts/
ADD cramconvert.sh /scripts/
ADD samtools_merge.sh /scripts/
ADD samtools_sort.sh /scripts/
ADD samtools_stats.sh /scripts/

RUN sed -i "s/\r//g" /scripts/clean_all.sh
RUN sed -i "s/\r//g" /scripts/cramconvert.sh
RUN sed -i "s/\r//g" /scripts/samtools_merge.sh
RUN sed -i "s/\r//g" /scripts/samtools_sort.sh
RUN sed -i "s/\r//g" /scripts/samtools_stats.sh

WORKDIR /scratch
