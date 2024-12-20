FROM quay.io/biocontainers/bcftools:1.17--h3cc50cf_1

ARG VERSION=1.9

# Metadata
LABEL container.base.image="ubuntu:18.04"
LABEL software.name="BCFtools"
LABEL software.version=${VERSION}
LABEL software.description="Utilities for variant calling and manipulating files in the Variant Call Format (VCF) and its binary counterpart BCF"
LABEL software.website="http://www.htslib.org"
LABEL software.documentation="http://www.htslib.org/doc/bcftools.html"
LABEL software.license="MIT/Expat"
LABEL tags="Genomics"

ADD extract_deletions.sh /scripts/
ADD bcftools_mpileup.sh /scripts/
ADD format_all_genotypes.sh /scripts/

RUN sed -i "s/\r//g" /scripts/extract_deletions.sh
RUN sed -i "s/\r//g" /scripts/bcftools_mpileup.sh
RUN sed -i "s/\r//g" /scripts/format_all_genotypes.sh

WORKDIR /scratch