FROM quay.io/biocontainers/kraken2:2.1.2--pl5321h9f5acd7_3

ARG VERSION=2.1.2--pl5321h9f5acd7_3

LABEL container.base.image="quay.io/biocontainer/kraken:2.1.2--pl5321h9f5acd7_3"
LABEL software.name="kraken"
LABEL software.version=${VERSION}
LABEL software.description="kraken metagenomics analyser"
LABEL software.website="http://www.htslib.org"
LABEL software.documentation="http://www.htslib.org/doc/samtools.html"
LABEL software.license="MIT/Expat"
LABEL tags="Genomics"

ADD kraken.sh /scripts/

RUN sed -i "s/\r//g" /scripts/kraken.sh

WORKDIR /scratch

