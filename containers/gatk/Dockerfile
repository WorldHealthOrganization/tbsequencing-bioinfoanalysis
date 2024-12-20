FROM quay.io/biocontainers/gatk4:4.4.0.0--py36hdfd78af_0

ARG VERSION=4.1.4.1

LABEL container.base.image="quay.io/biocontainer/gatk4:4.4.0.0--py36hdfd78af_0"
LABEL software.name="picard"
LABEL software.version=${VERSION}
LABEL software.description="Java tools for working with NGS data in the BAM format"
LABEL software.website="https://github.com/broadinstitute/picard/"
LABEL software.documentation=""
LABEL software.license="MIT/Expat"
LABEL tags="Genomics"

ADD gatk_rmdup.sh /scripts/

RUN sed -i "s/\r//g" /scripts/gatk_rmdup.sh

WORKDIR /scratch

