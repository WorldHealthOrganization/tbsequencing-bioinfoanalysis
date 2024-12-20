FROM quay.io/biocontainers/fastqc:0.12.1--hdfd78af_0

ARG VERSION=0.12.1--hdfd78af_0

LABEL container.base.image="quay.io/biocontainers/fastqc:0.12.1--hdfd78af_0"
LABEL software.name="fastqc"
LABEL software.version=${VERSION}
LABEL software.description="Read quality controls"
LABEL software.website="https://cab.spbu.ru/software/spades/"
LABEL software.documentation="https://cab.spbu.ru/software/spades/"
LABEL software.license="MIT/Expat"
LABEL tags="Genomics, Assembly"

ADD fastqc.sh /scripts/

RUN sed -i "s/\r//g" /scripts/fastqc.sh

WORKDIR /scratch

