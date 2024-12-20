FROM quay.io/biocontainers/mosdepth:0.3.3--hd299d5a_3

ARG VERSION=0.3.3--hd299d5a_3

LABEL container.base.image="quay.io/biocontainers/mosdepth:0.3.3--hd299d5a_3"
LABEL software.name="mosdepth"
LABEL software.version=${VERSION}
LABEL software.description="software for calculating coverage and depth across intervals"
LABEL software.website="http://www.htslib.org"
LABEL software.documentation="http://www.htslib.org/doc/samtools.html"
LABEL software.license="MIT/Expat"
LABEL tags="Genomics"

ADD cleanup_files.sh /scripts/
ADD download_ftp.sh /scripts/
ADD mosdepth.sh /scripts/
ADD rename_alignment.sh /scripts/

RUN sed -i "s/\r//g" /scripts/cleanup_files.sh
RUN sed -i "s/\r//g" /scripts/download_ftp.sh
RUN sed -i "s/\r//g" /scripts/mosdepth.sh
RUN sed -i "s/\r//g" /scripts/rename_alignment.sh

WORKDIR /scratch

