FROM quay.io/biocontainers/sra-tools:3.0.5--h9f5acd7_1

ARG VERSION=3.0.0

RUN mkdir /root/.ncbi/

RUN echo '/repository/user/cache-disabled="true"' > /root/.ncbi/user-settings.mkfg
RUN echo '/repository/user/main/public/root="/scratch/ncbi/"' >> /root/.ncbi/user-settings.mkfg
RUN printf '/LIBS/GUID = "%s"\n' `uuid` >> /root/.ncbi/user-settings.mkfg

RUN vdb-config --report-cloud-identity yes

LABEL container.base.image="quay.io/biocontainers/sra-tools"
LABEL software.name="sra-tools"
LABEL software.version=${VERSION}
LABEL software.description="Utilities for downloading raw data from NCBI"
LABEL tags="Genomics"

ADD fasterq_dump.sh /scripts/

ADD normalized_to_fastq.sh /scripts/

RUN sed -i "s/\r//g" /scripts/fasterq_dump.sh

RUN sed -i "s/\r//g" /scripts/normalized_to_fastq.sh

WORKDIR /scratch