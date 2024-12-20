FROM quay.io/biocontainers/agat:0.5.0--pl526r35_0

ARG VERSION=0.5.0

LABEL container.base.image="quay.io/biocontainer/agat:0.5.0--pl526r35_0"
LABEL software.name="agat"
LABEL software.version=${VERSION}
LABEL software.description="Another Gff Analysis Toolkit (AGAT). Suite of tools to handle gene annotations in any GTF/GFF format."
LABEL software.website="https://github.com/NBISweden/AGAT"
LABEL software.license="GPL/GPLv3"
LABEL tags="Perl"

ENTRYPOINT ["agat_convert_sp_gff2gtf.pl"]

