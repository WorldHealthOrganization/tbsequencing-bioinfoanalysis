FROM quay.io/biocontainers/raxml-ng:1.0.1--h7447c1b_0 

ARG VERSION=1.0.1

LABEL container.base.image="quay.io/biocontainers/raxml-ng:1.0.1--h7447c1b_0"
LABEL software.name="raxml-ng"
LABEL software.version=${VERSION}
LABEL software.description="Phylogenetic reconstruction"
LABEL software.website="https://cme.h-its.org/exelixis/web/software/raxml/"
LABEL software.documentation="https://cme.h-its.org/exelixis/web/software/raxml/#documentation"
LABEL software.license="MIT"
LABEL tags="RAxML"

#This is to fix import for references other than human

ENTRYPOINT ["raxml-ng"]

