FROM quay.io/biocontainers/hail:0.2.33--py36hf1ae8f4_1

ARG VERSION=0.2.33

LABEL container.base.image="quay.io/biocontainer/hail:0.2.33--py36hf1ae8f4_1"
LABEL software.name="hail"
LABEL software.version=${VERSION}
LABEL software.description="Python module for working with genomic data"
LABEL software.website="https://hail.is"
LABEL software.documentation="https://github.com/hail-is/hail"
LABEL software.license="MIT"
LABEL tags="Python"

#This is to fix import for references other than human

RUN sed -i "s/default('chr' + ht\['seqname'\]))/default(ht\['seqname'\]))/" /usr/local/lib/python3.6/site-packages/hail/experimental/import_gtf.py

RUN pip install pandas xlrd psycopg2-binary

ADD combine_datasets.py /scratch/

ADD prepare_datasets.py /scratch/

ADD prepare_gene_models.py /scratch/

ADD prepare_outputs.py /scratch/

ADD write_results_files.py /scratch/

ENTRYPOINT ["python"]

