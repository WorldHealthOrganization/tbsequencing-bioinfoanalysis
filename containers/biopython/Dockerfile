FROM quay.io/biocontainers/biopython:1.78

ARG VERSION=1.77

LABEL container.base.image="quay.io/biocontainers/biopython:1.78"
LABEL software.name="biopython"
LABEL software.version=${VERSION}
LABEL software.description="Python module for processing biosequences"
LABEL software.website="https://biopython.org/"
LABEL software.documentation="https://biopython.org/wiki/Documentation"
LABEL software.license="Biopython License Agreement"
LABEL tags="Python"

RUN pip install bcbio-gff==0.6.9 gffutils==0.11.0 pandas==1.4.2 xlrd==2.0.1 psycopg2-binary==2.9.3 bs4==0.0.1 urllib3==1.26.9 openpyxl==3.0.10 jinja2==3.1.2 xlsxwriter==3.0.3 pycountry==22.3.5 boto3==1.24.5 recordlinkage==0.15 requests==2.27.1 numpy==1.26.4

ADD gff_to_bed.py /scripts/

ADD copy_fastq_from_s3.py /scripts/

ADD download_references_ncbi_dataset_api.py /scripts/

# ADD sample_data_preparation.py /scripts/

ADD format_sequencing_stats.py /scripts/

ADD format_taxonomy_assignment.py /scripts/

ADD calculate_quality_metrics_for_sample.py /scripts/

ADD export_to_s3.py /scripts/

ADD create_snpEff_config_file.py /scripts/

ADD query_rds.py /scripts/

ADD load_annotation_variants.py /scripts/

ADD fix_gff_for_snpEff.py /scripts/

WORKDIR /scratch

ENTRYPOINT ["python"]