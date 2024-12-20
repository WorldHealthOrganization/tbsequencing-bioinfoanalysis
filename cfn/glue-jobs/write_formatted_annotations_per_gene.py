import sys
# import boto3
# import os
# import json

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from variant_annotation_categorization import formatted_annotation_per_gene
from biosql_gene_views import protein_id_view


args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "glue_db_name",
    "postgres_db_name",
    "rds_glue_connection_name",
])

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
# spark._jsc.hadoopConfiguration().set('spark.sql.broadcastTimeout', '3600')

job = Job(glueContext)
job.init(args['JOB_NAME'], args)


dbname = args["postgres_db_name"]
rds_connector_name = args["rds_glue_connection_name"]
glue_dbname = args["glue_db_name"]

dbxref = glueContext.create_data_frame.from_catalog(
    database=glue_dbname,
    table_name=dbname + "_biosql_dbxref"
)

sqv = glueContext.create_data_frame.from_catalog(
    database=glue_dbname,
    table_name=dbname + "_biosql_seqfeature_qualifier_value"
)

sdc = glueContext.create_data_frame.from_catalog(
    database=glue_dbname,
    table_name=dbname + "_biosql_seqfeature_dbxref"
)

protein_id = protein_id_view(dbxref, sqv, sdc).alias("protein_id")

vta = glueContext.create_data_frame.from_catalog(
    database=glue_dbname,
    table_name=dbname + "_public_genphen_varianttoannotation",
    transformation_ctx = "vta_datasource",
    additional_options = {
        "jobBookmarkKeys": ["variant_id", "annotation_id"]
    }
)

print(f"### new variant to annotation link counts: {vta.count()} ###")

annot = glueContext.create_data_frame.from_catalog(
    database=glue_dbname,
    table_name=dbname + "_public_genphen_annotation"
)

fapg = formatted_annotation_per_gene(vta, annot, dbxref, protein_id)

print("### final ###")
fapg.show(20)
print(f"### new fapg counts: {fapg.count()} ###")

fapg_dynamic_frame = (
    DynamicFrame.fromDF(
        fapg,
        glueContext,
        "final"
    )
    .resolveChoice(
        choice="match_catalog",
        database=glue_dbname,
        table_name=dbname+"_public_genphen_formattedannotationpergene",
        transformation_ctx="resolve_choice"
    )
)

datasink5 = (
    glueContext.write_dynamic_frame.from_catalog(
        frame=fapg_dynamic_frame,
        database=glue_dbname,
        table_name=dbname+"_public_genphen_formattedannotationpergene",
        transformation_ctx="datasink5"
    )
)

job.commit()
