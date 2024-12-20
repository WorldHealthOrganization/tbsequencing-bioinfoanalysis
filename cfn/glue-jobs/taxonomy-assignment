import sys
# import boto3
# import os
# import json

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(
    sys.argv,
    ['JOB_NAME', "glue_db_name", "postgres_db_name", "rds_port", "rds_host"]
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

datasource0 = (
    glueContext.create_dynamic_frame.from_catalog(
        database = args["glue_db_name"],
        table_name = "taxonomy_assignment",
        transformation_ctx = "datasource0"
    )
)

applymapping1 = (
    ApplyMapping.apply(
        frame = datasource0,
        mappings = [("sampleid", "long", "sample_id", "int"), ("ncbitaxonid", "long", "ncbi_taxon_id", "int"), ("value", "double", "value", "double")],
        transformation_ctx = "applymapping1"
    )
)

selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["value", "ncbi_taxon_id", "sample_id"], transformation_ctx = "selectfields2")

def correct_taxonid(dynamicRecord):
    if dynamicRecord["ncbi_taxon_id"]==1570328:
        dynamicRecord["ncbi_taxon_id"]=1795
    return dynamicRecord

map4 = (
    Map.apply(frame = selectfields2, f = correct_taxonid, transformation_ctx = "map4")
    .filter(f=lambda x: x["ncbi_taxon_id"]!=0)
)

resolvechoice3 = ResolveChoice.apply(frame = map4, choice = "MATCH_CATALOG", database = args["glue_db_name"], table_name = f"{args['postgres_db_name']}_public_submission_taxonomystats", transformation_ctx = "resolvechoice3")

resolvechoice4 = ResolveChoice.apply(frame = resolvechoice3, choice = "make_cols", transformation_ctx = "resolvechoice4")

datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = resolvechoice4, database = args["glue_db_name"], table_name = f"{args['postgres_db_name']}_public_submission_taxonomystats", transformation_ctx = "datasink5")

job.commit()