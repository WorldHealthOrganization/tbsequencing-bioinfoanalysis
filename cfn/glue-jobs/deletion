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

def AddRefAltAD(dynamicRecord):
    dynamicRecord["reference_ad"] = int(dynamicRecord["rr"]+dynamicRecord["dr"])
    dynamicRecord["alternative_ad"] = int(dynamicRecord["rv"]+dynamicRecord["dv"])
    dynamicRecord["total_dp"] = int(dynamicRecord["reference_ad"]+dynamicRecord["alternative_ad"])
    return dynamicRecord

glueContext = GlueContext(SparkContext().getOrCreate())
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

datasource0 = glueContext.create_dynamic_frame.from_catalog(database = args["glue_db_name"], table_name = "deletion", transformation_ctx = "datasource0")

total = Map.apply(frame = datasource0, f = AddRefAltAD)

applymapping1 = ApplyMapping.apply(frame = total, mappings = [("sampleid", "long", "sample_id", "int"), ("chromosome", "string", "chromosome", "string"), ("position", "long", "position", "int"), ("deletion", "string", "reference_nucleotide", "string"), ("alternativenucleotide", "string", "alternative_nucleotide", "string"), ("genotyper", "string", "genotyper", "string"), ("quality", "long", "quality", "double"), ("alternative_ad", "int", "alternative_ad", "int"), ("reference_ad", "int", "reference_ad", "int"), ("value", "string", "genotype_value", "string"), ("total_dp", "int", "total_dp", "int")], transformation_ctx = "applymapping1")

selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["alternative_ad", "total_dp", "reference_nucleotide", "genotype_value", "chromosome", "position", "genotyper", "alternative_nucleotide", "reference_ad", "sample_id", "quality"], transformation_ctx = "selectfields2")

resolvechoice3 = ResolveChoice.apply(frame = selectfields2, choice = "MATCH_CATALOG", database = args["glue_db_name"], table_name = f"{args['postgres_db_name']}_genphensql_staged_genotype", transformation_ctx = "resolvechoice3")

resolvechoice4 = ResolveChoice.apply(frame = resolvechoice3, choice = "make_cols", transformation_ctx = "resolvechoice4")

datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = resolvechoice4, database = args["glue_db_name"], table_name = f"{args['postgres_db_name']}_genphensql_staged_genotype", transformation_ctx = "datasink5")

job.commit()