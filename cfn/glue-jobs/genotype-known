import sys

from awsglue.transforms import ApplyMapping, Filter, SelectFields
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from awsglue.context import GlueContext

from pyspark.context import SparkContext

args = getResolvedOptions(
    sys.argv,
    ['JOB_NAME', "glue_db_name", "postgres_db_name"]
)

glueContext = GlueContext(SparkContext().getOrCreate())
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database = args["glue_db_name"],
    table_name = "genotype",
    transformation_ctx = "datasource0"
)

applymapping1 = ApplyMapping.apply(
    frame = datasource0,
    mappings = [
        ("genotyper", "string", "genotyper", "string"),
        ("sampleid", "long", "sample_id", "int"),
        ("chrom", "string", "chromosome", "string"),
        ("pos", "long", "position", "int"),
        ("altad", "long", "alternative_ad", "int"),
        ("ref", "string", "reference_nucleotide", "string"),
        ("alt", "string", "alternative_nucleotide", "string"),
        ("qual", "string", "quality", "double"),
        ("refad", "long", "reference_ad", "int"),
        ("id", "string", "variant_id", "int"),
        ("totaldp", "long", "total_dp", "int"),
        ("value", "string", "genotype_value", "string")
    ],
    transformation_ctx = "applymapping1"
)

known_variant_genotype = Filter.apply(
    frame=applymapping1,
    f = lambda x: x["variant_id"] is not None,
    transformation_ctx = "filterknownvariants"
)

selectfields2 = SelectFields.apply(
    frame = known_variant_genotype,
    paths = [
        "variant_id",
        "sample_id",
        "reference_ad",
        "alternative_ad", 
        "total_dp",
        "quality",
        "genotyper",
        "genotype_value"
    ],
    transformation_ctx = "selectfields2"
)

datasink5 = glueContext.write_dynamic_frame.from_catalog(
    frame = selectfields2,
    database = args["glue_db_name"],
    table_name = f"{args['postgres_db_name']}_public_submission_genotype",
    transformation_ctx = "datasink5"
)

job.commit()