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

new_variant_genotype = (
    Filter.apply(
        frame=applymapping1,
        f = lambda x: x["variant_id"] is None,
        transformation_ctx = "filterunknownvariants"
    )
)

selectfields_new_variant_genotype = (
    SelectFields.apply(
        frame = new_variant_genotype,
        paths = [
            "chromosome",
            "position",
            "reference_nucleotide",
            "alternative_nucleotide",
            "sample_id",
            "reference_ad",
            "alternative_ad",
            "total_dp",
            "quality",
            "genotyper",
            "genotype_value"
        ],
        transformation_ctx = "selectfields_new_variant"
    )
)

datasink_unknown = glueContext.write_dynamic_frame.from_catalog(
    frame = selectfields_new_variant_genotype,
    database = args["glue_db_name"],
    table_name = f"{args['postgres_db_name']}_genphensql_staged_genotype",
    transformation_ctx = "datasink_unknown"
)

new_variant = (
    new_variant_genotype
    .select_fields(
        [
            "chromosome",
            "position",
            "reference_nucleotide",
            "alternative_nucleotide"
        ]
    )
    .toDF()
    .distinct()
)

# I don't think we need to double check the variants
# It would mean that bcftools annotate failed at recognizing one existing variant
# It has a significant computational cost

# if new_variant.count():
#     variant = (
#         glueContext.create_data_frame.from_catalog(
#             database = args["glue_db_name"],
#             table_name = f"{args['postgres_db_name']}_public_genphen_variant"
#         )
#         .alias("variant")
#     )

#     double_checked_new_variant = (
#         new_variant
#         .join(
#             variant,
#             ["chromosome", "position", "reference_nucleotide", "alternative_nucleotide"],
#             "left_anti"
#         )
#     )

new_variant_dynamic_frame = (
    DynamicFrame.fromDF(
        new_variant,
        glueContext,
        "final"
    )
)

datasink_new_variant = glueContext.write_dynamic_frame.from_catalog(
    frame = new_variant_dynamic_frame,
    database = args["glue_db_name"],
    table_name = f"{args['postgres_db_name']}_public_genphen_variant",
    transformation_ctx = "datasink_new_variant"
)

job.commit()