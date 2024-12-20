import sys

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, regexp_extract, length, substring, expr

args = getResolvedOptions(
    sys.argv,
    ['JOB_NAME', "glue_db_name", "postgres_db_name"]
)

glueContext = GlueContext(SparkContext.getOrCreate())

spark = glueContext.spark_session

job = Job(glueContext)

variant = (
    glueContext.create_data_frame.from_catalog(
        database = args["glue_db_name"],
        table_name = f"{args['postgres_db_name']}_public_genphen_variant"
    )
    .alias("variant")
)

unique_del_variants_from_new_genotypes = (
    glueContext.create_data_frame.from_catalog(database = args["glue_db_name"], table_name = f"{args['postgres_db_name']}_genphensql_staged_genotype")
    .where(
        col("reference_nucleotide").rlike("^DEL-[0-9]+$")
    )
    .select(
        col("chromosome"),
        col("position"),
        col("reference_nucleotide"),
        col("alternative_nucleotide")
    )
    .distinct()
    .withColumn(
        "deletion_length", regexp_extract(col("reference_nucleotide"), 'DEL-([0-9]+)', 1).cast("int")-col("position")
    )
    .where(
        col("deletion_length")<100000
    )
    .alias("del_variants_new_genotypes")
)
        
bioentry = glueContext.create_data_frame.from_catalog(
    database = args["glue_db_name"],
    table_name = f"{args['postgres_db_name']}_biosql_bioentry"
)

biosequence = (
    glueContext.create_data_frame.from_catalog(
        database = args["glue_db_name"],
        table_name = f"{args['postgres_db_name']}_biosql_biosequence"
        )
    .alias("biosequence")
)

print(unique_del_variants_from_new_genotypes.count())

del_variants = (
    unique_del_variants_from_new_genotypes
    .join(
        variant,
        (variant.chromosome==unique_del_variants_from_new_genotypes.chromosome)
        & (variant.position==unique_del_variants_from_new_genotypes.position)
        & (variant.alternative_nucleotide==unique_del_variants_from_new_genotypes.alternative_nucleotide)
        & (length(variant.reference_nucleotide)==unique_del_variants_from_new_genotypes.deletion_length),
        how="left_anti"
    )
    .select(
        col("chromosome"),
        col("position"),
        col("alternative_nucleotide"),
        col("deletion_length"),
    )
    .distinct()
    .join(
        bioentry,
        bioentry.identifier==unique_del_variants_from_new_genotypes.chromosome,
        how="inner"
    )
    .join(
        biosequence,
        bioentry.bioentry_id==biosequence.bioentry_id,
        how="inner"
    )
    .withColumn(
        "reference_nucleotide",
        expr("substring(biosequence.seq, del_variants_new_genotypes.position, del_variants_new_genotypes.deletion_length)")
    )
    .select(
        col("chromosome"),
        col("position"),
        col("reference_nucleotide"),
        col("alternative_nucleotide"),
    )
    .distinct()
    .alias("new_del_variants")
)

print(del_variants.count())

final=DynamicFrame.fromDF(
    del_variants,
    glueContext,
    "final"
)

datasink5 = glueContext.write_dynamic_frame.from_catalog(
    frame = final,
    database = args["glue_db_name"],
    table_name = f"{args['postgres_db_name']}_public_genphen_variant",
    transformation_ctx = "datasink5"
)

job.commit()