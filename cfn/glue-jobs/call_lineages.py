import sys

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql.functions import col, bround, length

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "glue_db_name",
    "postgres_db_name",
    "rds_glue_connection_name",
    "genotype_threshold"
])

job.init(args['JOB_NAME'], args)

glue_db_name = args["glue_db_name"]
dbname = args["postgres_db_name"]
rds_connector_name = args["rds_glue_connection_name"]
threshold = float(args["genotype_threshold"])

conn = glueContext.extract_jdbc_conf(rds_connector_name)


lineage_markers = (
    glueContext.create_data_frame.from_catalog(
        database=glue_db_name,
        table_name=dbname + "_public_genphen_variantlineageassociation"
    ).alias("lineage_marker")
)

lineage_name = (
    glueContext.create_data_frame.from_catalog(
        database=glue_db_name,
        table_name=dbname + "_public_genphen_lineage"
    ).alias("lineage_name")
)

unknown_markers = (
    lineage_markers
    .where(
        col("variant_id").isNull()
    )
)

# if we have newly inserted markers (via the Django admin)
# we assign the variant id to the markers
# and resinsert them
if unknown_markers.count():

    # fetching variant id
    variant = (
        glueContext.create_data_frame.from_catalog(
            database=glue_db_name,
            table_name=dbname + "_public_genphen_variant"
        ).alias("variant")
    )

    lineage_markers = (
        lineage_markers
        .join(
            variant,
            on=
                (col("variant.position")==col("lineage_marker.position"))
                & (col("variant.alternative_nucleotide")==col("lineage_marker.alternative_nucleotide"))
                & (length(col("variant.reference_nucleotide"))==1),
            how="inner"
        )
        .select(
            col("variant.variant_id"),
            col("variant.position"),
            col("variant.alternative_nucleotide"),
            col("count_presence"),
            col("lineage_id"),
        )
    )


    # overwriting the table with the variant id column filled
    (
        lineage_markers
        .write
        .mode('overwrite')
        .format("jdbc")
        .option("connschema", dbname)
        .option("url", conn['fullUrl'])
        .option("dbtable", "public.genphen_variantlineageassociation")
        .option("user", conn['user'])
        .option('truncate', 'true')
        .option("password", conn['password'])
        .option("driver", "org.postgresql.Driver")
        .save()
    )

# only keep the column of interest
# we need the position for the grouping of absent alleles
# otherwise we would only need variant id
lineage_markers = (
    lineage_markers
    .select(
        col("variant_id"),
        col("position"),
        col("count_presence"),
        col("lineage_id"),
    )
    .alias("marker")
)

# a cross join between samples and markers
# is also necessary because of the absent alleles identification
sample = (
    glueContext.create_data_frame.from_catalog(
        database=glue_db_name,
        table_name=dbname + "_public_submission_sample",
    ).alias("sample")
)

genotype = (
    glueContext.create_data_frame.from_catalog(
        database=glue_db_name,
        table_name=dbname + "_public_submission_genotype",
        transformation_ctx = "genotype_datasource",
    )
    .alias("genotype")
)

# we use genotypes from bcftools so that 
# we don't have to deal with MCNV
sample_x_markers = (
    sample.
    crossJoin(
        lineage_markers
    )
    .join(
        genotype,
        on = 
            (col("sample.id")==col("sample_id"))
            & (col("genotype.variant_id")==col("marker.variant_id"))
            & (col("genotype.genotyper")=="bcftools"),
        how="left"
    )
    .withColumn(
       "af",
        bround(col("alternative_ad").cast("long")/col("total_dp").cast("long"), 2)
    )
    .fillna(
        0,
        subset="af"
    )
    .groupBy(
        "sample_id",
        "position",
        "count_presence",
        "lineage_id",
    )
    .agg(
        {"af": "sum"}
    )
    .where(
        ((col("count_presence")==True) & (col("sum(af)")>=threshold))
        | ((col("count_presence")==False) & (col("sum(af)")<=(1-threshold)))
    )
    .groupBy(
        "sample_id",
        "lineage_id"
    )
    .count()
)

final = (
    sample_x_markers
    .join(
        lineage_name,
        on=col("lineage_id")==col("lineage_name.id"),
        how="inner"
    )
)

job.commit()
