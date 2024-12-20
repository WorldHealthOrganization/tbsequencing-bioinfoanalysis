import sys

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(
    sys.argv,
    ['JOB_NAME', "glue_db_name", "postgres_db_name", "rds_port", "rds_host"]
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

datasource0 = glueContext.create_dynamic_frame.from_catalog(database = args["glue_db_name"], table_name = "locus_stats", transformation_ctx = "datasource0")

datasource1 = glueContext.create_dynamic_frame.from_catalog(database = args["glue_db_name"], table_name = f"{args['postgres_db_name']}_public_genphen_locustag")

join1 = Join.apply(frame1 = datasource0, frame2 = datasource1, keys1 = "locustagname", keys2 = "locus_tag_name", transformation_ctx="join3")

applymapping1 = ApplyMapping.apply(frame = join1, mappings = [("gene_db_crossref_id", "int", "gene_db_crossref_id", "int"), ("sampleid", "long", "sample_id", "int"), ("meandepth", "double", "mean_depth", "double"), ("cov10x", "double", "coverage_10x", "double"), ("cov15x", "double", "coverage_15x", "double"), ("cov20x", "double", "coverage_20x", "double"), ("cov30x", "double", "coverage_30x", "double")], transformation_ctx = "applymapping1")

selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["sample_id", "gene_db_crossref_id", "mean_depth", "coverage_15x", "coverage_30x", "coverage_20x", "coverage_10x"], transformation_ctx = "selectfields2")

resolvechoice3 = ResolveChoice.apply(frame = selectfields2, choice = "MATCH_CATALOG", database = args["glue_db_name"], table_name = f"{args['postgres_db_name']}_public_submission_locussequencingstats", transformation_ctx = "resolvechoice3")

resolvechoice4 = ResolveChoice.apply(frame = resolvechoice3, choice = "make_cols", transformation_ctx = "resolvechoice4")

datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = resolvechoice4, database = args["glue_db_name"], table_name = f"{args['postgres_db_name']}_public_submission_locussequencingstats", transformation_ctx = "datasink5")

job.commit()