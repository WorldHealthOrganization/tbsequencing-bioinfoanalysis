import sys
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', "database_name", "postgres_db_name"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

datasource0 = glueContext.create_dynamic_frame.from_catalog(database = args["database_name"], table_name = f"{args['postgres_db_name']}_genphensql_staged_genotype")

datasource_variant = glueContext.create_dynamic_frame.from_catalog(database = args["database_name"], table_name = f"{args['postgres_db_name']}_public_genphen_variant").toDF()

datasource_sample = glueContext.create_dynamic_frame.from_catalog(database = args["database_name"], table_name = f"{args['postgres_db_name']}_public_submission_sample")

join_sample = Join.apply(frame1 = datasource0, frame2 = datasource_sample, keys1 = "SampleId", keys2 = "Id").toDF()

join_variant = DynamicFrame.fromDF(join_sample.join(datasource_variant, [
    join_sample.Chromosome==datasource_variant.Chromosome,
    join_sample.Position==datasource_variant.Position,
    join_sample.ReferenceNucleotide==datasource_variant.ReferenceNucleotide,
    join_sample.AlternativeNucleotide==datasource_variant.AlternativeNucleotide
    ],
    "inner"), glueContext, "join_variant")

applymapping1 = ApplyMapping.apply(frame = join_variant, mappings = [("Id", "int", "sampleid", "int"), ("VariantId", "int", "variantid", "int"), ("totaldp", "int", "totaldp", "int"), ("quality", "double", "quality", "double"), ("alternativead", "int", "alternativead", "int"), ("referencead", "int", "referencead", "int"), ("genotyper", "string", "genotyper", "string"), ("genotypevalue", "string", "genotypevalue", "string")])

selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["sampleid", "variantid", "totaldp", "genotypeid", "quality", "alternativead", "referencead", "genotyper", "genotypevalue"], transformation_ctx = "selectfields2")

resolvechoice3 = ResolveChoice.apply(frame = selectfields2, choice = "MATCH_CATALOG", database = args["database_name"], table_name = f"{args['postgres_db_name']}_public_submission_genotype", transformation_ctx = "resolvechoice3")

resolvechoice4 = ResolveChoice.apply(frame = resolvechoice3, choice = "make_cols", transformation_ctx = "resolvechoice4")

datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = resolvechoice4, database = args["database_name"], table_name = f"{args['postgres_db_name']}_public_submission_genotype", transformation_ctx = "datasink5")

job.commit()