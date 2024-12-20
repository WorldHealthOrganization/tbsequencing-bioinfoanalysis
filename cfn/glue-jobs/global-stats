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

datasource0 = glueContext.create_dynamic_frame.from_catalog(database = args["glue_db_name"], table_name = "global_stats", transformation_ctx = "datasource0")

applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("sampleid", "long", "sample_id", "int"), ("mediandepth", "double", "median_depth", "double"), ("coverage10x", "double", "coverage_10x", "double"), ("coverage15x", "double", "coverage_15x", "double"), ("coverage20x", "double", "coverage_20x", "double"), ("coverage30x", "double", "coverage_30x", "double"), ("rawtotalsequences", "long", "raw_total_sequences", "long"), ("filteredsequences", "long", "filtered_sequences", "long"), ("sequences", "long", "sequences", "long"), ("issorted", "long", "is_sorted", "long"), ("firstfragments", "long", "first_fragments", "long"), ("lastfragments", "long", "last_fragments", "long"), ("readsmapped", "long", "reads_mapped", "long"), ("readsmappedandpaired", "long", "reads_mapped_and_paired", "long"), ("readsunmapped", "long", "reads_unmapped", "long"), ("readsproperlypaired", "long", "reads_properly_paired", "long"), ("readspaired", "long", "reads_paired", "long"), ("readsduplicated", "long", "reads_duplicated", "long"), ("readsmq0", "long", "reads_mq_0", "long"), ("readsqcfailed", "long", "reads_qc_failed", "long"), ("nonprimaryalignments", "long", "non_primary_alignments", "long"), ("totallength", "long", "total_length", "long"), ("totalfirstfragmentlength", "long", "total_first_fragment_length", "long"), ("totallastfragmentlength", "long", "total_last_fragment_length", "long"), ("basesmapped", "long", "bases_mapped", "long"), ("basesmappedcigar", "long", "bases_mapped_cigar", "long"), ("basestrimmed", "long", "bases_trimmed", "long"), ("basesduplicated", "long", "bases_duplicated", "long"), ("mismatches", "long", "mismatches", "long"), ("errorrate", "double", "error_rate", "double"), ("averagelength", "long", "average_length", "int"), ("averagefirstfragmentlength", "long", "average_first_fragment_length", "int"), ("averagelastfragmentlength", "long", "average_last_fragment_length", "int"), ("maximumlength", "long", "maximum_length", "int"), ("maximumfirstfragmentlength", "long", "maximum_first_fragment_length", "int"), ("maximumlastfragmentlength", "long", "maximum_last_fragment_length", "int"), ("averagequality", "double", "average_quality", "double"), ("insertsizeaverage", "double", "insert_size_average", "double"), ("insertsizestandarddeviation", "double", "insert_size_standard_deviation", "double"), ("inwardorientedpairs", "long", "inward_oriented_pairs", "int"), ("outwardorientedpairs", "long", "outward_oriented_pairs", "int"), ("pairswithotherorientation", "long", "pairs_with_other_orientation", "int"), ("pairsondifferentchromosomes", "long", "pairs_on_different_chromosomes", "int"), ("percentageofproperlypairedreads", "double", "percentage_of_properly_paired_reads", "double")], transformation_ctx = "applymapping1")

selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["median_depth", "percentage_of_properly_paired_reads", "reads_paired", "filtered_sequences", "reads_properly_paired", "outward_oriented_pairs", "coverage_30x", "reads_unmapped", "average_last_fragment_length", "coverage_20x", "coverage_10x", "reads_qc_failed", "bases_mapped", "bases_trimmed", "total_last_fragment_length", "reads_mapped_and_paired", "maximum_length", "insert_size_average", "total_first_fragment_length", "reads_duplicated", "insert_size_standard_deviation", "reads_mq_0", "average_quality", "sample_id", "first_fragments", "total_length", "sequences", "raw_total_sequences", "pairs_with_other_orientation", "average_length", "maximum_first_fragment_length", "last_fragments", "reads_mapped", "pairs_on_different_chromosomes", "is_sorted", "bases_mapped_cigar", "mismatches", "non_primary_alignments", "coverage_15x", "maximum_last_fragment_length", "error_rate", "inward_oriented_pairs", "average_first_fragment_length", "bases_duplicated"], transformation_ctx = "selectfields2")

resolvechoice3 = ResolveChoice.apply(frame = selectfields2, choice = "MATCH_CATALOG", database = args["glue_db_name"], table_name = f"{args['postgres_db_name']}_public_submission_summarysequencingstats", transformation_ctx = "resolvechoice3")

resolvechoice4 = ResolveChoice.apply(frame = resolvechoice3, choice = "make_cols", transformation_ctx = "resolvechoice4")

datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = resolvechoice4, database = args["glue_db_name"], table_name = f"{args['postgres_db_name']}_public_submission_summarysequencingstats", transformation_ctx = "datasink5")

job.commit()