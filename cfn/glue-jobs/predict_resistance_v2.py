import sys

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext

import pyspark.sql.functions as F
from pyspark.sql.types import BooleanType, StringType

from awsglue.dynamicframe import DynamicFrame

from variant_annotation_categorization import (
    formatted_annotation_per_gene,
    multiple_variant_decomposition,
    sanitize_synonymous_variant,
    missense_codon_list,
    tiered_drug_variant_categories
)

from biosql_gene_views import (
    protein_id_view,
    gene_or_locus_tag_view,
    merge_gene_locus_view
)

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "glue_database_name",
    "postgres_database_name",
    "rds_glue_connection_name"
])

job.init(args['JOB_NAME'], args)

glue_db_name = args["glue_database_name"]
dbname = args["postgres_database_name"]
rds_connector_name = args["rds_glue_connection_name"]

conn = glueContext.extract_jdbc_conf(rds_connector_name)

dbxref = glueContext.create_data_frame.from_catalog(database = glue_db_name, table_name = dbname+"_biosql_dbxref")
sqv = glueContext.create_data_frame.from_catalog(database = glue_db_name, table_name = dbname+"_biosql_seqfeature_qualifier_value")
sdc = glueContext.create_data_frame.from_catalog(database = glue_db_name, table_name = dbname+"_biosql_seqfeature_dbxref")
seqfeature = glueContext.create_data_frame.from_catalog(database = glue_db_name, table_name = dbname+"_biosql_seqfeature").alias("seqfeature")
term = glueContext.create_data_frame.from_catalog(database = glue_db_name, table_name = dbname+"_biosql_term").alias("term1")

variant = glueContext.create_data_frame.from_catalog(database = glue_db_name, table_name = dbname+"_public_genphen_variant").alias("variant")

tier = glueContext.create_data_frame.from_catalog(database = glue_db_name, table_name = dbname+"_public_genphen_genedrugresistanceassociation").alias("tier")

variant_grades = (
    glueContext.create_data_frame.from_catalog(database = glue_db_name, table_name = dbname+"_public_genphen_variantgrade")
    .where(
        (F.col("grade_version")==2)
    )
)

unknown_markers = (
    variant_grades
    .where(
        F.col("variant_id").isNull()
    )
)

if unknown_markers.count():
    variant_grades = (
        variant_grades
        .join(
            variant,
            on=["position", "alternative_nucleotide", "reference_nucleotide"],
            how="inner"
        )
        .groupBy(
            F.col("variant.variant_id"),
            F.col("drug_id"),
            F.col("grade_version"),
        )
        .agg(
            F.min("grade").alias("grade")
        )
    )

    # overwriting the table with the variant id column filled
    (
        variant_grades
        .write
        .mode('overwrite')
        .format("jdbc")
        .option("connschema", dbname)
        .option("url", conn['fullUrl'])
        .option("user", conn['user'])
        .option("password", conn['password'])
        .option("dbtable", "public.genphen_variantgrade")
        .option('truncate', 'true')
        .save()
    )

protein_id = ( 
    protein_id_view(
        dbxref,
        sqv,
        sdc        
    )
    .alias("protein_id")
)

fapg = (
    formatted_annotation_per_gene(
        glueContext.create_data_frame.from_catalog(database = glue_db_name, table_name = dbname+"_public_genphen_varianttoannotation"),
        glueContext.create_data_frame.from_catalog(database = glue_db_name, table_name = dbname+"_public_genphen_annotation"),
        dbxref,
        protein_id
    )
    .alias("fapg")
)

mvd = multiple_variant_decomposition(variant).alias("mvd")

san = sanitize_synonymous_variant(fapg, tier, mvd)

gene_locus_tag = (
    merge_gene_locus_view(
        gene_or_locus_tag_view(sdc, sqv, seqfeature, term, "gene_symbol").alias("gene"),
        gene_or_locus_tag_view(sdc, sqv, seqfeature, term, "rv_symbol").alias("locus_tag")
    )
    .alias("gene_locus_tag")
)

promoter_distance = glueContext.create_data_frame.from_catalog(database = glue_db_name, table_name = dbname+"_public_genphen_promoterdistance")

var_cat = (
    tiered_drug_variant_categories(fapg, san, tier, variant, mvd, promoter_distance, missense_codon_list(fapg, variant, tier), pool_frameshift=True)
    .drop(
        "position"
    )
    .join(
        gene_locus_tag,
        "gene_db_crossref_id",
        "inner"
    )
    .withColumn(
        "variant_catalogue_name",
        F.concat_ws(
            "_",
            F.col("resolved_symbol"),
            F.col("variant_category")
        )
    )
    .distinct()
)

drug = glueContext.create_data_frame.from_catalog(
    database=glue_db_name,
    table_name=dbname + "_public_genphen_drug"
)

v2_grading = (
    glueContext.create_data_frame.from_catalog(database = glue_db_name, table_name = dbname+"_public_genphen_variantgrade")
    .where(
        (F.col("grade_version")==2)
        & F.col("grade").isin([1, 2])
    )
)



# Get LOF variants in relevant genes
# These are the expert rules state in table 2 of the report
# Expert rule LoF is grade 2

lofs_association = (
    spark.createDataFrame([
    ('katG', 'Isoniazid'),
    ('pncA', 'Pyrazinamide'),
    ('Rv0678', 'Bedaquiline'),
    ('pepQ', 'Bedaquiline'),
    ('Rv0678', 'Clofazimine'),
    ('pepQ', 'Clofazimine'),
    ('ddn', 'Delamanid'),
    ('fbiA', 'Delamanid'),
    ('fbiB', 'Delamanid'),
    ('fbiC', 'Delamanid'),
    ('fgd1', 'Delamanid'),
    ('Rv2983', 'Delamanid'),
    ('ddn', 'Pretomanid'),
    ('fbiA', 'Pretomanid'),
    ('fbiB', 'Pretomanid'),
    ('fbiC', 'Pretomanid'),
    ('fgd1', 'Pretomanid'),
    ('Rv2983', 'Pretomanid'),
    ('gid', 'Streptomycin'),
    ('ethA', 'Ethionamide'),
    ('ethA', 'Prothionamide'),
    ('tlyA', 'Capreomycin'),
    ], ['resolved_symbol', 'drug_name'])
    .join(
        drug,
        "drug_name",
        "inner"
    )
    .select(
        F.col("resolved_symbol"),
        F.col("drug_id")
    )
)


lofs_variants = (
    var_cat
    .join(
        lofs_association,
        ["resolved_symbol", "drug_id"],
        "inner"
    )
    .where(
        F.col("predicted_effect").isin(
            ["start_lost", "stop_gained", "frameshift", "feature_ablation"]
        )
    )
    .select(
        F.col("variant_id"),
        F.col("variant_catalogue_name"),
        F.col("drug_id"),
        F.lit(2).alias("grade")
    )
    .distinct()   
)


# Getting LoF variants for epistatis rule
epistatis_associations = (
    spark.createDataFrame([
        ('mmpL5', 'Bedaquiline'),
        ('eis', 'Amikacin'),
        ('eis', 'Kanamycin'),
        ], ['resolved_symbol', 'drug_name'])
    .join(
        drug,
        "drug_name",
        "inner"
    )
    .select(
        F.col("resolved_symbol"),
        F.col("drug_id")
    )
)

# Assigning grade 0 for epistatis
# When grouping and selecting min,
# it will overrule all other possible variants
epistatis_variants = (
    var_cat
    .join(
        epistatis_associations,
        ["resolved_symbol", "drug_id"],
        "inner"
    )
    .where(
        F.col("predicted_effect").isin(
            ["start_lost", "stop_gained", "frameshift", "feature_ablation"]
        )
    )
    .select(
        F.col("variant_id"),
        F.col("variant_catalogue_name"),
        F.col("drug_id"),
        F.lit(0).alias("grade")
    )
    .distinct()
)

# rpoB variants
# Everything not already graded, not synonymous and between codon 426 and 452 is grade 2
rpoB_variants = (
    var_cat
    .where(
        (F.col("resolved_symbol")=="rpoB")
        & (F.col("predicted_effect")!="synonymous_variant")
        & (F.col("distance_to_reference")>=426)
        & (F.col("distance_to_reference")<=452)
    )
    .join(
        v2_grading,
        "variant_id",
        "left_anti"
    )
    .select(
        F.col("variant_id"),
        F.col("variant_catalogue_name"),
        F.col("drug_id"),
        F.lit(2).alias("grade")
    )
)

# merging all grades, from variant + rules
# final list
v2_grading_matching = (
    v2_grading
    .join(
        var_cat,
        ["variant_id", "drug_id"],
        "inner"
    )
    .select(
        F.col("variant_id"),
        F.col("variant_catalogue_name"),
        F.col("drug_id"),
        F.col("grade")
    )
    .unionByName(
        lofs_variants
    )
    .unionByName(
        epistatis_variants
    )
    .unionByName(
        rpoB_variants
    )
    .distinct()
)


sample = (
        glueContext.create_data_frame.from_catalog(
        database=glue_db_name,
        table_name=dbname + "_public_submission_sample"
    )
    .withColumnRenamed(
        "id",
        "sample_id"
    )
    .alias("sample")
)

locus_sequencing_stats = (
    glueContext.create_data_frame.from_catalog(
        database=glue_db_name,
        table_name=dbname + "_public_submission_locussequencingstats",
        transformation_ctx = "lss_datasource",
        additional_options = {
            "jobBookmarkKeys": ["id"]
        }
    )
    .alias("locus_sequencing_stats")
)

#selecting genes which are the subject of deletion
#for the LoF rule
deletion_genes = (
    spark.createDataFrame([
    ('katG', 'Isoniazid'),
    ('pncA', 'Pyrazinamide'),
    ('gid', 'Streptomycin'),
    ('ethA', 'Ethionamide'),
    ('ethA', 'Prothionamide'),
    ('tlyA', 'Capreomycin'),
    ], ['resolved_symbol', 'drug_name'])
    .join(
        drug,
        "drug_name",
        "inner"
    )
    .select(
        F.col("resolved_symbol"),
        F.col("drug_id")
    )
)

cov = (
    locus_sequencing_stats
    .join(
        gene_locus_tag,
        "gene_db_crossref_id",
        "inner"
    )
    .join(
        deletion_genes,
        "resolved_symbol",
        "inner"
    )
    .where(
        F.col("coverage_10X")<0.95
    )
    .select(
        F.col("sample_id"),
        F.col("drug_id"),
        F.lit(1).alias("grade"),
        F.concat(F.col("resolved_symbol"), F.lit("_deletion")).alias("variant_catalogue_name")
    )
)

print(cov)


# genotypes_of_interest
genotype = (
    glueContext.create_data_frame.from_catalog(
        database=glue_db_name,
        table_name=dbname + "_public_submission_genotype",
        transformation_ctx = "genotype_datasource",
        additional_options = {
            "jobBookmarkKeys": ["genotype_id"]
        }
    )
    .alias("genotype")
)

df = (
    sample
    .join(
        genotype,
        "sample_id",
        "inner"
    )
    .join(
        v2_grading_matching,
        "variant_id",
        "inner"
    )
    .where(
        F.col("genotyper").isin("freebayes", "delly")
        & (
            ((F.col("alternative_ad").cast("long")/F.col("total_dp").cast("long"))>0.5)
            | ( ~F.col("variant_catalogue_name").startswith("rrs") & ~F.col("variant_catalogue_name").startswith("rrl"))
        )
    )
    .withColumn(
        "maf",
        F.bround(F.col("alternative_ad").cast("long")/F.col("total_dp").cast("long"), 2).cast(StringType())
    )
    .select(
        F.col("sample_id"),
        F.col("drug_id"),
        F.col("grade"),
        F.when(
            F.col("maf")<0.75, F.concat(F.col("variant_catalogue_name"), F.lit(" ("), F.col("maf"), F.lit(")"))
        ).otherwise(F.col("variant_catalogue_name")).alias("variant_catalogue_name"),
    )
    .unionByName(
        cov
    )
    .alias("df1")
)

#grouping by sample and drug and getting the min grade
min_grade = (
    df
    .groupBy(
        "sample_id",
        "drug_id"
    )
    .agg(
        F.min("grade").alias("grade")
    )
    .alias(
        "min_grade"
    )
)

# fetching back the mutation which made the grading
grade_cond = [
    F.col("min_grade.sample_id") == F.col("df1.sample_id"),
    F.col("min_grade.drug_id") == F.col("df1.drug_id"),
    F.col("min_grade.grade").cast(BooleanType()) | ~F.col("df1.grade").cast(BooleanType()),
]

min_grade_mutation = (
    min_grade
    .join(
        df,
        grade_cond,
        how="inner"
    )
    .groupBy(
        "min_grade.sample_id",
        "min_grade.drug_id",
        "min_grade.grade"
    )
    .agg(
        F.concat_ws(
            "; ",
            F.collect_set(F.col("variant_catalogue_name"))
        ).alias("mutation")
    )
)

all_drugs = (
    spark.createDataFrame([
    ('Amikacin',),
    ('Bedaquiline',),
    ('Capreomycin',),
    ('Clofazimine',),
    ('Delamanid',),
    ('Ethambutol',),
    ('Ethionamide',),
    ('Isoniazid',),
    ('Kanamycin',),
    ('Levofloxacin',),
    ('Linezolid',),
    ('Moxifloxacin',),
    ('Pretomanid',),
    ('Prothionamide',),
    ('Pyrazinamide',),
    ('Rifampicin',),
    ('Streptomycin',),
    ], ['drug_name'])
    .join(
        drug,
        "drug_name",
        "inner"
    )
    .select(
        F.col("drug_id")
    )
)


sensible_genes = (
        spark.createDataFrame([
        ('rrs', 'Amikacin'),
        ('rrs', 'Capreomycin'),
        ('rrs', 'Kanamycin'),
        ('rrs', 'Streptomycin'),
        ('rrl', 'Linezolid'),
        ], ['resolved_symbol', 'drug_name']
    )
    .join(
        drug,
        "drug_name",
        "inner"
    )
    .select(
        F.col("resolved_symbol"),
        F.col("drug_id")
    )
)

uncalled = (
    locus_sequencing_stats
    .join(
        sample,
        "sample_id",
        "left_semi"
    )
    .join(
        gene_locus_tag,
        "gene_db_crossref_id",
        "inner"
    )
    .join(
        sensible_genes,
        "resolved_symbol",
        "inner"
    )
    .where(
        (F.col("coverage_10X")<0.95) & (F.col("mean_depth")<15)
    )
    .select(
        F.col("sample_id"),
        F.col("resolved_symbol"),
        F.col("drug_id"),
        F.col("coverage_10X")
    )
    .distinct()
)

finale = (
    locus_sequencing_stats
    .alias("sample")
    .select(
        F.col("sample_id"),
    )
    .distinct()
    .crossJoin(
        all_drugs
    )
    .join(
        min_grade_mutation,
        ["sample_id", "drug_id"],
        "left"
    )
    .withColumn(
        "resistance_prediction",
        F.when(F.col("grade").isin([1, 2]), "R").otherwise("S")
    )
    .join(
        uncalled,
        ["sample_id", "drug_id"],
        "left",
    )
    .select(
        F.col("sample_id"),
        F.col("drug_id"),
        F.lit(2).alias("version"),
        F.when(
            ~F.col("coverage_10X").isNull() & (F.col("resistance_prediction")!="R"), "U")
            .otherwise(F.col("resistance_prediction"))
        .alias("resistance_flag"),
        F.when(F.col("grade").isin([0,1,2]), F.col("mutation")).otherwise("").alias("variant")
    )
)


result_dynamic_frame = (
    DynamicFrame.fromDF(
        finale,
        glueContext,
        "final"
    )
    .resolveChoice(
        choice="match_catalog",
        database=glue_db_name,
        table_name=dbname+"_public_submission_genotyperesistance",
        transformation_ctx="resolve_choice"
    )
)

datasink5 = (
    glueContext.write_dynamic_frame.from_catalog(
        frame=result_dynamic_frame,
        database=glue_db_name,
        table_name=dbname+"_public_submission_genotyperesistance",
        transformation_ctx="datasink5"
    )
)

job.commit()