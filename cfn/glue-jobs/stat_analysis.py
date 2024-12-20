import sys, boto3, os, io, pandas, datetime
from scipy import stats

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SQLContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.window import Window
import pyspark.sql.functions as F

from phenotypic_data_views import *
from biosql_gene_views import *
from variant_annotation_categorization import *
from variant_classification_algorithms import *
from neutral_variants import *

args = getResolvedOptions(sys.argv, ['JOB_NAME', "glue_database_name", "extraction", "postgres_db_name"])

glueContext = GlueContext(SparkContext.getOrCreate())

spark = glueContext.spark_session

spark._jsc.hadoopConfiguration().set('spark.hadoop.fs.s3.maxConnections', '1000')

job = Job(glueContext)

d = datetime.datetime.now().isoformat()

phenotypes = glueContext.create_data_frame.from_catalog(database = args["glue_database_name"], table_name = f"{args['postgres_db_name']}_genphensql_phenotypic_drug_susceptibility_test").alias("phenotypes")

phenotypes_category = glueContext.create_data_frame.from_catalog(database = args["glue_database_name"], table_name = f"{args['postgres_db_name']}_genphensql_phenotypic_drug_susceptibility_test_category").alias("phenotypes_category")

tier = glueContext.create_data_frame.from_catalog(database = args["glue_database_name"], table_name = f"{args['postgres_db_name']}_public_genphen_genedrugresistanceassociation").alias("tier")

d = datetime.datetime.now().isoformat()

# Select samples based on their phenotype 
# Only keep clean samples (ie all results either R or S)
# For neutral mutation identification we only keep WHO current & past categories
clean_phenotypes = (
    join_phenotypes_with_categories(phenotypes, phenotypes_category)
    .where(
        F.col("phenotypes_category.category").isin(
            ["WHO_current", "WHO_past", "WHO_undefined"]
        )
    )
    .withColumn("rank",
        F.rank().over(
            Window.partitionBy(
                F.col("sample_id"),
                F.col("phenotypes.drug_id")
            )
            .orderBy(
                F.when(F.col("category")=="WHO_current", 1)
                .when(F.col("category")=="WHO_past", 2)
                .when(F.col("category")=="WHO_undefined", 3)
            )
        )
    )
    .where(F.col("rank")==1)
    .groupBy(
        F.col("phenotypes.sample_id"),
        F.col("phenotypes.drug_id"),
        F.col("phenotypes_category.category").alias("phenotype_category"),
    )
    .agg(
        F.concat_ws("", F.collect_set(F.col("phenotypes.test_result"))).alias("concat_results")
    )
    .where(F.col("concat_results").rlike("^(R+|S+)$"))
    .select(
        F.col("phenotypes.sample_id"),
        F.col("phenotypes.drug_id"),
        F.col("phenotype_category"),
        F.col("concat_results").substr(1, 1).alias("phenotype"),
    )
    .alias("clean_phenotypes")
)


drug = glueContext.create_data_frame.from_catalog(database = args["glue_database_name"], table_name = f"{args['postgres_db_name']}_public_genphen_drug").alias("drug")

# According to the instructions, we pool ofloxacin tests with levofloxacin and prothionamide with ethionamide
clean_phenotypes = (
    clean_phenotypes
    .join(
        drug.alias("original"),
        on="drug_id",
        how="inner"
    )
    .join(
        drug.alias("corrected"),
        on=((F.col("original.drug_name")=="Ofloxacin") & (F.col("corrected.drug_name")=="Levofloxacin"))
            | ((F.col("original.drug_name")=="Prothionamide") & (F.col("corrected.drug_name")=="Ethionamide")),
        how="left"
    )
    .withColumn(
        "final_drug_id",
        F.coalesce(
            F.col("corrected.drug_id"),
            F.col("clean_phenotypes.drug_id"),
        )
    )
    .select(
        F.col("sample_id"),
        F.col("final_drug_id").alias("drug_id"),
        F.col("phenotype_category"),
        F.col("phenotype")
    )
)

sample = glueContext.create_data_frame.from_catalog(database = args["glue_database_name"], table_name = f"{args['postgres_db_name']}_genphensql_sample")

summary_stats = glueContext.create_data_frame.from_catalog(database = args["glue_database_name"], table_name = f"{args['postgres_db_name']}_public_submission_summarysequencingstats")

seq_data = glueContext.create_data_frame.from_catalog(database = args["glue_database_name"], table_name = f"{args['postgres_db_name']}_genphensql_sequencing_data")

filtered_samples = (
    sample
    .join(clean_phenotypes,
        on="sample_id",
        how="inner")
    .join(summary_stats,
        on="sample_id",
        how="inner")
    .join(seq_data,
        on="sample_id",
        how="inner")
    .where(
        (F.col("sequencing_platform")=="ILLUMINA") 
        & (F.col("library_preparation_strategy")=="WGS")
        & (F.col("median_depth")>15)
        & (F.col("coverage_20x")>0.95)
    )
    .select(
        F.col("sample_id"),
        F.col("drug_id"),
    )
    .distinct()
    .alias("filtered_samples")
)

glueContext.write_dynamic_frame.from_options(
    frame=DynamicFrame.fromDF(
        clean_phenotypes
            .join(
                filtered_samples,
                on=["sample_id", "drug_id"],
                how="left_semi"
            )
            .join(
                drug,
                on="drug_id",
                how="inner"
            )
            .withColumn(
                "phenotype_category",
                F.when(F.col("phenotype_category").isin(["WHO_current", "WHO_past"]), F.lit("WHO"))
                .otherwise(F.lit("ALL"))
            )
            .drop("drug_id"),
        glueContext,
        "final"),
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "s3://aws-glue-assets-231447170434-us-east-1/"+args["JOB_NAME"]+"/"+d+"_"+args["JOB_RUN_ID"]+"/phenotypes/",
        "partitionKeys": ["drug_name"],
    }
)

dbxref = glueContext.create_data_frame.from_catalog(database = args["glue_database_name"], table_name = "postgres_biosql_dbxref")

sqv = glueContext.create_data_frame.from_catalog(database = args["glue_database_name"], table_name = "postgres_biosql_seqfeature_qualifier_value")

sdc = glueContext.create_data_frame.from_catalog(database = args["glue_database_name"], table_name = "postgres_biosql_seqfeature_dbxref")

protein_id = protein_id_view(dbxref, sqv, sdc).alias("protein_id")

annot = glueContext.create_data_frame.from_catalog(database = args["glue_database_name"], table_name = f"{args['postgres_db_name']}_public_genphen_annotation")

vta = glueContext.create_data_frame.from_catalog(database = args["glue_database_name"], table_name = f"{args['postgres_db_name']}_public_genphen_varianttoannotation")

fapg = formatted_annotation_per_gene(vta, annot, dbxref, protein_id).alias("fapg")

promoter_distance = glueContext.create_data_frame.from_catalog(database = args["glue_database_name"], table_name = f"{args['postgres_db_name']}_genphensql_promoter_distance")

tier = glueContext.create_data_frame.from_catalog(database = args["glue_database_name"], table_name = f"{args['postgres_db_name']}_public_genphen_genedrugresistanceassociation").alias("tier")

variant = glueContext.create_data_frame.from_catalog(database = args["glue_database_name"], table_name = f"{args['postgres_db_name']}_public_genphen_variant")

mvd = multiple_variant_decomposition(variant).alias("mvd")

variant_category = tiered_drug_variant_categories(fapg, tier, variant, mvd, promoter_distance).alias("variant_category")

genotype = glueContext.create_data_frame.from_catalog(database = args["glue_database_name"], table_name = f"{args['postgres_db_name']}_public_submission_genotype").alias("genotype")

additional_variant_information = glueContext.create_data_frame.from_catalog(database = args["glue_database_name"], table_name = f"{args['postgres_db_name']}_genphensql_additional_variant_information").alias("additional_info").where(F.col("description")=="merker_neutral_variant")

seqfeature = glueContext.create_data_frame.from_catalog(database = args["glue_database_name"], table_name = "postgres_biosql_seqfeature").alias("seqfeature")

term = glueContext.create_data_frame.from_catalog(database = args["glue_database_name"], table_name = "postgres_biosql_term").alias("term1")

gene_name = gene_or_locus_tag_view(sdc, sqv, seqfeature, term, "gene_symbol").alias("gene")

clean_phenotypes_neutral = (
    clean_phenotypes
    .where(
        F.col("phenotype_category").isin(["WHO_current", "WHO_past"])
    )
)

filtered_samples_neutral = (
    filtered_samples
    .join(
        clean_phenotypes_neutral,
        on=["sample_id", "drug_id"],
        how="left_semi"
    )
)

neutral_variants = get_neutral_variants(filtered_samples_neutral, clean_phenotypes_neutral, variant_category, genotype, 0.1, additional_variant_information, drug, gene_name)

# First getting all (genotypes, category) of filtered samples

def simple_extraction():
    genotype_categorized = (
        genotype
        .where(
            genotype.genotyper.isin(['freebayes', 'delly'])
            & (genotype.quality>1000)
            & (genotype.total_dp>10)
        )
        .join(
            variant_category,
            on="variant_id",
            how="inner"
        )
        .join(
            other=filtered_samples,
            on=["sample_id", "drug_id"],
            how="left_semi",
        )
        .withColumn(
            "af", 
            F.bround(genotype.alternative_ad.cast("long")/genotype.total_dp.cast("long"), 2)
        )
        .where(
            F.col("af")>0.25
        )
    )

    position = most_frequent_position_by_category(genotype_categorized)

    final_results = (
        filtered_samples
        .join(
            genotype_categorized,
            on=["sample_id", "drug_id"],
            how="inner"
        )
        .groupBy(
            F.col("filtered_samples.sample_id"),
            F.col("drug_id"),
            F.col("tier"),
            F.col("gene_db_crossref_id"),
            F.col("variant_category"),
        )
        .agg(
            {"af": "max"}
        )
        .alias("final_results")
    )


    final_results = (
        final_results
        .join(
            neutral_variants[1]
                .select(
                    F.col("gene_db_crossref_id"),
                    F.col("variant_category"),
                    F.col("drug_id"),
                    F.lit(True).alias("neutral")
                )
                .distinct(),
            on=["gene_db_crossref_id", "variant_category", "drug_id"],
            how="left"
        )
        .join(
            position,
            on=["gene_db_crossref_id", "variant_category"],
            how="left"
        )
    )

    locus_tag = gene_or_locus_tag_view(sdc, sqv, seqfeature, term, "rv_symbol").alias("locus_tag")

    gene_locus_tag = merge_gene_locus_view(gene_name, locus_tag).alias("gene_locus_tag")

    glueContext.write_dynamic_frame.from_options(
        frame=DynamicFrame.fromDF(
            final_results
            .join(
                drug,
                on="drug_id",
                how="inner"
            )
            .join(
                gene_locus_tag,
                on="gene_db_crossref_id",
                how="inner"
            )
            .select(
                F.col("drug_name"),
                F.col("final_results.sample_id"),
                F.col("tier"),
                F.col("resolved_symbol"),
                F.col("variant_category"),
                F.col("Effect"),
                F.col("neutral"),
                F.col("max(af)")
            )
            .repartition("drug_name", "tier"),
            glueContext,
            "final"
        ),
        connection_type="s3",
        format="csv",
        connection_options={
            "path": "s3://aws-glue-assets-231447170434-us-east-1/"+args["JOB_NAME"]+"/"+d+"_"+args["JOB_RUN_ID"]+"/full_genotypes/",
            "partitionKeys": ["drug_name", "tier"]
        }
    )


def complex_extraction():
    genotype_categorized = (
        genotype
        .where(
            genotype.genotyper.isin(['freebayes', 'delly'])
            & (genotype.quality>1000)
            & (genotype.total_dp>10)
        )
        .join(
            variant_category,
            on="variant_id",
            how="inner"
        )
        .join(
            other=filtered_samples,
            on=["sample_id", "drug_id"],
            how="left_semi",
        )
        .withColumn(
            "af", 
            F.bround(genotype.alternative_ad.cast("long")/genotype.total_dp.cast("long"), 2)
        )
        .where(
            F.col("af")>0.25
        )
    )

    position = most_frequent_position_by_category(genotype_categorized)

    relevant_variant = (
        genotype_categorized
        .select("variant_id")
        .distinct()
    )

    final_results = (
        filtered_samples
        .join(
            variant_category
            .join(relevant_variant,
                on="variant_id",
                how="left_semi"),
            on="drug_id",
            how="inner"
        )
        .join(
            genotype_categorized.select(F.col("variant_id"), F.col("sample_id"), F.col("af"), F.col("total_dp")),
            on=["sample_id", "variant_id"],
            how="left"
        )
        .groupBy(
            F.col("filtered_samples.sample_id"),
            F.col("gene_db_crossref_id"),
            F.col("drug_id"),
            F.col("tier"),
            F.col("variant_category"),
        )
        .agg(
            {"af": "max",
            "total_dp": "max"}
        )
        .alias("final_results")
    )

    deletions = (
        final_results
        .where(
            (F.col("variant_category")=="deletion") & (F.col("max(af)")>0.75)
        )
        .select(
            F.col("sample_id"),
            F.col("gene_db_crossref_id"),
            F.lit(1).alias("deleted"),
        )
        .distinct()
    )

    loc_seq_stats = glueContext.create_dynamic_frame.from_catalog(database = args["glue_database_name"], table_name = f"{args['postgres_db_name']}_public_submission_locussequencingstats").toDF().alias("loc_seq_stats")

    final_results = (
        final_results
        .join(
            loc_seq_stats,
            F.col("max(af)").isNull()
            & (loc_seq_stats.sample_id==final_results.sample_id)
            & (loc_seq_stats.gene_db_crossref_id==final_results.gene_db_crossref_id),
            how="left"
        )
        .join(
            deletions.alias("deletion"),
            on=(F.col("deletion.sample_id")==F.col("final_results.sample_id"))
            & (F.col("deletion.gene_db_crossref_id")==F.col("final_results.gene_db_crossref_id"))
            & (F.col("variant_category")!="deletion"),
            how="left"
        )
        .withColumn(
            "variant_allele_frequency",
            F.when(
                    (F.col("coverage_10x")>0.99)
                    | (F.col("max(af)")<0.25)
                    | F.col("deleted").isNotNull(),
                    F.lit(0))
            .otherwise(F.col("max(af)"))
        )
        .withColumn(
            "variant_binary_status",
            F.when(F.col("max(af)")>0.75, F.lit(1))
            .when(
                (F.col("max(af)")<0.25) 
                | (F.col("coverage_10x")>0.99) 
                | F.col("deleted").isNotNull(),
                F.lit(0)
            )
        )
        .drop(
            "max(af)",
            "max(total_dp)",
            "locus_seq_stats.gene_db_crossref_id"
        )
    )

    final_results = (
        final_results
        .join(
            neutral_variants[1]
                .select(
                    F.col("gene_db_crossref_id"),
                    F.col("variant_category"),
                    F.col("drug_id"),
                    F.lit(True).alias("neutral")
                )
                .distinct(),
            on=["gene_db_crossref_id", "variant_category", "drug_id"],
            how="left"
        )
        .join(
            position,
            on=["gene_db_crossref_id", "variant_category"],
            how="left"
        )
    )

    locus_tag = gene_or_locus_tag_view(sdc, sqv, seqfeature, term, "rv_symbol").alias("locus_tag")

    gene_locus_tag = merge_gene_locus_view(gene_name, locus_tag).alias("gene_locus_tag")

    glueContext.write_dynamic_frame.from_options(
        frame=DynamicFrame.fromDF(
            final_results
            .join(
                drug,
                on="drug_id",
                how="inner"
            )
            .join(
                gene_locus_tag,
                on="gene_db_crossref_id",
                how="inner"
            )
            .select(
                F.col("drug_name"),
                F.col("final_results.sample_id"),
                F.col("tier"),
                F.col("resolved_symbol"),
                F.col("variant_category"),
                F.col("Effect"),
                F.col("neutral"),
                F.col("variant_allele_frequency"),
                F.col("variant_binary_status"),
            ),
            glueContext,
            "final"
        ),
        connection_type="s3",
        format="csv",
        connection_options={
            "path": "s3://aws-glue-assets-231447170434-us-east-1/"+args["JOB_NAME"]+"/"+d+"_"+args["JOB_RUN_ID"]+"/full_genotypes/",
            "partitionKeys": ["drug_name", "tier"]
        }
    )

if args["extraction"]=="simple":
    simple_extraction()
elif args["extraction"]=="complex":
    complex_extraction()