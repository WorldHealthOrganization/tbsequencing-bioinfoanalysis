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
# from variant_classification_algorithms import *
# from neutral_variants import *
# from genetic_relatedness_matrix import *

# def farhat_lab_extraction(genotype_categorized, most_frequent_position, all_position, filtered_sample_drug, variant_category, loc_seq_stats, sample, drug, gene_locus_tag):

#     # getting the complete list of (genomic-variants, drug) pairs
#     relevant_variant = (
#         genotype_categorized
#         .select(
#             F.col("variant_id"),
#             F.col("drug_id"),
#         )
#         .distinct()
#     )

#     # We do a left join on genotypes instead of an inner join
#     final_results = (
#         filtered_sample_drug
#         .join(
#             variant_category
#             .join(
#                 relevant_variant,
#                 on=["variant_id", "drug_id"],
#                 how="left_semi"
#             ),
#             on="drug_id",
#             how="inner"
#         )
#         .join(
#             genotype_categorized.select(
#                 F.col("variant_id"),
#                 F.col("sample_id"),
#                 F.col("af"),
#                 F.col("total_dp")
#             ),
#             on=["sample_id", "variant_id"],
#             how="left"
#         )
#         .groupBy(
#             F.col("filtered_samples.sample_id"),
#             F.col("gene_db_crossref_id"),
#             F.col("drug_id"),
#             F.col("tier"),
#             F.col("variant_category"),
#         )
#         .agg(
#             {"af": "max",
#             "total_dp": "max"}
#         )
#         .alias("final_results")
#     )

#     # identifying genes that have well identified deletions so that they don't count as sequencing defects.
#     deletions = (
#         final_results
#         .where(
#             F.col("variant_category").isin("deletion", "frameshift", "inframe_deletion")
#         )
#         .select(
#             F.col("sample_id"),
#             F.col("gene_db_crossref_id"),
#         )
#         .distinct()
#     )

#     # narrowing down the samples which have at least one sequencing defect
#     missing_sample_names = (
#         loc_seq_stats.alias("loc_seq_stats")
#         .join(
#             sample.select(
#                 F.col("sample_id"),
#                 F.col("sample_name"),
#             ),
#             on="sample_id",
#             how="inner"
#         )
#         .join(
#             tier,
#             on="gene_db_crossref_id",
#             how="inner"
#         )
#         .join(
#             filtered_sample_drug,
#             on=["sample_id", "drug_id"],
#             how="left_semi"
#         )
#         # Discard deleted genes. They do not qualify for sequencing defects.
#         .join(
#             deletions.alias("deletion"),
#             on=(F.col("deletion.sample_id")==F.col("loc_seq_stats.sample_id"))
#             & (F.col("deletion.gene_db_crossref_id")==F.col("loc_seq_stats.gene_db_crossref_id")),
#             how="left_anti"
#         )
#         .where(
#             F.col("coverage_10x")!=1
#         )
#         .select(
#             F.col("sample_name")
#         )
#         .distinct()
#         .collect()
#     )

#     s_names = [x["sample_name"] for x in missing_sample_names]

#     # using a push down predicate from glue so that we fetch from S3 the position specific coverage data
#     coverage = (
#         glueContext.create_dynamic_frame.from_catalog(
#             database = args["glue_database_name"],
#             table_name = "coverage_cb2f8fa7182317fd41dd87a668ced0a3",
#             push_down_predicate="sample_name IN ('" + "','".join(s_names)+"')",
#         )
#         .toDF()
#         .where(
#             F.col("depth")<10
#         )
#     )

#     missing_variant_categories = (
#         coverage
#         .join(
#             most_frequent_position,
#             on="position",
#             how="left"
#         )
#         .join(
#             sample.select(
#                 F.col("sample_id"),
#                 F.col("sample_name"),
#             ),
#             on="sample_name",
#             how="inner"
#         )
#         .select(
#             F.col("sample_id"),
#             F.col("gene_db_crossref_id"),
#             F.col("variant_category"),
#             F.lit(True).alias("missing"),
#         )
#     )

#     # Extracting locus-sample pairs with lack of coverage and fine graining the data
#     final_results = (
#         final_results
#         .join(
#             missing_variant_categories,
#             on=["sample_id", "gene_db_crossref_id", "variant_category"],
#             how="left"
#         )
#         .withColumn(
#             "variant_allele_frequency",
#             F.when(F.col("missing")==True, F.lit(None))
#             .when(F.col("max(af)")<0.25, F.lit(0))
#             .when(F.col("max(af)").isNotNull(), F.col("max(af)"))
#             .otherwise(F.lit(0))
#         )
#         .withColumn(
#             "variant_binary_status",
#             F.when(F.col("missing")==True, F.lit(None))
#             .when(F.col("max(af)")>0.75, F.lit(1))
#             .when(F.col("max(af)")<0.25, F.lit(0))
#             .otherwise(F.lit(0))
#         )
#         .drop(
#             "max(af)",
#             "max(total_dp)",
#             "locus_seq_stats.gene_db_crossref_id"
#         )
#     )

#     final_results = (
#         final_results
#         .join(
#             all_position,
#             on=["gene_db_crossref_id", "variant_category"],
#             how="left"
#         )
#     )

#     glueContext.write_dynamic_frame.from_options(
#         frame=DynamicFrame.fromDF(
#             final_results
#             .join(
#                 drug,
#                 on="drug_id",
#                 how="inner"
#             )
#             .join(
#                 gene_locus_tag,
#                 on="gene_db_crossref_id",
#                 how="inner"
#             )
#             .select(
#                 F.col("drug_name"),
#                 F.col("final_results.sample_id"),
#                 F.col("tier"),
#                 F.col("resolved_symbol"),
#                 F.col("variant_category"),
#                 F.col("predicted_effect"),
#                 F.lit(None).alias("neutral"),
#                 F.col("variant_allele_frequency"),
#                 F.col("variant_binary_status"),
#                 F.col("position"),
#             ),
#             glueContext,
#             "final"
#         ),
#         connection_type="s3",
#         format="csv",
#         connection_options={
#             "path": "s3://aws-glue-assets-231447170434-us-east-1/"+args["JOB_NAME"]+"/"+args["extraction"].strip("/").strip("")+"/extraction_currently_running/"+d+"_"+args["JOB_RUN_ID"]+"/full_genotypes/",
#             "partitionKeys": ["drug_name", "tier"]
#         }
#     )

def twalker_extraction(genotype_categorized, position, filtered_sample_drug, loc_seq_stats, drug, gene_locus_tag, tier, orphan=False):

    final_results = (
        filtered_sample_drug.alias("filtered_samples")
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
            {
                "af": "max",
                "quality": "max"
            }
        )
        .alias("final_results")
    )

    if not orphan:

        output_folder = "full_genotypes"
        partition = ["drug_name", "tier"]
        # We identify genes that are deleted so that we don't consider them as sequencing failures later on
        # We can't know which frameshift are deletion or insertion here, for the moment
        # So we exclude everything... Ideally we should only exclude deletions that lead to frameshifts
        deletions = (
            genotype_categorized
            .where(
                F.col("predicted_effect").isin("deletion", "frameshift", "inframe_deletion")
            )
        )

        # For uncovered (sample, gene pairs), we add rows with empty variant_category & predicted effect values into the final data
        missing = (
            filtered_sample_drug
            .join(
                tier,
                "drug_id",
                "inner"
            )
            .join(
                loc_seq_stats,
                on=["gene_db_crossref_id", "sample_id"],
                how="inner"
            )
            .join(
                deletions,
                on=["sample_id", "gene_db_crossref_id"],
                how="left_anti"
            )
            .where(
                F.col("coverage_10x")<1
            )
        )

        uncovered = (
            missing
            .select(
                F.col("drug_id"),
                F.col("sample_id"),
                F.col("tier"),
                F.col("gene_db_crossref_id"),
                F.lit(None).alias("variant_category"),
                F.lit(None).alias("max(af)"),
                F.lit(None).alias("max(quality)")
            )
        )

        sample_drug_tiers_missing = (
            missing
            .select(
                F.col("drug_id"),
                F.col("sample_id"),
                F.col("tier"),
            )
            .distinct()
        )

        glueContext.write_dynamic_frame.from_options(
            frame=DynamicFrame.fromDF(
                sample_drug_tiers_missing
                .join(
                    drug,
                    on="drug_id",
                    how="inner"
                )
                .select(
                    F.col("drug_name"),
                    F.col("sample_id"),
                    F.col("tier"),
                )
                .repartition("drug_name"),
                glueContext,
                "final"
            ),
            connection_type="s3",
            format="csv",
            connection_options={
                "path": "s3://aws-glue-assets-231447170434-us-east-1/"+args["JOB_NAME"]+"/"+args["extraction"].strip("/").strip("")+"/extraction_currently_running/"+d+"_"+args["JOB_RUN_ID"]+"/missing_sample_drug_tier/",
                "partitionKeys": ["drug_name", "tier"]
            }
        )


        final_results = (
            final_results
            .unionByName(
                uncovered
            )
        )


    else:
        output_folder = "orphan_genotypes"
        partition = []

    final_results = (
        final_results
        .join(
            position,
            on=["gene_db_crossref_id", "variant_category"],
            how="left"
        )
        .select(
            F.col("drug_id"),
            F.col("sample_id"),
            F.col("tier"),
            F.col("gene_db_crossref_id"),
            F.col("variant_category"),
            F.col("predicted_effect"),
            F.col("max(af)"),
            F.col("max(quality)"),
            F.col("position"),
        )
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
            F.col("predicted_effect"),
            F.lit(None).alias("neutral"),
            F.col("max(af)"),
            F.col("max(quality)"),
            F.col("position"),
        )
    )

    if orphan:
        final_results = (
            final_results
            .coalesce(1)
        )
    else:
        final_results = (
            final_results
            .repartition("drug_name", "tier")
        )

    glueContext.write_dynamic_frame.from_options(
        frame=DynamicFrame.fromDF(
            final_results,
            glueContext,
            "final"
        ),
        connection_type="s3",
        format="csv",
        connection_options={
            "path": "s3://aws-glue-assets-231447170434-us-east-1/"+args["JOB_NAME"]+"/"+args["extraction"].strip("/").strip("")+"/extraction_currently_running/"+d+"_"+args["JOB_RUN_ID"]+"/"+output_folder+"/",
            "partitionKeys": partition
        }
    )



if __name__=="__main__":

    d = datetime.datetime.now().isoformat()

    args = getResolvedOptions(sys.argv, ['JOB_NAME', "postgres_db_name", "glue_db_name", "sample_fraction", "unpool_frameshifts", "log_s3_bucket"])

    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session
    spark._jsc.hadoopConfiguration().set('spark.sql.broadcastTimeout', '3600')
    job = Job(glueContext)

    bucket = args["log_s3_bucket"].split("s3://")[1].strip("/")

    dbname = args["postgres_db_name"]
    glue_dbname = args["glue_db_name"]

    data_frame = {}
    tables = { 
            "biosql": ["dbxref", "seqfeature_qualifier_value", "seqfeature_dbxref", "location", "seqfeature", "term"],
            "public": {
                "genphen":  [
                    "pdstestcategory",
                    "drug",
                    "growthmedium",
                    "pdsassessmentmethod",
                    "epidemcutoffvalue",
                    "genedrugresistanceassociation",
                    "microdilutionplateconcentration",
                    "annotation",
                    "varianttoannotation",
                    "promoterdistance",
                    "variant",
                    "sample",
                    "summarysequencingstats",
                    "sequencingdata",
                    ],
                "submission" : [
                    "pdstest",
                    "mictest",
                    "genotype",
                    "locussequencingstats"
                ]
            }
    }


    # country = glueContext.create_data_frame.from_catalog(database = glue_dbname, table_name = "postgres_public_country")

    # dataset = glueContext.create_data_frame.from_catalog(database = glue_dbname, table_name = "postgres_genphensql_dataset")

    # dataset_to_sample = glueContext.create_data_frame.from_catalog(database = glue_dbname, table_name = "postgres_genphensql_dataset_to_sample")


    for schema in tables.keys():
        if isinstance(tables[schema], list):
            for table in tables[schema]:
                tname = dbname + "_biosql_" + table
                print(tname)
                data_frame[table] = glueContext.create_data_frame.from_catalog(
                    database = glue_dbname,
                    table_name = tname
                )
        elif isinstance(tables[schema], dict):
            for app in tables[schema].keys():
                for table in tables[schema][app]:
                    tname = dbname + "_" + schema + "_" + app + "_" + table
                    print(tname)
                    data_frame[table] = glueContext.create_data_frame.from_catalog(
                        database = glue_dbname,
                        table_name = tname
                    )
        else: 
            raise ValueError

    # cryptic_qual = (
    #     glueContext.create_data_frame.from_catalog(database = glue_dbname, table_name = "cryptic_low_quality")
    #     .select(
    #         F.col("uniqueid").alias("sample_id"),
    #         F.col("`attribute.1`").alias("drug_id"),
    #     )
    #     .where(
    #         F.col("quality")=="LOW"
    #     )
    #     .alias("cryptic_quals")
    # )

    # print(mic.count())

    # mic = (
    #     mic
    #     # .join(
    #     #     cryptic_qual,
    #     #     on=(F.col("cryptic_quals.sample_id")==F.col("mic.sample_id")) & (F.col("cryptic_quals.drug_id")==F.col("mic.drug_id")) & F.col("mic.plate").startswith("UKMYC"),
    #     #     how="left_anti",
    #     # )
    # )

    # print(mic.count())

    clean_phenotypes = preparing_binary_data_for_final_algorithm(
        data_frame["pdstest"],
        data_frame["pdstestcategory"], 
        data_frame["drug"],
        data_frame["growthmedium"],
        data_frame["pdsassessmentmethod"],
        data_frame["mictest"],
        data_frame["epidemcutoffvalue"],
        data_frame["genedrugresistanceassociation"],
        data_frame["microdilutionplateconcentration"]
    )


    final_bin_mic_cc, final_bin_mic_cc_atu = preparing_cc_cc_atu_data_for_final_algorithm(
        data_frame["mictest"],
        data_frame["drug"],
        data_frame["microdilutionplateconcentration"],
        data_frame["epidemcutoffvalue"],
        data_frame["genedrugresistanceassociation"]
    )

    all_phenotypes = (
        clean_phenotypes
        .unionByName(final_bin_mic_cc)
        .unionByName(final_bin_mic_cc_atu)
    )


    all_samples_with_pheno = (
        data_frame["sample"]
        .join(
            all_phenotypes,
            on="sample_id",
            how="inner"
        )
        .select(
            F.col("sample_id")
        )
    )

    samples_with_sequencing = (
        all_samples_with_pheno
        .join(
            data_frame["sequencingdata"],
            on="sample_id",
            how="inner"
        )
        .where(
            (F.col("sequencing_platform")=="ILLUMINA") 
            & (F.col("library_preparation_strategy")=="WGS")
        )
        .select(
            F.col("sample_id"),
        )
        .distinct()
    )

    filtered_samples = (
        samples_with_sequencing
        .join(
            data_frame["summarysequencingstats"],
            on="sample_id",
            how="inner"
        )
        .where(
            (F.col("median_depth")>15)
            & (F.col("coverage_20x")>0.95)
        )
        .select(
            F.col("sample_id"),
        )
        .distinct()
        .alias("filtered_samples")
    )

    filtered_samples_drug_phenotypes = (
        filtered_samples
        .join(
            all_phenotypes,
            on="sample_id",
            how="inner"
        )
        .select(
            F.col("sample_id"),
            F.col("drug_id"),
            F.col("phenotypic_category"),
            F.col("phenotype"),
        )
        .alias("filtered_samples")
    )

    filtered_samples_drug_phenotypes_box = (
        filtered_samples_drug_phenotypes
        .join(
            data_frame["drug"],
            on="drug_id",
            how="inner"
        )
        .withColumn(
            "box",
            F.col("phenotypic_category")
        )
        .withColumn(
            "phenotypic_category",
            F.when(
                F.col("phenotypic_category").isin("WHO_current", "non_WHO_CC_SR", "WHO_past"),
                F.lit("WHO")
            )
            .when(
                F.col("phenotypic_category").isin("WHO_undefined", "non_WHO_CC_R", "non_WHO_CC_S", "CRyPTIC_MIC", "MYCOTB_MIC"),
                F.lit("ALL")
            )
            .when(
                F.col("phenotypic_category").rlike("^(CC-ATU|CC).*"),
                F.regexp_extract(
                    F.col("phenotypic_category"),
                    r'^(CC-ATU|CC).*',
                    1
                )
            )
            .otherwise(F.col("phenotypic_category"))
        )
        .drop(
            "drug_id"
        )
    )

    glueContext.write_dynamic_frame.from_options(
        frame=DynamicFrame.fromDF(
            filtered_samples_drug_phenotypes_box,
            glueContext,
            "final"
        ),
        connection_type="s3",
        format="csv",
        connection_options={
            "path": "s3://aws-glue-assets-231447170434-us-east-1/"+args["JOB_NAME"]+"/"+args["extraction"].strip("").strip("/")+"/extraction_currently_running/"+d+"_"+args["JOB_RUN_ID"]+"/phenotypes/",
            "partitionKeys": ["drug_name"],
        }
    )


    order_categories = {'WHO': 1, "not-WHO":2, 'ALL': 3, 'CC': 4, 'CC-ATU':5}

    sort_col_categories = F.create_map([F.lit(x) for x in chain(*order_categories.items())])[F.col('phenotypic_category')]

    drug_sample_overview_intermediate = (
        filtered_samples_drug_phenotypes_box
        .groupby(
            F.col("phenotypic_category"),
            F.col("drug_name"),
            F.col("phenotype")
        )
        .agg(
            F.countDistinct("sample_id").alias("count")
        )
        .select(
            F.col("drug_name"),
            F.col("phenotype"),
            F.when(F.col("phenotypic_category")=="ALL", "not-WHO").otherwise(F.col("phenotypic_category")).alias("phenotypic_category"),
            F.col("count")
        )
    )

    sum_all_drug_sample_overview = (
        drug_sample_overview_intermediate
        .where(
            F.col("phenotypic_category").isin("WHO", "not-WHO")
        )
        .groupby(
            F.col("drug_name"),
            F.col("phenotype")
        )
        .agg(
            F.sum(F.col("count")).alias("count")
        )
        .select(
            F.col("drug_name"),
            F.col("phenotype"),
            F.col("count"),
            F.lit("ALL").alias("phenotypic_category")
        )
    )

    drug_sample_overview = (
        drug_sample_overview_intermediate
        .unionByName(
            sum_all_drug_sample_overview
        )
        .groupby(
            F.col("drug_name"),
            F.col("phenotypic_category"),
        )
        .pivot(
            "phenotype"
        )
        .sum("count")
        .sort(
            F.col("drug_name"),
            sort_col_categories,
        )
        .withColumn(
            "total",
            F.col("R")+F.col("S")
        )
        .withColumn(
            "res_percentage",
            F.col("R")/F.col("total")
        )
        .withColumn(
            "95 interval lower",
            100*(F.col("res_percentage")-1.96*F.sqrt((F.col("res_percentage")*(F.lit(1)-F.col("res_percentage")))/F.col("total")))
        )
        .withColumn(
            "95 interval upper",
            100*(F.col("res_percentage")+1.96*F.sqrt((F.col("res_percentage")*(F.lit(1)-F.col("res_percentage")))/F.col("total")))
        )
        .withColumn(
            "res_percentage",
            F.col("res_percentage")*100
        )
    )


    # lineages = glueContext.create_data_frame.from_catalog(database = glue_dbname, table_name = "lineages")

    # lineages.show()

    # samples_per_country = (
    #     filtered_samples_drug_phenotypes_box
    #     .join(
    #         sample,
    #         on="sample_id",
    #         how="inner",
    #     )
    #     .join(
    #         country,
    #         on="country_id",
    #         how="left"
    #     )
    #     .groupby(
    #         F.col("country_usual_name"),
    #         F.col("three_letters_code"),
    #     )
    #     .agg(
    #         F.countDistinct("sample_id").alias("total")
    #     )
    # )


    # non_public_sequencing_data = (
    #     filtered_samples_drug_phenotypes_box
    #     .select(
    #         F.col("sample_id")
    #     )
    #     .distinct()
    #     .join(
    #         seq_data,
    #         on="sample_id",
    #         how="inner"
    #     )
    #     .where(
    #         F.col("data_location")=="S3"
    #     )
    #     .join(
    #         dataset_to_sample,
    #         on="sample_id",
    #         how="left"
    #     )
    #     .join(
    #         dataset,
    #         on="dataset_id",
    #         how="inner"
    #     )
    #     .where(
    #         ~F.col("dataset_name").startswith("SEQTREAT2020")
    #     )
    #     .groupby(
    #         F.col("dataset_name"),
    #         F.col("dataset_owner"),
    #         F.col("contact_email")
    #     )
    #     .agg(
    #         F.countDistinct("sample_id").alias("total")
    #     )
    # )

    # samples_per_rif_r = (
    #     filtered_samples_drug_phenotypes_box
    #     .where(
    #         (F.col("drug_name")=="Rifampicin")
    #         & (F.col("phenotypic_category").isin("WHO", "ALL"))
    #     )
    #     .select(
    #         F.col("sample_id"),
    #         F.concat(F.lit("RIF_"), F.col("phenotype")).alias("RIF")
    #     )
    #     .distinct()
    #     .join(
    #         sample,
    #         on="sample_id",
    #         how="inner",
    #     )
    #     .join(
    #         country,
    #         on="country_id",
    #         how="left"
    #     )
    #     .groupby(
    #         F.col("country_usual_name"),
    #         F.col("three_letters_code"),
    #         F.col("RIF")
    #     )
    #     .agg(
    #         F.countDistinct("sample_id").alias("count")
    #     )
    #     .groupby(
    #         F.col("country_usual_name"),
    #         F.col("three_letters_code")
    #     )
    #     .pivot(
    #         "RIF"
    #     )
    #     .agg(
    #         F.first("count")
    #     )
    # )

    # samples_per_rif_r_per_lineage = (
    #     filtered_samples_drug_phenotypes_box
    #     .where(
    #         (F.col("drug_name")=="Rifampicin")
    #         & (F.col("phenotypic_category").isin("WHO", "ALL"))
    #     )
    #     .select(
    #         F.col("sample_id"),
    #         F.concat(F.lit("RIF_"), F.col("phenotype")).alias("RIF")
    #     )
    #     .distinct()
    #     .join(
    #         sample,
    #         on="sample_id",
    #         how="inner",
    #     )
    #     .join(
    #         country,
    #         on="country_id",
    #         how="left"
    #     )
    #     .join(
    #         lineages,
    #         on="sample_id",
    #         how="inner"
    #     )
    #     .withColumn(
    #         "true_lineage",
    #         F.split(F.col("lineage"), "\.").getItem(0)
    #     )
    #     .groupby(
    #         F.col("country_usual_name"),
    #         F.col("three_letters_code"),
    #         F.col("true_lineage"),
    #         F.col("RIF")
    #     )
    #     .agg(
    #         F.countDistinct("sample_id").alias("count")
    #     )
    #     .groupby(
    #         F.col("country_usual_name"),
    #         F.col("three_letters_code"),
    #         F.col("RIF"),
    #     )
    #     .pivot(
    #         "true_lineage"
    #     )
    #     .agg(
    #         F.first("count")
    #     )
    #     .na.fill(0)
    #     .withColumn(
    #         "L1 percentage",
    #         F.round(F.col("1")/(F.col("1")+F.col("2")+F.col("3")+F.col("4")+F.col("5")+F.col("6")+F.col("7")+F.col("BOV")+F.col("BOV_AFRI"))*100, 1)
    #     )
    #     .groupby(
    #         F.col("country_usual_name"),
    #         F.col("three_letters_code"),
    #     )
    #     .pivot(
    #         "RIF"
    #     )
    #     .agg(
    #         F.first("L1 percentage")
    #     )
    # )

    # output = io.BytesIO()
    # writer = pandas.ExcelWriter(output, engine="openpyxl")
    # drug_sample_overview.toPandas().to_excel(writer, sheet_name="Drug Sample Category count", index=False)
    # writer.sheets["Drug Sample Category count"].auto_filter.ref = "A1:D1"
    # samples_per_country.join(samples_per_rif_r, on=["country_usual_name", "three_letters_code"], how="left").sort("country_usual_name").toPandas().to_excel(writer, sheet_name="Samples country count", index=False)
    # non_public_sequencing_data.toPandas().to_excel(writer, sheet_name="Non pub data", index=False)
    # samples_per_rif_r_per_lineage.toPandas().to_excel(writer, sheet_name="Lineage_data", index=False)

    # writer.save()
    # data = output.getvalue()

    # s3.Bucket('aws-glue-assets-231447170434-us-east-1').put_object(Key=args["JOB_NAME"]+"/"+args["extraction"].strip("").strip("/")+"/extraction_currently_running/"+d+"_"+args["JOB_RUN_ID"]+"/drug_sample_category_count.xlsx", Body=data)


    # We are done with phenotypes
    # Drop then, but keep the list of (sample, drug) that are relevant for the rest
    filt_samp_drug = (
        filtered_samples_drug_phenotypes
        .drop(
            "phenotypic_category",
            "phenotype",
        )
        .distinct()
        .alias("filtered_samples")
    )


    fapg = formatted_annotation_per_gene(data_frame["varianttoannotation"], data_frame["annotation"], data_frame["dbxref"], protein_id).alias("fapg")

    mvd = multiple_variant_decomposition(data_frame["variant"]).alias("mvd")
    
    san = sanitize_synonymous_variant(fapg, data_frame["genedrugresistanceassociation"], mvd)

    mnvs_miss = missense_codon_list(fapg, data_frame["variant"], data_frame["genedrugresistanceassociation"])    

    var_cat = tiered_drug_variant_categories(fapg, san, data_frame["genedrugresistanceassociation"], data_frame["variant"], mvd, data_frame["promoterdistance"], mnvs_miss, bool(int(args["unpool_frameshifts"])))


    # additional_variant_information = glueContext.create_data_frame.from_catalog(database = glue_dbname, table_name = "postgres_genphensql_additional_variant_information").alias("additional_info").where(F.col("description")=="merker_neutral_variant")

    gene_name = gene_or_locus_tag_view(data_frame["seqfeature_dbxref"], data_frame["seqfeature_qualifier_value"], data_frame["seqfeature"], data_frame["term"], "gene_symbol").alias("gene")

    locus_tag = gene_or_locus_tag_view(data_frame["seqfeature_dbxref"], data_frame["seqfeature_qualifier_value"], data_frame["seqfeature"], data_frame["term"], "rv_symbol").alias("locus_tag")

    gene_locus_tag = merge_gene_locus_view(gene_name, locus_tag).alias("gene_locus_tag")

    # samples_without_phenotypes = (
    #     filtered_samples
    #     .alias("all_samples")
    #     .crossJoin(
    #         tier
    #         .select("drug_id")
    #         .distinct()   
    #     )
    #     .join(
    #         filt_samp_drug,
    #         on=["sample_id", "drug_id"],
    #         how = "left_anti"
    #     )
    #     .select(
    #         F.col("sample_id"),
    #         F.col("drug_id")
    #     )
    #     .distinct()
    # )

    # v1_variant = glueContext.create_data_frame.from_catalog(database = glue_dbname, table_name = "postgres_genphensql_additional_variant_information").alias("v1_match").where(F.col("description")!="merker_neutral_variant")

    # v1_v2_matching = (
    #     var_cat
    #     .join(
    #         v1_variant,
    #         "variant_id",
    #         "inner"
    #     )
    #     .join(
    #         g_l_tag,
    #         "gene_db_crossref_id",
    #         "inner"
    #     )
    #     .select(
    #         F.col("resolved_symbol"),
    #         F.col("variant_category"),
    #         F.col("predicted_effect"),
    #         F.col("description")
    #     )
    #     .distinct()
    # )

    # glueContext.write_dynamic_frame.from_options(
    #     frame=DynamicFrame.fromDF(
    #         v1_v2_matching.drop("variant_id").coalesce(1),
    #         glueContext,
    #         "final"
    #     ),
    #     connection_type="s3",
    #     format="csv",
    #     connection_options={
    #         "path": "s3://aws-glue-assets-231447170434-us-east-1/"+args["JOB_NAME"]+"/"+args["extraction"].strip("").strip("/")+"/extraction_currently_running/"+d+"_"+args["JOB_RUN_ID"]+"/v1_matching/",
    #         "partitionKeys": [],
    #     }
    # )

    gen_cat = (
        data_frame["genotype"]
        .where(
            F.col("genotyper").isin('freebayes', 'delly')
        )
        .join(
            var_cat,
            on="variant_id",
            how="inner"
        )
        .join(
            other=filtered_samples.select("sample_id").distinct().crossJoin(data_frame["genedrugresistanceassociation"].select("drug_id").distinct()),
            on=["sample_id", "drug_id"],
            how="left_semi",
        )
        .withColumn(
            "af", 
            F.bround(F.col("alternative_ad").cast("long")/F.col("total_dp").cast("long"), 2)
        )
        .where(
            F.col("af")>=0.25
        )
    )

    # pos = most_frequent_position_by_category(gen_cat)

    all_pos = all_positions_by_category(var_cat)


    #neutral_variants = get_neutral_variants(filtered_samples_neutral, clean_phenotypes_neutral, variant_category, genotype, 0.1, additional_variant_information, drug, gene_name)


    # twalker_extraction(gen_cat, all_pos, samples_without_phenotypes, lss, drug, g_l_tag, orphan=True)

    twalker_extraction(
        gen_cat,
        all_pos,
        filt_samp_drug,
        data_frame["locussequencingstats"],
        data_frame["drug"],
        gene_locus_tag,
        data_frame["genedrugresistanceassociation"],
    )



    # if args["extraction"]=="twalker":
    # elif args["extraction"]=="farhat_lab":
    #     all_mic_data = (
    #         mic
    #         .join(
    #             drug,
    #             "drug_id",
    #             "inner"
    #         )
    #         .join(
    #             tier,
    #             "drug_id",
    #             "left_semi"
    #         )
    #         .join(
    #             filtered_samples,
    #             "sample_id",
    #             "left_semi"
    #         )
    #         .select(
    #             F.col("sample_id"),
    #             F.col("drug_name"),
    #             F.col("plate").alias("medium"),
    #             F.col("mic_value")
    #         )
    #     )

    #     S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    #         frame=DynamicFrame.fromDF(all_mic_data,
    #             glueContext,
    #             "final"),
    #         connection_type="s3",
    #         format="csv",
    #         connection_options={
    #             "path": "s3://aws-glue-assets-231447170434-us-east-1/"+args["JOB_NAME"]+"/"+args["extraction"].strip("").strip("/")+"/extraction_currently_running/"+d+"_"+args["JOB_RUN_ID"]+"/mic/",
    #             "partitionKeys": ["drug_name"],
    #         }
    #     )

    #     snv_sites = get_snv_positions_sites(seqfeat, sdc, sqv, term, location, variant)


    #     grm_data = (
    #         get_sample_x_variant_data(snv_sites, filt_samp_drug.select("sample_id").distinct(), genotype, 0.01)[0]
    #         .select(
    #             F.col("sample_id"),
    #             F.col("position"),
    #             F.coalesce(F.col("alternative_nucleotide"), F.col("selected_sites.reference_nucleotide")),
    #             F.col("dp"),
    #         )
    #     )

    #     S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    #         frame=DynamicFrame.fromDF(grm_data,
    #             glueContext,
    #             "final"),
    #         connection_type="s3",
    #         format="csv",
    #         connection_options={
    #             "path": "s3://aws-glue-assets-231447170434-us-east-1/"+args["JOB_NAME"]+"/"+args["extraction"].strip("").strip("/")+"/extraction_currently_running/"+d+"_"+args["JOB_RUN_ID"]+"/grm/",
    #             "partitionKeys": [],
    #         }
    #     )

    #     pca_data = glueContext.create_dynamic_frame.from_catalog(database = glue_dbname, table_name = "postgres_genphensql_pca_dimension_results")

    #     S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    #         frame=pca_data,
    #         connection_type="s3",
    #         format="csv",
    #         connection_options={
    #             "path": "s3://aws-glue-assets-231447170434-us-east-1/"+args["JOB_NAME"]+"/"+args["extraction"].strip("").strip("/")+"/extraction_currently_running/"+d+"_"+args["JOB_RUN_ID"]+"/pca/",
    #             "partitionKeys": [],
    #         }
    #     )

        # farhat_lab_extraction(gen_cat, pos, all_pos, filt_samp_drug, var_cat, lss, sample, drug, g_l_tag)

    # elif args["extraction"]=="custom":
    #     args = getResolvedOptions(sys.argv, ["reference_mutation", "gene_list", "drug_list"])

    #     gene_name = args["reference_mutation"].split("_")[0].strip()


    #     hgvs = args["reference_mutation"].split("_")[1].strip()

    #     sample_ids = (
    #         gen_cat
    #         .join(
    #             g_l_tag.where(F.col("resolved_symbol")==gene_name),
    #             on="gene_db_crossref_id",
    #             how="inner"
    #         )
    #         .where(
    #             F.col("variant_category")==hgvs
    #         )
    #         .select("sample_id")
    #         .distinct()
    #     )


    # s3 = boto3.client("s3")

    # files = s3.list_objects_v2(
    #     Bucket="aws-glue-assets-231447170434-us-east-1",
    #     Prefix=args["JOB_NAME"]+"/"+args["extraction"].strip("/").strip("")+"/extraction_currently_running/"+d+"_"+args["JOB_RUN_ID"]
    # )

    # for file in files["Contents"]:
    #     s3.copy(
    #         CopySource={"Bucket":"aws-glue-assets-231447170434-us-east-1", "Key":file["Key"]},
    #         Bucket="aws-glue-assets-231447170434-us-east-1",
    #         Key=file["Key"].replace("extraction_currently_running", "extraction_successful")
    #     )