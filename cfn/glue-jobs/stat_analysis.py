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

def solo_extraction(genotype_categorized, position, filtered_sample_drug, loc_seq_stats, drug, gene_locus_tag, tier, bucket, orphan=False):

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
                "path": "s3://"+bucket+"/"+args["JOB_NAME"]+"/"+d+"_"+args["JOB_RUN_ID"]+"/missing_sample_drug_tier/",
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
            "path": "s3://"+bucket+"/"+args["JOB_NAME"]+"/"+d+"_"+args["JOB_RUN_ID"]+"/"+output_folder+"/",
            "partitionKeys": partition
        }
    )



if __name__=="__main__":

    d = datetime.datetime.now().isoformat().replace(":", "-")

    args = getResolvedOptions(sys.argv, ['JOB_NAME', "postgres_db_name", "glue_db_name", "unpool_frameshifts", "TempDir"])

    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session
    spark._jsc.hadoopConfiguration().set('spark.sql.broadcastTimeout', '3600')
    job = Job(glueContext)

    bucket = args["TempDir"].split("s3://")[1].strip("/")

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
                    "variantadditionalinfo",
                    "country",
                    "variantgrade"
                    ],
                "submission" : [
                    "pdstest",
                    "mictest",
                    "genotype",
                    "locussequencingstats",
                    "sequencingdata",
                    "sample",
                    "summarysequencingstats",
                    "samplealias",
                ]
            }
    }

    for schema in tables.keys():
        if isinstance(tables[schema], list):
            for table in tables[schema]:
                tname = dbname + "_biosql_" + table
                data_frame[table] = glueContext.create_data_frame.from_catalog(
                    database = glue_dbname,
                    table_name = tname
                )
        elif isinstance(tables[schema], dict):
            for app in tables[schema].keys():
                for table in tables[schema][app]:
                    tname = dbname + "_" + schema + "_" + app + "_" + table
                    data_frame[table] = glueContext.create_data_frame.from_catalog(
                        database = glue_dbname,
                        table_name = tname
                    )
        else: 
            raise ValueError

    data_frame["mictest"] = (
        data_frame["mictest"]
        .alias("mic")
        .withColumnRenamed(
            "range",
            "mic_value"
        )
        .where(
            F.col("staging")==False
        )
    )

    data_frame["pdstest"] = (
        data_frame["pdstest"]
        .alias("pdst")
        .where(
            F.col("staging")==False
        )
    )

    data_frame["sample"] = (
        data_frame["sample"]
        .alias("sample")
        .withColumnRenamed(
            "id",
            "sample_id"
        )
    )

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
            "path": "s3://"+bucket+"/"+args["JOB_NAME"]+"/"+d+"_"+args["JOB_RUN_ID"]+"/phenotypes/",
            "partitionKeys": ["drug_name"],
        }
    )


    order_categories = {
        'WHO': 1,
        "not-WHO":2,
        'ALL': 3,
        'CC': 4,
        'CC-ATU':5
    }

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
            F.when(
                F.col("phenotypic_category")=="ALL", "not-WHO")
                .otherwise(F.col("phenotypic_category")
            ).alias("phenotypic_category"),
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

    sample_country = (
        filtered_samples_drug_phenotypes_box
        .select(
            F.col("sample_id")
        )
        .distinct()
        .join(
            data_frame["sample"].alias("sample"),
            on="sample_id",
            how="left",
        )
        .select(
            F.col("sample_id"),
            F.col("country_id")
        )
    )

    samplealias_country = (
        filtered_samples_drug_phenotypes_box
        .select(
            F.col("sample_id")
        )
        .distinct()
        .join(
            data_frame["samplealias"].alias("alias"),
            on="sample_id",
            how="left",
        )
        .select(
            F.col("sample_id"),
            F.col("country_id")
        )
    )

    samples_per_country = (
        sample_country
        .unionByName(
            samplealias_country
        )
        .groupby(
            F.col("sample_id")
        )
        .agg(
            F.first("country_id", ignorenulls=True).alias("country_id")
        )
        .alias("union")
        .join(
            data_frame["country"].alias("country"),
            F.col("union.country_id")==F.col("country.three_letters_code"),
            how="left"
        )
        .groupby(
            F.col("country_usual_name"),
            F.col("three_letters_code"),
        )
        .agg(
            F.countDistinct("sample_id").alias("total")
        )
    )



    s3 = boto3.resource("s3")
    try:

        output = io.BytesIO()
        writer = pandas.ExcelWriter(output, engine="openpyxl")
        drug_sample_overview.toPandas().to_excel(writer, sheet_name="Drug Sample Category count", index=False)
        writer.sheets["Drug Sample Category count"].auto_filter.ref = "A1:D1"
        samples_per_country.sort("country_usual_name").toPandas().to_excel(writer, sheet_name="Samples country count", index=False)
        # samples_per_country.join(samples_per_rif_r, on=["country_usual_name", "three_letters_code"], how="left").sort("country_usual_name").toPandas().to_excel(writer, sheet_name="Samples country count", index=False)
        # non_public_sequencing_data.toPandas().to_excel(writer, sheet_name="Non pub data", index=False)
        # samples_per_rif_r_per_lineage.toPandas().to_excel(writer, sheet_name="Lineage_data", index=False)
        writer.save()
        data = output.getvalue()

        s3.Bucket(bucket).put_object(Key=args["JOB_NAME"]+"/"+d+"_"+args["JOB_RUN_ID"]+"/drug_sample_category_count.xlsx", Body=data)
    except ModuleNotFoundError:
        csv_buffer = io.BytesIO()
        samples_per_country.toPandas().to_csv(csv_buffer, index=False)
        s3.Object(bucket, args["JOB_NAME"]+"/"+d+"_"+args["JOB_RUN_ID"]+"/variant_mapping.csv").put(Body=csv_buffer.getvalue())


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

    protein_id = protein_id_view(data_frame["dbxref"], data_frame["seqfeature_qualifier_value"], data_frame["seqfeature_dbxref"]).alias("protein_id")

    fapg = formatted_annotation_per_gene(data_frame["varianttoannotation"], data_frame["annotation"], data_frame["dbxref"], protein_id).alias("fapg")

    mvd = multiple_variant_decomposition(data_frame["variant"]).alias("mvd")
    
    san = sanitize_synonymous_variant(fapg, data_frame["genedrugresistanceassociation"], mvd)

    mnvs_miss = missense_codon_list(fapg, data_frame["variant"], data_frame["genedrugresistanceassociation"])    

    var_cat = tiered_drug_variant_categories(fapg, san, data_frame["genedrugresistanceassociation"], data_frame["variant"], mvd, data_frame["promoterdistance"], mnvs_miss, bool(int(args["unpool_frameshifts"])))[0].alias("variant_category")

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


    #Excluding 2 deletions at the end of fbiC which do not impact the protein sequence
    fbiC_excluded_variants = (
        data_frame["variant"]
        .where(
            (F.col("position")==1305494)
            & (F.col("alternative_nucleotide")=="C")
            &
                (
                (F.col("reference_nucleotide")=="CGGCCTAGCCCCGGCGACGATGCCGGGTCGCGGGATGCGGCCCGTTGAGGAGCGGGGCAATCT")
                | (F.col("reference_nucleotide")=="CGGCCTAGCCCCGGCGACGATGCCGGGTCGCGGGATGCGGCCCGTTGAGGAGCGGGGCAATCTGGCCTAGCCCCGGCGACGATGCCGGGTCGCGGGATGCGGCCCGTTGAGGAGCGGGGCAATCT")
            )
        )
    )

    var_cat = (
        var_cat
        .join(
            fbiC_excluded_variants,
            on="variant_id",
            how="left_anti"
        )
    )

    data_frame["variantadditionalinfo"].show()

    v1_variant = ( 
        data_frame["variantadditionalinfo"]
        .drop(
            "variant_id", 
            "id"
        )
        .join(
            data_frame["variant"],
            on=["position", "alternative_nucleotide", "reference_nucleotide"],
            how="inner"
        )
    )


    v1_variant = (
        v1_variant
        .join(
            var_cat,
            on="variant_id",
            how="inner"
        )
    )

    v1_variant= (
        v1_variant
        .join(
            gene_locus_tag,
            "gene_db_crossref_id",
            "inner"
        )
        .select(
            F.col("resolved_symbol"),
            F.col("variant_category"),
            F.col("predicted_effect"),
            F.col("v1_annotation").alias("description")
        )
        .distinct()
    )


    glueContext.write_dynamic_frame.from_options(
        frame=DynamicFrame.fromDF(
            v1_variant.drop("variant_id").coalesce(1),
            glueContext,
            "final"
        ),
        connection_type="s3",
        format="csv",
        connection_options={
            "path": "s3://"+bucket +"/"+args["JOB_NAME"]+"/"+d+"_"+args["JOB_RUN_ID"]+"/prev_version_matching/",
            "partitionKeys": [],
        }
    )

    v2_grading_matching = (
        data_frame["variantgrade"]
        .where(
            F.col("grade_version")==2
        )
        .join(
            var_cat
            .select(
                F.col("variant_id"),
                F.col("gene_db_crossref_id"),
                F.col("variant_category"),
            ),
            on="variant_id",
            how="inner"
        )
        .join(
            data_frame["drug"],
            on="drug_id",
            how="inner"
        )
        .join(
            gene_locus_tag,
            on="gene_db_crossref_id",
            how="inner"
        )
        .select(
            F.concat_ws(
                "_",
                F.col("resolved_symbol"),
                F.col("variant_category"),
            ).alias("variant"),
            F.col("drug_name"),
            F.col("grade")
        )
    )

    glueContext.write_dynamic_frame.from_options(
        frame=DynamicFrame.fromDF(
            v2_grading_matching.drop("variant_id").coalesce(1),
            glueContext,
            "final"
        ),
        connection_type="s3",
        format="csv",
        connection_options={
            "path": "s3://"+bucket +"/"+args["JOB_NAME"]+"/"+d+"_"+args["JOB_RUN_ID"]+"/prev_version_grades/",
            "partitionKeys": [],
        }
    )

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


    solo_extraction(
        gen_cat,
        all_pos,
        filt_samp_drug,
        data_frame["locussequencingstats"],
        data_frame["drug"],
        gene_locus_tag,
        data_frame["genedrugresistanceassociation"],
        bucket,
        orphan = False
    )