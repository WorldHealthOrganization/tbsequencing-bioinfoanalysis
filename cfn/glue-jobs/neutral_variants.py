import sys, boto3, io, pandas, datetime
from scipy import stats
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql.window import Window
import pyspark.sql.functions as F

from biosql_gene_views import *
from variant_annotation_categorization import *
from variant_classification_algorithms import *
from phenotypic_data_views import *


s3 = boto3.resource('s3')


def get_neutral_variants(filtered_samples, clean_phenotypes, variant_category, genotype, ppv_threshold, additional_variant_information, drug, gene_name):
    # Select samples based on their phenotype 
    # Only keep clean samples (ie all results either R or S)
    # For neutral mutation identification we only keep WHO current & past categories

    # First getting all (genotypes, category) of filtered samples
    genotype_categorized = (
        genotype
        .where(
            genotype.genotyper.isin(['freebayes', 'delly'])
            & (genotype.quality>1000)
        )
        .join(
            other=filtered_samples,
            on="sample_id",
            how="left_semi",
        )
        .withColumn(
            "percent_reads", 
            F.bround(genotype.alternative_ad.cast("long")/genotype.total_dp.cast("long"), 2)
        )
        .where(
            F.col("percent_reads")>0.75
        )
        .join(
            variant_category,
            on="variant_id",
            how="inner"
        )
    )

    # Adding the phenotypes of the sample to each categorized genotype
    genotype_categorized_phenotyped = (
        genotype_categorized
        .join(
            clean_phenotypes,
            on=["sample_id", "drug_id"],
            how="inner",
        )
    )

    # Counting categories for each (drug, phenotype)
    # We will need the susceptible count later as well for set D1 and D2
    counts_raw_setA = (
        count_number_of_samples_per_category_phenotype(
            genotype_categorized_phenotyped,
            ["R", "S"]
        )
    )

    # Calculate the raw data for all variant categories
    ppv_raw_setA = (
        calculate_positive_predictive_value(
            counts_raw_setA
        )
    )

    # Get neutral variants for set A
    excluded_setA = filter_variant_categories_on_upper_ppv(ppv_raw_setA, ppv_threshold, "A").alias("excluded_setA")

    for key, value in {x:x+"_raw_setA" for x in ["R", "S", "PPV", "Upper_PPV"]}.items():
        ppv_raw_setA = (
            ppv_raw_setA
            .withColumnRenamed(
                key, value
            )
        )

    #Get samples with known markers of resistance
    #So that we can exclude them to try & find neutral mutations
    excluded_samples_for_setB = (
        genotype_categorized_phenotyped
        .join(drug,
            on="drug_id",
            how="inner")
        .join(gene_name,
            on="gene_db_crossref_id",
            how="inner")
        .where( 
            ((F.col("drug_name")=="Isoniazid")
                & (
                    ((F.col("gene.gene_symbol")=='inhA') 
                        & ((F.col("variant_category")=="c.-777C>T")
                            | (F.col("variant_category")=="p.Ser94Ala")
                            | (F.col("variant_category")=="c.-778A>G")
                            | (F.col("variant_category")=="c.-770T>C")
                            | (F.col("variant_category")=="c.-770T>A")
                            | (F.col("variant_category")=="c.-154G>A")))
                    | ((F.col("gene.gene_symbol")=='katG')
                        & ((F.col("variant_category").rlike('^p\.Ser315.*'))))
                )
            )
            | ((F.col("drug_name")=="Rifampicin")
                & (
                    ((F.col("gene.gene_symbol")=='rpoB')
                        & ((F.col("variant_category")=="p.Val170Phe") 
                            | (F.col("variant_category")=="p.Ile491Phe")
                            | ((F.col("distance_to_reference")>=1278)
                                & (F.col("distance_to_reference")<=1356)
                                & (F.col("predicted_effect")!="synonymous_variant")
                                )
                        )
                    )
                )
            )
            | ((F.col("drug_name")=="Ethambutol")
                & (
                    ((F.col("gene.gene_symbol")=='embB')
                        & ((F.col("variant_category")=="p.Met306Ile") 
                            | (F.col("variant_category")=="p.Met306Leu")
                            | (F.col("variant_category")=="p.Met306Val")
                            | (F.col("variant_category")=="p.Asp354Ala")
                            | (F.col("variant_category")=="p.Gly406Ala")
                            | (F.col("variant_category")=="p.Gly406Cys")
                            | (F.col("variant_category")=="p.Gly406Asp")
                            | (F.col("variant_category")=="p.Gly406Ser")
                            | (F.col("variant_category")=="p.Gln497Arg")
                        )
                    )
                ))
            | ((F.col("drug_name")=="Pyrazinamide")
                & (
                    ((F.col("gene.gene_symbol")=='pncA')
                        & (
                            (F.col("variant_category")=="c.-11A>G")
                            | (
                                ~F.col("variant_category").rlike("^c\..*")
                                & (F.col("variant_category")!="p.Ile6Leu")
                                & (F.col("variant_category")!="p.Leu35Arg")
                                )
                        )
                    )
                ))
            | ((F.col("drug_name")=="Levofloxacin") | (F.col("drug_name")=="Moxifloxacin")
                & (
                    ((F.col("gene.gene_symbol")=='gyrA')
                        & ((F.col("variant_category")=="p.Gly88Ala")
                            | (F.col("variant_category")=="p.Gly88Cys")
                            | (F.col("variant_category")=="p.Asp89Asn")
                            | (F.col("variant_category")=="p.Ala90Val")
                            | (F.col("variant_category")=="p.Ser91Pro")
                            | (F.col("variant_category")=="p.Asp94Ala")
                            | (F.col("variant_category")=="p.Asp94Gly")
                            | (F.col("variant_category")=="p.Asp94His")
                            | (F.col("variant_category")=="p.Asp94Asn")
                            | (F.col("variant_category")=="p.Asp94Tyr")
                        )
                    )
                    |
                    ((F.col("gene.gene_symbol")=='gyrB')
                        & ((F.col("variant_category")=="p.Ala504Val")
                            | (F.col("distance_to_reference")>=1491)
                                & (F.col("distance_to_reference")<=1506)
                                & (F.col("predicted_effect")!="synonymous_variant")
                        )
                    )
                ))
            | ((F.col("drug_name")=="Amikacin")
                & (
                    ((F.col("gene.gene_symbol")=='rrs')
                        & ((F.col("variant_category")=="n.1401A>G")
                            | (F.col("variant_category")=="n.1402C>T")
                            | (F.col("variant_category")=="n.1484G>T")
                        )
                    )
                    |
                    ((F.col("gene.gene_symbol")=='eis') & (F.col("variant_category")=="c.-14C>T")))
                )
            | ((F.col("drug_name")=="Streptomycin")
                & (
                    ((F.col("gene.gene_symbol")=='rrs')
                        & ((F.col("variant_category")=="n.514A>C")
                            | (F.col("variant_category")=="n.517C>T")
                            | (F.col("variant_category")=="n.1484G>T")
                        )
                    )
                    |
                    ((F.col("gene.gene_symbol")=='rpsL')
                        & ((F.col("variant_category")=="p.Lys43Arg")
                            | (F.col("variant_category")=="p.Lys88Gln")
                            | (F.col("variant_category")=="p.Lys88Arg")
                        )
                    )
                )
            )
            | ((F.col("drug_name")=="Ethionamide")
                & (
                    ((F.col("gene.gene_symbol")=='inhA') 
                        & (F.col("variant_category")=="p.Ser94Ala")
                    )
                    | ((F.col("gene.gene_symbol")=='fabG1') 
                        & ((F.col("variant_category")=="c.-15C>T")
                            | (F.col("variant_category")=="c.-16A>G")
                            | (F.col("variant_category")=="c.-8T>C")
                            | (F.col("variant_category")=="c.-8T>A")
                            | (F.col("variant_category")=="c.609G>A"))
                    )
                )
            )
            | ((F.col("drug_name")=="Kanamycin")
                & (
                    ((F.col("gene.gene_symbol")=='rrs')
                        & ((F.col("variant_category")=="n.1401A>G")
                            | (F.col("variant_category")=="n.1402C>T")
                            | (F.col("variant_category")=="n.1484G>T")
                        )
                    )
                    |
                    ((F.col("gene.gene_symbol")=='eis')
                        & ((F.col("variant_category")=="c.-37G>T")
                            | (F.col("variant_category")=="c.-14C>T")
                            | (F.col("variant_category")=="c.-12C>T")
                            | (F.col("variant_category")=="c.-10G>A"))
                        )
                    )
                )
            | ((F.col("drug_name")=="Capreomycin")
                & (
                    ((F.col("gene.gene_symbol")=='rrs')
                        & ((F.col("variant_category")=="n.1401A>G")
                            | (F.col("variant_category")=="n.1402C>T")
                            | (F.col("variant_category")=="n.1484G>T")
                        )
                    )
                    |
                    ((F.col("gene.gene_symbol")=='tlyA')
                        & ((F.col("variant_category")=="p.Asn236Lys"))
                        )
                    )
                ))
        .select(
            F.col("sample_id"),
            F.col("drug_id")
        )
    )


    # Get PPV and UPPV values directly when excluding samples that have well known markers for resistance (the infamous WKME)
    ppv_wkme_setB = (
        calculate_positive_predictive_value(
            count_number_of_samples_per_category_phenotype(
                genotype_categorized_phenotyped
                # Remove all pairs of (sample, drug) for which one of the usual markers for resistance have been found
                .join(excluded_samples_for_setB,
                    on=["sample_id", "drug_id"],
                    how="left_anti"),
                ["R", "S"]
            )
        )
    )

    # Neutral variants set B
    excluded_setB = filter_variant_categories_on_upper_ppv(ppv_wkme_setB, ppv_threshold, "B").alias("excluded_setB")

    for key, value in {x:x+"_WKME_setB" for x in ["R", "S", "PPV", "Upper_PPV"]}.items():
        ppv_wkme_setB = (
            ppv_wkme_setB
            .withColumnRenamed(
                key, value
            )
        )

    # Now get all variants that have not been marked as neutral so far
    remaining_variants_AB = (
        variant_category
        .alias("variant_category_2")
        # Excluding sets A+B with an anti join
        .join(excluded_setA,
            on = ["drug_id", "tier", "gene_db_crossref_id", "variant_category"],
            how="left_anti")
        .join(excluded_setB,
            on = ["drug_id", "tier", "gene_db_crossref_id", "variant_category"],
            how="left_anti")
        .select(
            F.col("drug_id"),
            F.col("tier"),
            F.col("gene_db_crossref_id"),
            F.col("variant_category")
        )
        .distinct()
        .alias("remaining_variants_AB")
    )

    solo_samples_set_D1 = (
        classify_solo_samples(
            genotype_categorized_phenotyped
            # Only select R samples as we are using the S counts from set A
            .where(
                F.col("phenotype")=="R"
            )
            # Variant category can be associated to only one (drug, tier). So no need to join on tier.
            # Keeping only non neutral variants so far
            .join(
                remaining_variants_AB,
                on=["drug_id", "gene_db_crossref_id", "variant_category"],
                how="left_semi"
            )
        )
    )

    ppv_solo_set_D1 = (
        calculate_positive_predictive_value(
            count_number_of_samples_per_category_phenotype(
                solo_samples_set_D1
                .join(
                    genotype_categorized_phenotyped.alias("genotype_2"),
                    on=["drug_id", "tier", "sample_id"],
                    how="inner"
                )
                # Remove again the neutral variants before performing the counts/ppv
                .join(
                    remaining_variants_AB,
                    on=["drug_id", "gene_db_crossref_id", "variant_category"],
                    how="left_semi"
                ),
                ["R"]
            )
            .join(
                counts_raw_setA.select("drug_id", "tier", "gene_db_crossref_id", "variant_category", "S"),
                on=["drug_id", "tier", "gene_db_crossref_id", "variant_category"],
                how="inner"
            )
        )
    )

    excluded_setD1 = filter_variant_categories_on_upper_ppv(ppv_solo_set_D1, ppv_threshold, "D1").alias("excluded_set_D1")

    for key, value in {x:x+"_SOLO_setD1" for x in ["R", "S", "PPV", "Upper_PPV"]}.items():
        ppv_solo_set_D1 = (
            ppv_solo_set_D1
            .withColumnRenamed(
                key, value
            )
        )

    remaining_variants_ABD1 = (
        remaining_variants_AB
        .join(excluded_setD1,
            on = ["drug_id", "tier", "gene_db_crossref_id", "variant_category"],
            how="left_anti")
        .alias("remaining_variants_ABD1")
    )

    solo_samples_set_D2 = (
        classify_solo_samples(
            genotype_categorized_phenotyped
            # Only select R samples as we are using the S counts from set A
            .where(
                F.col("phenotype")=="R"
            )
            # Variant category can be associated to only one (drug, tier). So no need to join on tier.
            .join(
                remaining_variants_ABD1,
                on=["drug_id", "gene_db_crossref_id", "variant_category"],
                how="left_semi"
            )
        )
    )

    ppv_solo_set_D2 = (
        calculate_positive_predictive_value(
            count_number_of_samples_per_category_phenotype(
                solo_samples_set_D2
                .join(
                    genotype_categorized_phenotyped.alias("genotype_3"),
                    on=["drug_id", "tier", "sample_id"],
                    how="inner"
                )
                .join(
                    remaining_variants_ABD1,
                    on=["drug_id", "gene_db_crossref_id", "variant_category"],
                    how="left_semi"
                ),
                ["R"]
            )
            .join(
                counts_raw_setA.select("drug_id", "tier", "gene_db_crossref_id", "variant_category", "S"),
                on=["drug_id", "tier", "gene_db_crossref_id", "variant_category"],
                how="inner"
            )
        )
    )

    excluded_setD2 = filter_variant_categories_on_upper_ppv(ppv_solo_set_D2, ppv_threshold, "D2").alias("excluded_set_D2")

    for key, value in {x:x+"_SOLO_setD2" for x in ["R", "S", "PPV", "Upper_PPV"]}.items():
        ppv_solo_set_D2 = (
            ppv_solo_set_D2
            .withColumnRenamed(
                key, value
            )
        )

    remaining_variants_ABCD1D2 = (
        remaining_variants_ABD1
        .join(excluded_setD2,
            on = ["drug_id", "tier", "gene_db_crossref_id", "variant_category"],
            how="left_anti")
        .alias("remaining_variants_ABCD1D2")
    )

    merker_missed = (
        remaining_variants_ABCD1D2
        .join(variant_category.alias("variant_category"),
            on=["drug_id", "tier", "gene_db_crossref_id", "variant_category"],
            how="inner")
        .join(drug,
            on="drug_id",
            how="inner")
        .join(
            additional_variant_information,
            on=(F.col("additional_info.variant_id")==F.col("variant_category.variant_id")) 
            & F.col("drug_name").isin(["Pyrazinamide", "Ethambutol", "Rifampicin", "Isoniazid"]),
            how="left_semi"
        )
        .select(
            F.col("drug_id"),
            F.col("tier"),
            F.col("gene_db_crossref_id"),
            F.col("variant_category"),
            F.lit("M").alias("set"))
        .distinct()
    ).alias("excluded_set_M")

    ppv_df = (
        ppv_raw_setA
        .join(ppv_wkme_setB,
            on=["drug_id", "gene_db_crossref_id", "tier", "variant_category"],
            how="left")
        .join(ppv_solo_set_D1,
            on=["drug_id", "gene_db_crossref_id", "tier", "variant_category"],
            how="left")
        .join(ppv_solo_set_D2,
            on=["drug_id", "gene_db_crossref_id", "tier", "variant_category"],
            how="left")
    )

    all_excluded = (
        excluded_setA
        .unionByName(excluded_setB)
        .unionByName(excluded_setD1)
        .unionByName(excluded_setD2)
        .unionByName(merker_missed, allowMissingColumns=True)
    )

    return(ppv_df, all_excluded)

def write_neutral_variants_to_excel(ppv_df, all_excluded, drug, locus_tag, gene_locus_tag, most_frequent_position, file_name, date):
    ppv_df_joined = (
        ppv_df
            .join(drug,
            on="drug_id",
            how="inner")
        .join(gene_locus_tag,
            on="gene_db_crossref_id",
            how="inner")
        .join(locus_tag,
            on="gene_db_crossref_id",
            how="inner")
        .join(
            most_frequent_position,
            on=["gene_db_crossref_id", "variant_category"],
            how="inner"
        )
        .drop(
            "drug_id",
            "gene_db_crossref_id"
        )
        .withColumnRenamed("drug_name", "Drug")
        .withColumnRenamed("resolved_symbol", "Gene")
        .withColumnRenamed("rv_symbol", "LocusTag")
        .withColumnRenamed("tier", "Tier")
        .withColumnRenamed("variant_category", "Variant")
        .toPandas()
    )

    ppv_df_joined = ppv_df_joined.set_index(["Drug", "Tier", "Gene", "LocusTag", "Variant", "Effect", "ModalPosition"])

    column_list = ["R", "S", "PPV", "Upper_PPV"]

    ppv_df_joined.columns = pandas.MultiIndex.from_tuples(
        [("Set A - Raw", x) for x in column_list]
        + [("Set B - After removal of isolates with well known markers of resistance", x) for x in column_list]
        + [("Set D1 - Solo after removing neutral from A+B", x) for x in column_list]
        + [("Set D2 - Solo after removing neutral from A+B+D1", x) for x in column_list]
        )

    with io.BytesIO() as output:
        with pandas.ExcelWriter(output, engine='openpyxl') as writer:
            ppv_df_joined.reset_index(["Drug", "Tier", "Gene", "LocusTag", "Variant", "Effect", "ModalPosition"]).to_excel(writer, sheet_name = "Complete data")
            empty = not any((cell.value for cell in writer.sheets["Complete data"][3]))
            if empty:
                writer.sheets["Complete data"].delete_rows(3)
            writer.sheets["Complete data"].move_range("B1:H1", rows=1)
            writer.sheets["Complete data"].auto_filter.ref = "A2:"+writer.sheets["Complete data"].dimensions.split(":")[1]
            writer.sheets["Complete data"].column_dimensions["B"].auto_size=True
            writer.sheets["Complete data"].column_dimensions["P"].width = "35"
            writer.sheets["Complete data"].column_dimensions["T"].width = "35"
            writer.sheets["Complete data"].column_dimensions["X"].width = "35"

            all_excluded_joined = (
                all_excluded
                .join(drug,
                    on="drug_id",
                    how="inner")
                .join(gene_locus_tag,
                    on="gene_db_crossref_id",
                    how="inner")
                .join(locus_tag,
                    on="gene_db_crossref_id",
                    how="inner")
                .join(
                    most_frequent_position,
                    on=["gene_db_crossref_id", "variant_category"],
                    how="inner"
                )
                .drop(
                    "drug_id",
                    "gene_db_crossref_id",
                )
                .withColumnRenamed("drug_name", "Drug")
                .withColumnRenamed("resolved_symbol", "Gene")
                .withColumnRenamed("rv_symbol", "LocusTag")
                .withColumnRenamed("tier", "Tier")
                .withColumnRenamed("variant_category", "Variant")
                .withColumnRenamed("set", "Set")
                .select(
                    F.col("Drug"),
                    F.col("Tier"),
                    F.col("Gene"),
                    F.col("LocusTag"),
                    F.col("Variant"),
                    F.col("Effect"),
                    F.col("ModalPosition"),
                    F.col("Set"),
                    F.col("PPV"),
                    F.col("Upper_PPV"),
                )
            )

            all_excluded_joined.toPandas().to_excel(writer, sheet_name="Neutral variants", index=False)

            writer.sheets["Neutral variants"].auto_filter.ref = "A1:"+writer.sheets["Neutral variants"].dimensions.split(":")[1]

        data = output.getvalue()

    s3.Bucket('aws-glue-assets-231447170434-us-east-1').put_object(Key=args["JOB_NAME"]+"/"+date+"_"+args["JOB_RUN_ID"]+"/"+file_name+".xlsx", Body=data)

if __name__ == "__main__":
    d = datetime.datetime.now().isoformat()

    args = getResolvedOptions(sys.argv, ['JOB_NAME', "glue_database_name", "postgres_db_name"])

    glueContext = GlueContext(SparkContext.getOrCreate())

    spark = glueContext.spark_session

    spark._jsc.hadoopConfiguration().set('spark.hadoop.fs.s3.maxConnections', '1000')

    job = Job(glueContext)

    phenotypes = glueContext.create_data_frame.from_catalog(database = args["glue_database_name"], table_name = f"{args['postgres_db_name']}_genphensql_phenotypic_drug_susceptibility_test").alias("phenotypes")

    phenotypes_category = glueContext.create_data_frame.from_catalog(database = args["glue_database_name"], table_name = f"{args['postgres_db_name']}_genphensql_phenotypic_drug_susceptibility_test_category").alias("phenotypes_category")

    sample = glueContext.create_data_frame.from_catalog(database = args["glue_database_name"], table_name = f"{args['postgres_db_name']}_genphensql_sample")

    summary_stats = glueContext.create_data_frame.from_catalog(database = args["glue_database_name"], table_name = f"{args['postgres_db_name']}_public_submission_summarysequencingstats")

    seq_data = glueContext.create_data_frame.from_catalog(database = args["glue_database_name"], table_name = f"{args['postgres_db_name']}_genphensql_sequencing_data")

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

    variant_category = tiered_drug_variant_categories(fapg, tier, variant, mvd, promoter_distance).alias("vc")

    genotype = glueContext.create_data_frame.from_catalog(database = args["glue_database_name"], table_name = f"{args['postgres_db_name']}_public_submission_genotype").alias("genotype")

    drug = glueContext.create_data_frame.from_catalog(database = args["glue_database_name"], table_name = f"{args['postgres_db_name']}_public_genphen_drug").alias("drug")

    term = glueContext.create_data_frame.from_catalog(database = args["glue_database_name"], table_name = "postgres_biosql_term").alias("term1")

    seqfeature = glueContext.create_data_frame.from_catalog(database = args["glue_database_name"], table_name = "postgres_biosql_seqfeature").alias("seqfeature")

    gene_name = gene_or_locus_tag_view(sdc, sqv, seqfeature, term, "gene_symbol").alias("gene")

    additional_variant_information = glueContext.create_data_frame.from_catalog(database = args["glue_database_name"], table_name = f"{args['postgres_db_name']}_genphensql_additional_variant_information").alias("additional_info").where(F.col("description")=="merker_neutral_variant")

    clean_phenotypes = (
        join_phenotypes_with_categories(phenotypes, phenotypes_category)
        .where(
            F.col("phenotypes_category.category").isin(
                ["WHO_current", "WHO_past"]
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
                )
            )
        )
        .where(F.col("rank")==1)
        .groupBy(
            F.col("phenotypes.sample_id"),
            F.col("phenotypes.drug_id"),
            F.col("phenotypes_category.category"),
        )
        .agg(F.concat_ws("", F.collect_set(F.col("phenotypes.test_result"))).alias("concat_results"))
        .where(F.col("concat_results").rlike("^(R+|S+)$"))
        .select(
            F.col("phenotypes.sample_id"),
            F.col("phenotypes.drug_id"),
            F.col("concat_results").substr(1, 1).alias("phenotype"),
        )
        .alias("clean_phenotypes")
    )

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
        .select(F.col("sample_id"))
        .distinct()
        .alias("filtered_samples")
    )

#    data = get_neutral_variants(filtered_samples, clean_phenotypes, variant_category, genotype, 0.1, additional_variant_information, drug, gene_name)

    locus_tag = gene_or_locus_tag_view(sdc, sqv, seqfeature, term, "rv_symbol").alias("locus_tag")

    gene_locus_tag = merge_gene_locus_view(gene_name, locus_tag).alias("gene_locus_tag")

#    write_neutral_variants_to_excel(data[0], data[1], drug, locus_tag, gene_locus_tag, data[2], "normal", d)

    print(variant_category.count())

    variant_category_no_syn = (
        variant_category
        .where(
            F.col("predicted_effect")!="synonymous_variant"
            )
    )

    print(variant_category_no_syn.count())

    data_no_syn = get_neutral_variants(filtered_samples, clean_phenotypes, variant_category_no_syn, genotype, 0.1, additional_variant_information, drug, gene_name)

    write_neutral_variants_to_excel(data_no_syn[0], data_no_syn[1], drug, locus_tag, gene_locus_tag, data_no_syn[2], "no-syn", d)