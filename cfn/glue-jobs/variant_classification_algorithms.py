import pyspark.sql.functions as F
from pyspark.sql.window import Window
from scipy import stats

# Get most frequent position (modal position) for each variant category
# As well as the predicted effect
# A bit touchy (think rpoB p.Thr444dup that has two different predicted effect
# Still it's working as intended at the moment
def most_frequent_position_by_category(genotype):
    most_frequent_position = (
        genotype.alias("genotype")
        .groupBy(
            F.col("gene_db_crossref_id"),
            F.col("variant_category"),
            F.col("position"),
            F.col("predicted_effect"),
        )
        .agg(
            F.countDistinct("sample_id").alias("genotype_count")
        )
        .withColumn(
            "row_number",
            F.row_number().over(
                Window.partitionBy(
                    F.col("gene_db_crossref_id"),
                    F.col("variant_category")
                )
                .orderBy(
                    F.desc("genotype_count")
                )
            )
        )
        .where(
            F.col("row_number")==1
        )
        .groupBy(
            F.col("gene_db_crossref_id"),
            F.col("variant_category"),
            F.col("position"),
        )
        # Keeping the first predicted effect at the moment.
        # TODO: Best implementation would keep the most frequent, as was done for position
        .agg(
            F.first("predicted_effect").alias("Effect")
        )
        .select(
            F.col("gene_db_crossref_id"),
            F.col("variant_category"),
            F.col("Effect"),
            F.col("position").alias("ModalPosition"),
        )
    )
    return(most_frequent_position)

def count_number_of_samples_per_category_phenotype(genotype, phenotype_values):
    counts = (
      genotype.alias("genotype")
        .groupBy(
            F.col("gene_db_crossref_id"),
            F.col("tier"),
            F.col("variant_category"),
            F.col("drug_id"),
        )
        .pivot(
            "phenotype",
            values=phenotype_values
        )
        .agg(F.countDistinct("sample_id"))
        .fillna(0)
    )
    return(counts)

def filter_variant_categories_on_upper_ppv(ppv, threshold, set):
    excluded = (
        ppv
        .where(
            F.col("Upper_PPV")<threshold
        )
        .drop(
            "R",
            "S"
        )
        .withColumn(
            "set",
            F.lit(set)
        )
    )
    return(excluded)


def classify_solo_samples(gen_cat_phen):
    solo = (
        gen_cat_phen
        .alias("genotype")
        # Variant category can be associated to only one (drug, tier). So no need to join on tier.
        .groupBy(
            F.col("sample_id"),
            F.col("drug_id"),
            F.col("tier"),
        )
        .agg(
            F.countDistinct("gene_db_crossref_id", "variant_category").alias("distinct_variant_category")
        )
        .groupBy(
            F.col("sample_id"),
            F.col("drug_id"),
        )
        # We pivot the tiers for each sample, drug so that we can select each pair easily
        .pivot(
            "tier",
            values=["1", "2"],
        )
        .agg(
            F.first("distinct_variant_category", ignorenulls=True)
        )
        .fillna(0)
        # Keep samples that have exactly one variant category in the first tier, or 0 in the first and 1 in the second.
        .where(
            (F.col("1")==1)
            | ((F.col("1")==0) & (F.col("2")==1))
        )
        # Do not forget to assign to each sample which tier it was selected for
        # That is to prevent select tier 2 for a sample that has been assigned to tier 1
        .select(
            F.col("sample_id"),
            F.col("drug_id"),
            F.when(F.col("1")==1, 1).otherwise(2).alias("tier"),
        )
    )
    return(solo)


def calculate_positive_predictive_value(counts):
    ppv = (
        counts
        # Simple PPV formula
        .withColumn(
            "PPV",
            F.col("R")/(F.col("R")+F.col("S"))
        )
        # And the upper bound of the PPV with the Clopper-Pearson exact formula and 95% interval (alpha=0.05) 
        .withColumn(
            "Upper_PPV",
            F.udf(lambda q, x, n: float(stats.beta.ppf(q, x, n)), "double")(F.lit(1-0.05/2), F.col("R")+F.lit(1), F.col("S"))
        )
    )
    return(ppv)
