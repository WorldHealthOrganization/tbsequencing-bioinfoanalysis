import pyspark.sql.functions as F

# Select samples based on their phenotype 
# Only keep clean samples (ie all results either R or S)
# For neutral mutation identification we only keep WHO current & past categories

def join_phenotypes_with_categories(phenotype, phenotype_category):
    joined_phenotype_category = (
        phenotype
        .join(
            other=phenotype_category,
            on=(F.col("phenotypes.drug_id")==F.col("phenotypes_category.drug_id")) 
            & (F.coalesce(F.col("phenotypes_category.medium_id"), F.lit(0))==F.coalesce(F.col("phenotypes.medium_id"), F.lit(0)))
            & (F.coalesce(F.col("phenotypes_category.method_id"), F.lit(0))==F.coalesce(F.col("phenotypes.method_id"), F.lit(0)))
            & (F.coalesce(F.col("phenotypes.concentration"), F.lit(0))==F.coalesce(F.col("phenotypes_category.concentration"), F.lit(0))),
            how="inner"
        )
    )
    return(joined_phenotype_category)

def binarize_mic_tests(spark_context, connection_url, db_user, token):
    binarized_mic = (
        spark_context.createDataFrame(
            spark_context.read.format("jdbc")
            .option("url", connection_url)
            .option("user", db_user)
            .option("password", token)
            .option("ssl", "true")
            .option("query", """
                SELECT mic.sample_id,
                    mic.drug_id,
                        CASE
                            WHEN upper(mic.mic_value)::double precision <= ecoff.value THEN 'S'::text
                            WHEN lower(mic.mic_value)::double precision > ecoff.value THEN 'R'::text
                            ELSE NULL::text
                        END AS result
                FROM minimum_inhibitory_concentration_test mic
                INNER JOIN genphen_epidemcutoffvalue ecoff ON mic.drug_id = ecoff.drug_id and mic.plate=ecoff.medium_name;
            """)
            .load()
            .select(
                F.col("mic.sample_id").alias("sample_id"),
                F.col("mic.drug_id").alias("drug_id"),
                F.col("mic.result").alias("result"),
            )
            .collect()
        )
    )
    return(binarized_mic)