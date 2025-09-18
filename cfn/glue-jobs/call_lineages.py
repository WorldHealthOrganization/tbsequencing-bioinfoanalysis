import pyspark.sql.functions as F


def assign_lineage_markers_to_variant_id(lineage_markers, variant, dbname, conn):

    unknown_markers = (
        lineage_markers
        .where(
            F.col("variant_id").isNull()
        )
    )

    # if we have newly inserted markers (via the Django admin)
    # we assign the variant id to the markers
    # and resinsert them
    if unknown_markers.count():

        lineage_markers = (
            lineage_markers
            .join(
                variant,
                on=
                    (F.col("variant.position")==F.col("lineage_marker.position"))
                    & (F.col("variant.alternative_nucleotide")==F.col("lineage_marker.alternative_nucleotide"))
                    & (F.length(F.col("variant.reference_nucleotide"))==1),
                how="inner"
            )
            .select(
                F.col("variant.variant_id"),
                F.col("variant.position"),
                F.col("variant.alternative_nucleotide"),
                F.col("count_presence"),
                F.col("lineage_id"),
            )
        )


    # overwriting the table with the variant id column filled
    (
        lineage_markers
        .write
        .mode('overwrite')
        .format("jdbc")
        .option("connschema", dbname)
        .option("url", conn['fullUrl'])
        .option("dbtable", "public.genphen_variantlineageassociation")
        .option("user", conn['user'])
        .option('truncate', 'true')
        .option("password", conn['password'])
        .option("driver", "org.postgresql.Driver")
        .save()
    )


def generate_lineage_marker_counts(lineage_markers, lineage_name, sample, genotype):


    # only keep the column of interest
    # we need the position for the grouping of absent alleles
    # otherwise we would only need variant id
    lineage_markers = (
        lineage_markers
        .select(
            F.col("variant_id"),
            F.col("position"),
            F.col("count_presence"),
            F.col("lineage_id"),
        )
        .alias("marker")
    )

    # we use genotypes from bcftools so that 
    # we don't have to deal with MCNV
    sample_x_markers = (
        sample.
        crossJoin(
            lineage_markers
        )
        .join(
            genotype,
            on = 
                (F.col("sample.id")==F.col("genotype.sample_id"))
                & (F.col("genotype.variant_id")==F.col("marker.variant_id"))
                & (F.col("genotype.genotyper")=="bcftools"),
            how="left"
        )
        .withColumn(
        "af",
            F.bround(F.col("alternative_ad").cast("long")/F.col("total_dp").cast("long"), 2)
        )
        .fillna(
            0,
            subset="af"
        )
        .groupBy(
            "sample_id",
            "position",
            "count_presence",
            "lineage_id",
        )
        .agg(
            {"af": "sum"}
        )
        .withColumn(
            "final_af",
            F.when(F.col("count_presence"), "sum(af)")
            .otherwise(1-F.col("sum(af)"))
        )
        .select(
            F.col("sample_id"),
            F.col("position"),
            F.col("lineage_id"),
            F.col("final_af")
        )
    )

    final = (
        sample_x_markers
        .join(
            lineage_name,
            on=F.col("lineage_id")==F.col("lineage_name.id"),
            how="inner"
        )
    )

    return(final)
