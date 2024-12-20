import sys

from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import functions
from pyspark.sql.functions import col, when, lit


sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "glue_database_name",
    "rds_database_name",
    "rds_glue_connection_name"
])

job.init(args['JOB_NAME'], args)

dbname = args["rds_database_name"]
rds_connector_name = args["rds_glue_connection_name"]
conn = glueContext.extract_jdbc_conf(rds_connector_name)
glue_db_name = args["glue_database_name"]

# materialized views cannot be obtained through glue database,
# so we query RDS directly
preferred_annotation = glueContext.create_data_frame.from_options(
    connection_type="postgresql",
    connection_options={
        "url": conn['url'] + "/" + dbname,
        "user": conn['user'],
        "password": conn['password'],
        "dbtable": "public.genphen_preferredannotation",
    },
    transformation_ctx="preferred_annotation"
)

drug_association = spark.createDataFrame([
    ('katG', 'Isoniazid'),
    ('fabG1', 'Isoniazid'),
    ('fabG1', 'Ethionamide'),
    ('inhA', 'Ethionamide'),
    ('ethA', 'Ethionamide'),
    ('rpoB', 'Rifampicin'),
    ('pncA', 'Pyrazinamide'),
    ('gyrA', 'Levofloxacin'),
    ('gyrA', 'Moxifloxacin'),
    ('gyrB', 'Levofloxacin'),
    ('gyrB', 'Moxifloxacin'),
    ('tlyA', 'Capreomycin'),
    ('ddn', 'Delamanid'),
    ('rplC', 'Linezolid'),
    ('rpsL', 'Streptomycin'),
    ('gid', 'Streptomycin'),
    ('embB', 'Ethambutol'),
    ('embA', 'Ethambutol')
], ['gene_name', 'drug'])

drug_association_eis = spark.createDataFrame([
    ('eis', 'Amikacin', 'c.-14C>T'),
    ('eis', 'Kanamycin', 'c.-14C>T'),
    ('eis', 'Kanamycin', 'c.-8delC'),
    ('eis', 'Kanamycin', 'c.-10G>A'),
    ('eis', 'Kanamycin', 'c.-12C>T'),
    ('eis', 'Kanamycin', 'c.-37G>T'),
    ('rrs', 'Streptomycin', 'n.517C>T'),
    ('rrs', 'Streptomycin', 'n.514A>C'),
    ('rrs', 'Streptomycin', 'n.878G>A'),
    ('rrs', 'Amikacin', 'n.1401A>G'),
    ('rrs', 'Amikacin', 'n.1402C>T'),
    ('rrs', 'Amikacin', 'n.1484G>T'),
    ('rrs', 'Capreomycin', 'n.1401A>G'),
    ('rrs', 'Capreomycin', 'n.1402C>T'),
    ('rrs', 'Capreomycin', 'n.1484G>T'),
    ('rrs', 'Kanamycin', 'n.1401A>G'),
    ('rrs', 'Kanamycin', 'n.1402C>T'),
    ('rrs', 'Kanamycin', 'n.1484G>T')
], ['gene_name_eis', 'drug_eis', 'hgvs_value_eis'])

df = preferred_annotation.join(drug_association, 'gene_name', 'left')

cond = [
    df.gene_name == drug_association_eis.gene_name_eis,
    df.hgvs_value == drug_association_eis.hgvs_value_eis
]
df = df.join(drug_association_eis, cond, 'left')

predicted_effect_rlike_sgslfv = df.predicted_effect.rlike(
    '^.*(stop_gained|start_lost|frameshift_variant).*$'
)

df = df.filter(
    (
            (df.gene_name == 'katG') & (
            predicted_effect_rlike_sgslfv |
            df.hgvs_value.isin(['p.Ser315Thr', 'p.Ser315Asn', 'p.Trp328Leu'])
    )
    ) | (
            (df.gene_name == 'fabG1') & (
        df.hgvs_value.isin(['c.-16A>G', 'c.-15C>T', 'c.-8T>C', 'c.-8T>A', 'c.609G>A'])
    )
    ) | (
            (df.gene_name == 'rpoB') &
            (df.predicted_effect != 'synonymous_variant') & (
                    (
                            (df.mii >= 426) &
                            (df.mii <= 452)
                    ) |
                    df.hgvs_value.isin(['p.Val170Phe', 'p.Ile491Phe'])
            )
    ) | (
            (df.gene_name == 'pncA') & (
            predicted_effect_rlike_sgslfv | (
            (df.predicted_effect == 'upstream_gene_variant') &
            df.hgvs_value.isin(['c.-12T>C',
                                'c.-11A>G',
                                'c.-11A>C',
                                'c.-11A>T',
                                'c.-7T>C',
                                'c.-5delG'])
    ) | (
                    ~df.predicted_effect.isin(['upstream_gene_variant',
                                               'downstream_gene_variant',
                                               'synonymous_variant',
                                               'initiator_codon_variant',
                                               'stop_retained_variant']) &
                    ~df.hgvs_value.isin(['p.Ile6Leu',
                                         'p.Glu15Gly',
                                         'p.Val21Ala',
                                         'p.Leu35Arg',
                                         'p.Ser66Leu',
                                         'p.Ala79Thr',
                                         'p.Ala79Val',
                                         'p.Thr87Met',
                                         'p.Thr114Met',
                                         'p.Asp136Asn',
                                         'p.Thr168Ile',
                                         'p.Ala171Val'])
            )
    )
    ) | (
            (df.gene_name == 'eis') &
            df.hgvs_value.isin(['c.-14C>T',
                                'c.-8delC',
                                'c.-10G>A',
                                'c.-12C>T',
                                'c.-37G>T'])
    ) | (
            (df.gene_name == 'rrs') &
            df.hgvs_value.isin(['n.1401A>G',
                                'n.1402C>T',
                                'n.1484G>T',
                                'n.517C>T',
                                'n.514A>C',
                                'n.878G>A'])
    ) | (
            (df.gene_name == 'gyrA') &
            df.hgvs_value.isin(['p.Gly88Cys',
                                'p.Gly88Ala',
                                'p.Ala90Val',
                                'p.Ser91Pro',
                                'p.Asp94Tyr',
                                'p.Asp94His',
                                'p.Asp94Asn',
                                'p.Asp94Gly',
                                'p.Asp94Ala'])
    ) | (
            (df.gene_name == 'gyrB') &
            df.hgvs_value.isin(['p.Asp461Asn',
                                'p.Asn499Asp',
                                'p.Glu501Val',
                                'p.Glu501Asp',
                                'p.Ala504Val'])
    ) | (
            (df.gene_name == 'tlyA') & (
            predicted_effect_rlike_sgslfv |
            df.hgvs_value.isin(['p.Leu74Pro',
                                'p.Gly232Asp',
                                'p.Asn236Lys'])
    )
    ) | (
            (df.gene_name == 'ddn') &
            (df.hgvs_value == 'p.Leu49Pro')
    ) | (
            (df.gene_name == 'rplC') &
            (df.hgvs_value == 'p.Cys154Arg')
    ) | (
            (df.gene_name == 'rpsL') &
            df.hgvs_value.isin(['p.Lys43Arg',
                                'p.Lys88Arg',
                                'p.Lys88Met',
                                'p.Lys88Gln'])
    ) | (
            (df.gene_name == 'gid') & (
            predicted_effect_rlike_sgslfv |
            df.hgvs_value.isin(['p.Gly69Asp',
                                'p.Ala134Glu',
                                'p.Pro75Arg',
                                'p.Pro84Leu',
                                'p.Gly73Ala'])
    )
    ) | (
            (df.gene_name == 'embB') &
            df.hgvs_value.isin(['p.Met306Val',
                                'p.Met306Ile',
                                'p.Gln497Arg',
                                'p.Asp354Ala',
                                'p.Gly406Ala',
                                'p.Tyr319Ser',
                                'p.Gly406Asp',
                                'p.Gly406Ser',
                                'p.Met306Leu',
                                'p.Gln497Lys',
                                'p.Asp328Tyr',
                                'p.Gly406Cys',
                                'p.Tyr319Cys',
                                'p.Leu74Arg'])
    ) | (
            (df.gene_name == 'embA') &
            (df.hgvs_value == 'c.-12C>T')
    ) | (
            (df.gene_name == 'ethA') & (
            predicted_effect_rlike_sgslfv |
            df.hgvs_value.isin(['p.Thr88Ile',
                                'p.Arg207Gly',
                                'c.-7T>C',
                                'p.Pro378Leu',
                                'p.Ser390Phe',
                                'p.Ala341Val',
                                'p.Ser57Tyr',
                                'p.Tyr32Asp',
                                'p.Gly11Val'])
    )
    ) | (
            (df.gene_name == 'inhA') &
            (df.hgvs_value == 'p.Ser94Ala')
    )
)
excluded_hgvs_values = [
    'p.Leu182Trp',
    'p.Leu182Ser',
    'p.Val180Gly',
    'p.Val180Ala',
    'p.Ala178_Ser179del',
    'p.Val180Phe',
    'p.Thr177Pro',
    'p.Met175Thr',
    'p.Met175Val',
    'p.Leu172Pro',
    'p.Leu172Arg',
    'p.Thr160_Ala171del',
    'p.Ala171Glu',
    'p.Thr168Pro',
    'p.Ala165del',
    'p.Ser164Pro',
    'p.Gly162Asp',
    'p.Thr160Pro',
    'p.Leu159Arg',
    'p.Val155dup',
    'p.Val155Gly',
    'p.Val155Met',
    'p.Arg154Gly',
    'p.Leu151Ser',
    'p.Ala146Val',
    'p.Ala146Thr',
    'p.Ala143Gly',
    'p.Thr142Met',
    'p.Thr142Lys',
    'p.Thr142Ala',
    'p.Gln141Pro',
    'p.Val139Gly',
    'p.Val139Ala',
    'p.Val139Leu',
    'p.Cys138Arg',
    'p.Gly132_Thr135del',
    'p.Thr135Pro',
    'p.Thr135Asn',
    'p.Ala134Val',
    'p.Val128_Val130del',
    'p.Ile133Thr',
    'p.Val125_Asp129del',
    'p.Gly132Ala',
    'p.Asp129_Val131del',
    'p.Gly132Asp',
    'p.Gly132Ser',
    'p.Val131Gly',
    'p.Val131Phe',
    'p.Asp126_Val130del',
    'p.Glu127_Asp129del',
    'p.Val130Gly',
    'p.Val128Gly',
    'p.Val125Gly',
    'p.Val125Phe',
    'p.Leu120Pro',
    'p.Leu120Arg',
    'p.Leu120Gln',
    'p.Trp119Cys',
    'p.Trp119Gly',
    'p.Trp119Arg',
    'p.Leu116Pro',
    'p.Leu116Arg',
    'p.Ser104_Gly108delinsArg',
    'p.Gly108Arg',
    'p.Phe106Ser',
    'p.Ala102_Gly105del',
    'p.Gly105Val',
    'p.Gly105Asp',
    'p.Ser104Arg',
    'p.Ser104Ile',
    'p.Tyr103Cys',
    'p.Tyr103Ser',
    'p.Tyr103His',
    'p.Tyr103Asp',
    'p.Ala102Pro',
    'p.Gly97Val',
    'p.Gly97Asp',
    'p.Gly97Cys',
    'p.Gly97Arg',
    'p.Gly97Ser',
    'p.Lys96delinsAsnLeuAlaIleSerAsnValIleValAspGlyVal',
    'p.Lys96Arg',
    'p.Lys96Thr',
    'p.Lys96Glu',
    'p.Lys96Gln',
    'p.Phe94Cys',
    'p.Phe94Ser',
    'p.Phe94Leu',
    'p.Ala89_Ala92dup',
    'p.Ile90Thr',
    'p.Ile90Ser',
    'p.Leu85Arg',
    'p.Leu85Pro',
    'p.His82Arg',
    'p.Phe81Val',
    'p.Ser74_Pro77del',
    'p.Thr76Ile',
    'p.Thr76Pro',
    'p.Cys72Tyr',
    'p.Cys72Arg',
    'p.His71Pro',
    'p.His71Arg',
    'p.His71Tyr',
    'p.His71Thr',
    'p.Pro69Leu',
    'p.Trp68Cys',
    'p.Trp68Gly',
    'p.Trp68Arg',
    'p.Asp63_Ser67delinsGlu',
    'p.Gly60_Thr61del',
    'p.Ser67Pro',
    'p.Ser66del',
    'p.Tyr64Asp',
    'p.Asp63Gly',
    'p.Asp63Ala',
    'p.Pro62_Asp63insAspTyrSer',
    'p.Pro62Leu',
    'p.Pro62Ser',
    'p.Pro62Thr',
    'p.Ser59Pro',
    'p.Phe58Leu',
    'p.His57Arg',
    'p.Pro54_Asp56del',
    'p.His57Asp',
    'p.His57Tyr',
    'p.Gly55del',
    'p.Pro54Gln',
    'p.Pro54Leu',
    'p.His51Gln',
    'p.His51Arg',
    'p.His51Pro',
    'p.His51Asp',
    'p.His51Tyr',
    'p.Asp49Glu',
    'p.Asp49Ala',
    'p.Asp49Gly',
    'p.Asp49Asn',
    'p.Lys48Thr',
    'p.Lys48Glu',
    'p.Thr47Pro',
    'p.Thr47Ala',
    'p.Ala46Glu',
    'p.Ala46Val',
    'p.Val44Gly',
    'p.Tyr34Asp',
    'p.Ile31Ser',
    'p.Ile31Thr',
    'p.Leu27Pro',
    'p.Gly17_Ala25del',
    'p.Gly24Asp',
    'p.Val21Gly',
    'p.Leu19Pro',
    'p.Gly17Asp',
    'p.Cys14Arg',
    'p.Asp12_Phe13insAlaSerGluProProSerGlnLysSer',
    'p.Phe13Leu',
    'p.Phe13Ile',
    'p.Asp12Glu',
    'p.Asp12Gly',
    'p.Asp12Ala',
    'p.Asp12Asn',
    'p.Gln10His',
    'p.Gln10Arg',
    'p.Gln10Pro',
    'p.Val9Gly',
    'p.Asp8Glu',
    'p.Asp8Ala',
    'p.Asp8Gly',
    'p.Asp8Asn',
    'p.Val7Gly',
    'p.Leu4_Val7delinsPheIleIle',
    'p.Val7Ala',
    'p.Val7Leu',
    'p.Ile6Thr',
    'p.Ala3_Ile5del',
    'p.Ile5Ser',
    'p.Leu4Trp',
    'p.Leu4Ser',
    'p.Ala3Glu'
]

pza_eq_rif_cond = (
        ~df.gene_name.isNull()
        & (df.gene_name == 'pncA')
        & (df.predicted_effect != 'upstream_gene_variant')
        & ~df.predicted_effect.rlike('^.*(stop_gained|start_lost|frameshift_variant).*$')
        & ~df.hgvs_value.isin(excluded_hgvs_values)
)

variant_list = df.select(
    df.variant_id,
    functions.coalesce(df.drug, df.drug_eis).alias("drug"),
    functions.when(pza_eq_rif_cond, 1).otherwise(0).alias("pza-equals-to-rif-flag"),
    functions.coalesce(df.gene_name, df.gene_name_eis).alias("gene_name"),
    functions.coalesce(df.hgvs_value, df.hgvs_value_eis).alias("hgvs_value")
)

print('--- "variant_list" sub-select ---')
variant_list.show(10)


# sample_list
submission_package = glueContext.create_data_frame.from_catalog(
    database=glue_db_name,
    table_name=dbname + "_public_submission_package"
).alias("package")

print('--- "public_submission_package" table ---')
submission_package.show(10)

sample_alias = glueContext.create_data_frame.from_catalog(
    database=glue_db_name,
    table_name=dbname + "_public_submission_samplealias"
).alias("samplealias")

print('--- "public_submission_samplealias" table ---')
sample_alias.show(10)

sample = glueContext.create_data_frame.from_catalog(
    database=glue_db_name,
    table_name=dbname + "_public_submission_sample"
).alias("sample")

print('--- "public_submission_sample" table ---')
sample.show(10)

sequencing_data = glueContext.create_data_frame.from_catalog(
    database=glue_db_name,
    table_name=dbname + "_public_submission_sequencingdata"
).alias("sequencing_data")

print('--- "public_submission_sequencingdata" table ---')
sequencing_data.show(10)


df = submission_package.join(
    sample_alias,
    submission_package.id == sample_alias.package_id,
    "inner"
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

print('--- "public_submission_locussequencingstats" table ---')
locus_sequencing_stats.show(10)

df = df.join(sample, df.sample_id == sample.id, "inner")
df = df.join(sequencing_data, df.sample_id == sequencing_data.sample_id, "inner")
df = df.filter((
        # functions.col("package.name").rlike('^.*SeqTreat_PhaseII.*$') &
        (functions.col("samplealias.name") != 'TB03805142') &
        (functions.col("sample.bioanalysis_status") == 'Annotated')
))

sample_list = (
    df
    .join(
        locus_sequencing_stats,
        on=functions.col("sample.id")==functions.col("locus_sequencing_stats.sample_id"),
        how="inner"
    )
    .select(functions.col("sample.id").alias("sample_id"))
    .distinct()
)


print('--- "sample_list" sub-select ---')
sample_list.show(10)
print('--- "sample_list" sub-select count---')
sample_list.count()


# cov

gene_name = glueContext.create_data_frame.from_catalog(
    database=glue_db_name,
    table_name=dbname + "_public_genphen_genename"
).alias("gene_name")

print('--- "public_genphen_genename" table ---')
gene_name.show(10)


df = locus_sequencing_stats.join(sample_list, "sample_id", "inner")
df = df.join(gene_name, "gene_db_crossref_id", "inner")

df = df.filter((
        df.gene_name.isin('pncA', 'katG', 'tlyA', 'gid', 'ethA')
        & (df.coverage_10x < 0.95)
))

cov = df.select(
    df.sample_id,
    functions.when(df.gene_name == "katG", "Isoniazid") \
        .when(df.gene_name == "pncA", "Pyrazinamide") \
        .when(df.gene_name == "tlyA", "Capreomycin") \
        .when(df.gene_name == "gid", "Streptomycin") \
        .when(df.gene_name == "ethA", "Ethionamide").alias("drug"),
    df.coverage_10x,
    df.gene_name,
    functions.lit("deletion").alias("hgvs_value")
)

print('--- "cov" sub-select ---')
cov.show(10)


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

print('--- "submission_genotype" table ---')
genotype.show(10)

df = sample_list.join(genotype, "sample_id", "inner")
df = df.join(variant_list, "variant_id", "inner")
df = df.filter((((df.alternative_ad.cast("float") / df.total_dp.cast("float")) > 0.5) | (
            df.gene_name != "rrs")) & (df.genotyper == "freebayes"))

genotypes_of_interest = df.select(
    df.sample_id,
    df.drug,
    df.variant_id,
    df["pza-equals-to-rif-flag"],
    df.gene_name,
    df.hgvs_value
)

print('--- "genotypes_of_interest" sub-select ---')
genotypes_of_interest.show(10)


# uncovered
df = sample_list.join(locus_sequencing_stats, "sample_id", "inner")
df = df.join(gene_name, "gene_db_crossref_id", "inner")
df = df.crossJoin(spark.createDataFrame([
    ('Amikacin',),
    ('Capreomycin',),
    ('Kanamycin',),
    ('Streptomycin',),
], ['drug']))
df = df.filter(
    (df.coverage_10x < 0.95)
    & (df.mean_depth < 15)
    & (df.gene_name == 'rrs')
)
uncovered = df.select(
    df.sample_id,
    df.drug,
    functions.lit(1).alias("unknown-flag")
)

print('--- "uncovered" sub-select ---')
uncovered.show(10)


# grouped
drug_list = spark.createDataFrame([
    ('Rifampicin',),
    ('Isoniazid',),
    ('Pyrazinamide',),
    ('Amikacin',),
    ('Capreomycin',),
    ('Kanamycin',),
    ('Levofloxacin',),
    ('Moxifloxacin',),
    ('Linezolid',),
    ('Streptomycin',),
    ('Ethionamide',),
    ('Ethambutol',)
], ['drug'])
cartesian_product = sample_list.crossJoin(drug_list)
df = cartesian_product.join(genotypes_of_interest.alias("goi"), ["sample_id", "drug"], "left")
df = df.join(cov.alias("cov"), ["sample_id", "drug"], "left")

df = df.withColumn("gene_name_fin", functions.coalesce("cov.gene_name", "goi.gene_name"))
df = df.withColumn("hgvs_value_fin", functions.coalesce("cov.hgvs_value", "goi.hgvs_value"))
df = df.withColumn("gene_name_hgvs_value", functions.expr("gene_name_fin || '_' || hgvs_value_fin"))

gp = df.groupBy("sample_id", "drug")


def bool_or(expr):
    return functions.count(
        functions.when(expr, functions.lit(1))
    ) > 0


def bool_and(expr):
    return functions.count(
        functions.when(expr, functions.lit(1))
    ) == functions.count("*")


grouped = gp.agg(
    # https://stackoverflow.com/questions/40709781/postgressql-bool-or-equivalent-in-spark
    (bool_or(functions.col("pza-equals-to-rif-flag") == 0) | bool_or(
        ~functions.col("coverage_10x").isNull())).alias("resistance_flag"),
    bool_and(functions.col("pza-equals-to-rif-flag") == 1).alias("pza-equals-to-rif-flag"),
    # https://stackoverflow.com/questions/73226020/string-aggregation-and-group-by-in-pyspark
    functions.expr(
        "concat_ws("
        "';', "
        "sort_array(collect_list(struct(gene_name_fin, gene_name_hgvs_value))).gene_name_hgvs_value)"
    ).alias("variant")
)

print('--- "grouped" sub-select ---')
grouped.show(10)


# final select
g1 = grouped.alias("g1")
g2 = grouped.alias("g2")

g2_cond = [
    functions.col("g1.drug") == "Pyrazinamide",
    ~g1["pza-equals-to-rif-flag"].isNull(),
    functions.col("g2.drug") == 'Rifampicin',
    functions.col("g1.sample_id") == functions.col("g2.sample_id")
]

print('### g1 before join ###')
g1.show(2)

print('### g2 before join ###')
g2.show(2)

df = g1.join(g2, g2_cond, "left")

print('### df after join ###')

df = df.select(
    col('g1.sample_id'),
    col('g1.drug'),
    col('g1.variant'),
    col('g1.pza-equals-to-rif-flag'),
    col('g1.resistance_flag').alias('g1_resistance_flag'),
    col('g2.resistance_flag').alias('g2_resistance_flag'),
)

df = df.join(sample, df.sample_id == sample.id, "inner")

df = df.join(uncovered.alias('uncovered'), ['sample_id', 'drug'], "left")
df = df.sort('drug', "sample.id")

print('--- Joined and sorted data for Final Select Statement ---')
df.show(10)


# drug_name to drug_id
drug = glueContext.create_data_frame.from_catalog(
    database=glue_db_name,
    table_name=dbname + "_public_genphen_drug"
)
df = df.join(drug, [df.drug == drug.drug_name], 'left')


result = df.select(
    col('sample_id'),
    col('drug_id'),
    lit(1).alias("version"),
    when(col('g1_resistance_flag'), 'R')\
        .when(col('pza-equals-to-rif-flag') & col('g2_resistance_flag'), 'R')\
        .when(col('uncovered.unknown-flag') == 1, 'U')\
        .otherwise('S').alias('resistance_flag'),
    when(col('pza-equals-to-rif-flag') & (col('g2_resistance_flag').isNull() | ~col('g2_resistance_flag')), "")\
        .otherwise(col('variant')).alias('variant')
)

# filtering out empty variants (FIXME how they managed to stay here?)
# 2024-07-18 Sacha: fixed. Removing the line
# result = result.filter(~result.variant.isNull())

print('--- Final Select Statement ---')
result.show(10)
print(f'--- Final Select Statement rows count: {result.count()} ---')


result_dynamic_frame = (
    DynamicFrame.fromDF(
        result,
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

# overwriting whole table
# not doing that anymore, trying to use bookmarks.
# (
#     result.write
#         .mode('append')
#         .format("jdbc")
#         .option("connschema", dbname)
#         .option("url", conn['url'])
#         .option("database", dbname)
#         .option("dbtable", "public.submission_genotyperesistance")
#         .option("user", conn['user'])
#         .option('truncate', 'true')
#         .option("password", conn['password'])
#         .save()
# )


# job.commit()
