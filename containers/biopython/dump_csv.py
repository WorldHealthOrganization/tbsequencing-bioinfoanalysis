import argparse, psycopg2, boto3, os, pandas


RAW_QUERY = """
WITH RankedAliases AS (
    SELECT
        sample_id,
        name,
        ROW_NUMBER() OVER (PARTITION BY sample_id ORDER BY id) as rn
    FROM submission_samplealias
    WHERE origin = 'BioSample'
    AND origin_label = 'Sample name'
)
SELECT
    ra.name as "sample_aliases_name",
    "genphen_drug"."drug_name",
    "submission_genotyperesistance"."resistance_flag",
    "submission_genotyperesistance"."variant"
FROM
    "submission_genotyperesistance"
INNER JOIN
    "submission_sample" ON ("submission_genotyperesistance"."sample_id" = "submission_sample"."id")
INNER JOIN
    "genphen_drug" ON ("submission_genotyperesistance"."drug_id" = "genphen_drug"."drug_id")
LEFT JOIN
    RankedAliases ra ON ("submission_sample"."id" = ra.sample_id AND ra.rn = 1)
WHERE (
    "submission_sample"."origin" = 'NCBI'
    AND "submission_genotyperesistance"."drug_id" IN (1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 13, 14, 15, 16, 17, 18)
    AND COALESCE("submission_genotyperesistance"."version", 1) = 2
)
ORDER BY
    "submission_genotyperesistance"."id" ASC
"""


def main(arguments):
    rds_client = boto3.client("rds", region_name=arguments.aws_region)
    token = rds_client.generate_db_auth_token(
        DBHostname=arguments.db_host,
        Port=arguments.db_port,
        DBUsername=arguments.db_user,
    )

    keepalive_kwargs = {
        "keepalives": 1,
        "keepalives_idle": 5,
        "keepalives_interval": 5,
        "keepalives_count": 5,
    }

    conn = psycopg2.connect(
        host=arguments.db_host,
        port=arguments.db_port,
        database=arguments.db_name,
        user=arguments.db_user,
        password=token,
        **keepalive_kwargs,
    )

    curr = conn.cursor()

    curr.execute(RAW_QUERY)
    try:
        outfile = "GenotypeResistance.csv"
        # Create DataFrame from query results
        df = pandas.DataFrame(
            curr.fetchall(),
            columns=["BioSample Accession", "Drug", "Genotypic Resistance", "Variant"],
        )

        # Save to local file first
        df.to_csv(outfile, sep="\t", header=True, index=False)

        # Upload to S3 with hardcoded path, need to change this to the correct bucket
        s3_bucket = os.environ["S3_BUCKET"].rsplit(":", 1)[-1]
        s3_key = "static/media/" + os.path.basename(outfile)

        s3_client = boto3.client("s3")
        s3_client.upload_file(outfile, s3_bucket, s3_key)
        print(f"File uploaded to s3://{s3_bucket}/{s3_key}")
    except KeyError as e:
        print(f"Environment variable not set: {e}")
    except Exception as e:
        print(f"Error saving or uploading file: {e}")

    conn.commit()
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db_host", help="AWS RDS database endpoint")
    parser.add_argument("--db_name", help="Database name")
    parser.add_argument(
        "--db_user", help="Database user name (with AWS RDS IAM authentication)"
    )
    parser.add_argument("--db_port", help="Database port")
    parser.add_argument("--aws_region", help="Database AWS region location")
    args = parser.parse_args()

    main(args)
