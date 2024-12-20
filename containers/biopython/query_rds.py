import argparse, psycopg2, boto3, os, pandas

def main(arguments):
    rds_client = boto3.client('rds', region_name=arguments.aws_region)
    token = rds_client.generate_db_auth_token(DBHostname=arguments.db_host, Port=arguments.db_port, DBUsername=arguments.db_user)

    keepalive_kwargs = {
        "keepalives": 1,
        "keepalives_idle": 5,
        "keepalives_interval": 5,
        "keepalives_count": 5,
    }

    conn = psycopg2.connect(host=arguments.db_host, port=arguments.db_port, database=arguments.db_name, user=arguments.db_user, password=token, **keepalive_kwargs)

    curr = conn.cursor()

    if arguments.file.endswith(".sql"):
        curr.execute(open("/data/"+arguments.file, 'r').read())
    else:
        curr.execute(os.environ["RDS_QUERY"])
        try:
            outfile = os.environ["RDS_QUERY_OUTPUT"]
            pandas.DataFrame(curr.fetchall(), columns=["Chromosome", "Position", "ID", "Ref", "Alt"]).to_csv(outfile, sep="\t", header=False, index=False)
        except KeyError:
            pass

    conn.commit()
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db_host", help="AWS RDS database endpoint")
    parser.add_argument("--db_name", help="Database name")
    parser.add_argument("--db_user", help="Database user name (with AWS RDS IAM authentication)")
    parser.add_argument("--db_port", help="Database port")
    parser.add_argument("--aws_region", help="Database AWS region location")
    parser.add_argument("--file", help="File to be loaded", default="")
    args = parser.parse_args()

    main(args)
