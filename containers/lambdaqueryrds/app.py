import boto3, psycopg2

def run_query(conn, curr, event):
    try:
        curr.execute(event["Query"], tuple(event["QueryParams"]))
    except KeyError:
        curr.execute(event["Query"])
    return(curr.fetchall())

def lambda_handler(event, context):
    rds_client = boto3.client('rds', region_name=event["DbConnection"]["Region"])
    token = rds_client.generate_db_auth_token(
        DBHostname=event["DbConnection"]["Endpoint"],
        Port=event["DbConnection"]["Port"],
        DBUsername=event["DbConnection"]["User"],
        Region=event["DbConnection"]["Region"]
    )
    conn = psycopg2.connect(
        host=event["DbConnection"]["Endpoint"],
        port=event["DbConnection"]["Port"],
        database=event["DbConnection"]["Name"],
        user=event["DbConnection"]["User"],
        password=token
    )
    curr = conn.cursor()
    return(globals()[event["CalledFunction"]](conn, curr, event))