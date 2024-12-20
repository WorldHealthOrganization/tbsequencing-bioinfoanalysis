import boto3, psycopg2

def run_query(conn, curr, event):
    try:
        query_params = event["QueryParams"]
        query_params = [i if not isinstance(i, list) else tuple(i) for i in query_params]
        curr.execute(event["Query"], tuple(query_params))
    except KeyError:
        curr.execute(event["Query"])
    conn.commit()
    try:
        return(curr.fetchall())
    except psycopg2.errors.ProgrammingError:
        return([])

def lambda_handler(event, context):
    if event.get("MockResponse"):
        fake_response = eval(event["MockResponse"])
        return fake_response
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