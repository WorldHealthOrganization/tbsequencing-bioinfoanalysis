import json, urllib.parse, boto3, datetime, os, re

def lambda_handler(event, context):
    
    s3 = boto3.client('s3')

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    try:
        metadata = s3.head_object(Bucket=bucket, Key=key)
        submission_date = metadata["LastModified"].date()
        content_type = metadata["ContentType"]
        if content_type == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
            batch = boto3.client('batch')

            batch.submit_job(
                jobName="ETL-" + re.compile(r'[\W]+').sub('', "_".join(key.split()).replace(".", "_").replace("/", "_"))[:110].encode("ascii", "ignore").decode(),
                jobQueue=os.environ["JOB_QUEUE"],
                jobDefinition=os.environ["JOB_DEF"],
                containerOverrides = {
                    "command" : [
                        "/scripts/sample_data_preparation.py",
                        "--db_host",
                        os.environ["DB_HOST"],
                        "--db_name",
                        os.environ["DB_NAME"],
                        "--db_user",
                        os.environ["DB_USER"],
                        "--db_port",
                        os.environ["DB_PORT"],
                        "--db_aws_region",
                        os.environ["AWS_REGION"],
                        "--submission_date",
                        submission_date.isoformat(),
                        "--data_format",
                        content_type,
                        "--data_file_bucket",
                        bucket
                        ],
                    "environment": [
                        {
                        "name": "FILE_NAME",
                        "value": key
                        }
                    ]
                }
            )
    except Exception as e:
        print(e)
        raise e
