import boto3, hashlib, json

def lambda_handler(event, context):
    
    for event_records in event["Records"]:
        message_records = json.loads(event_records["body"])["Records"]
        for message in message_records: 
            s3 = boto3.client('s3', message["awsRegion"])
            
            bucket = message['s3']['bucket']['name']
            key = message['s3']['object']['key']

            try:
                data = s3.get_object(Bucket=bucket, Key=key)
                if (key.endswith("fastq.gz") or key.endswith("fq.gz")):
                    content = data["Body"].read()
                    md5_value = hashlib.md5(content).hexdigest()
                    s3.put_object_tagging(
                        Bucket=bucket,
                        Key=key,    
                        Tagging={
                            'TagSet': [
                                {
                                    'Key': 'md5sum',
                                    'Value': md5_value
                                },
                            ]
                        }
                    )
            except Exception as e:
                print(e)
                raise e