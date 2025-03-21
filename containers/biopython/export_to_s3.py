import logging
import boto3
from botocore.exceptions import ClientError
import argparse
import os


def main(args):
    bucket_name = args.output_bucket
    s3 = boto3.client('s3')

    for folder_name in args.folder:
        try:
            os.chdir(f"/scratch/{folder_name}")
            for i in os.listdir():
                with open(i, "rb") as f:
                    try:
                        s3.upload_fileobj(f, bucket_name, f"{folder_name}/{folder_name}/{i}")
                    except ClientError as e:
                        logging.error(e)
        except FileNotFoundError:
            pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output_bucket")
    parser.add_argument("-f", "--folder", action="append")
    args = parser.parse_args()

    main(args)
