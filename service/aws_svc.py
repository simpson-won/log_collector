from log import logger
from config import AWS_ACCESS_KEY, AWS_SECERET_KEY
import boto3
from botocore.exceptions import ClientError


def s3_object_get(bucket: str, key: str):
    try:
        client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECERET_KEY)
        return client.get_object(Bucket=bucket, Key=key)
    except ClientError as e:
        logger.error(f"exception: delete_s3_object\n\t\t{e}")


def s3_object_delete(bucket: str, key: str):
    try:
        client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECERET_KEY)
        client.delete_object(Bucket=bucket, Key=key)
    except ClientError as e:
        logger.error(f"exception: delete_s3_object\n\t\t{e}")


def s3_bucket_list():
    try:
        client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECERET_KEY)
        response = client.list_buckets()
        return response["Buckets"]
    except ClientError as e:
        logger.error(f"exception: delete_s3_object\n\t\t{e}")


if __name__=="__main__":
    #s3_bucket_list()
    bucket = "aimmo-atlas-log"
    key = "logs/aimmo-core-azure/atlas-mcarxx-shard-00-01.1jkei.mongodb.net/mongod/2024-10-23/1729661983-9ac8775c-f29c-4b5c-b78e-315204dd682f.log"
    s3_object = s3_object_get(bucket=bucket, key=key)
    #s3_object_delete(bucket=bucket, key=key)
    body_bytes_list = s3_object["Body"].read().decode('utf-8').split("\n")
    for body_bytes in body_bytes_list:
        import json
        body = json.loads(body_bytes)
        print(f's3_object = {type(body)}')
        print(f's3_object = {body}')
