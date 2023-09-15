import json
import boto3
from botocore.exceptions import ClientError
from typing import Dict


def get_secret_value(secret_name: str, region_name: str = "eu-west-1") -> Dict:
    client = boto3.session.Session().client(service_name='secretsmanager',  region_name=region_name)
    try:
        secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise e
    return json.loads(secret_value_response.get("SecretString"))

if __name__ == '__main__':
    s3 = boto3.client('s3')
    response = s3.list_buckets()
    print(response)