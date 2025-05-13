import boto3
import botocore
from botocore.exceptions import ClientError
import random
import argparse
import logging
import os
import io
import zipfile
import subprocess
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

s3 = boto3.client('s3')
cloudformation = boto3.client('cloudformation')
bucket_name = None

def create_bucket(bucket_name, region):
    """
    Creates an S3 bucket if it does not exist.
    Returns True if the bucket was created, False if it already existed.
    """
    try:
        s3.head_bucket(Bucket=bucket_name)
        logging.info(f"Bucket '{bucket_name}' already exists.")
        return False
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            logging.info(f"Bucket '{bucket_name}' does not exist. Creating now...")
            s3.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={'LocationConstraint': region}
            )
            return True
        else:
            logging.error(f"Error checking or creating bucket: {e}")
            raise e

def create_zip_in_memory(file_path):
    """
    Creates a ZIP file in memory containing the given file.
    Returns the in-memory zip file as a byte stream.
    """
    memory_file = io.BytesIO()

    with zipfile.ZipFile(memory_file, 'w', zipfile.ZIP_DEFLATED) as zipf:
        file_name = os.path.basename(file_path)
        zipf.write(file_path, arcname=file_name)

    memory_file.seek(0)

    return memory_file

def upload_file(s3_client, bucket_name, file_path):
    file_name = os.path.basename(file_path)

    try:
        memory_zip = create_zip_in_memory(file_path)
        s3_client.upload_fileobj(memory_zip, bucket_name, f"{file_name}.zip")
        logger.info(f"File '{file_name}' uploaded as a zip to bucket '{bucket_name}'.")
    except botocore.exceptions.ClientError as e:
        logger.error(f"Failed to upload file '{file_name}' to bucket '{bucket_name}': {e}")
        raise

def create_cloudformation_stack(bucket_name, lambda_code_bucket, lambda_code_key):
    """
    Creates a CloudFormation stack that links the Lambda function to S3.
    """
    # Read the CloudFormation YAML template
    with open('lambda_s3_trigger.yaml', 'r') as file:
        template_body = file.read()

    # Define the parameters for the CloudFormation stack
    parameters = [
        {'ParameterKey': 'S3BucketName', 'ParameterValue': bucket_name},
        {'ParameterKey': 'LambdaCodeS3Bucket', 'ParameterValue': lambda_code_bucket},
        {'ParameterKey': 'LambdaCodeS3Key', 'ParameterValue': lambda_code_key}
    ]

    try:
        # Create the CloudFormation stack
        response = cloudformation.create_stack(
            StackName='lambda-s3-stack',
            TemplateBody=template_body,
            Parameters=parameters,
            Capabilities=['CAPABILITY_NAMED_IAM']  # If the stack creates IAM resources
        )
        logger.info(f"CloudFormation stack creation initiated: {response['StackId']}")
    except Exception as e:
        logger.error(f"Error creating CloudFormation stack: {e}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create an S3 bucket, upload a file as a zip, and deploy CloudFormation stack.")
    parser.add_argument('--bucket', required=True, help='S3 bucket name')
    parser.add_argument('--file', required=True, help='Path to the file to upload')
    parser.add_argument('--lambda-code-bucket', required=True, help='S3 bucket containing Lambda code')
    parser.add_argument('--lambda-code-key', required=True, help='S3 key for Lambda function code')

    args = parser.parse_args()
    bucket_name = args.bucket
    file_path = args.file
    lambda_code_bucket = args.lambda_code_bucket
    lambda_code_key = args.lambda_code_key

    # Step 1: Create S3 bucket and upload the file
    create_bucket(bucket_name, 'ap-south-1')
    upload_file(s3, bucket_name, file_path)

    # Step 2: Create CloudFormation stack
    create_cloudformation_stack(bucket_name, lambda_code_bucket, lambda_code_key)








