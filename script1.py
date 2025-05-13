import boto3
import botocore
from botocore.exceptions import ClientError
import argparse
import logging
import os
import io
import zipfile
import time
import json
import urllib3

# Suppress SSL certificate warnings (useful in local/test environments)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# AWS clients (explicit region and SSL verification disabled)
REGION = 'ap-south-1'
s3 = boto3.client('s3', region_name=REGION, verify=False)
lambda_client = boto3.client('lambda', region_name=REGION, verify=False)
iam = boto3.client('iam', region_name=REGION, verify=False)

# Constants
LAMBDA_ROLE_NAME = "LambdaS3ExecutionRole"
LAMBDA_POLICY_NAME = "LambdaS3ExecutionPolicy"

# Trust policy for Lambda to assume IAM role
LAMBDA_TRUST_POLICY = {
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Principal": {"Service": "lambda.amazonaws.com"},
        "Action": "sts:AssumeRole"
    }]
}

# Inline policy for S3 and CloudWatch access
LAMBDA_EXECUTION_POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": ["logs:*"],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": ["s3:GetObject", "s3:PutObject"],
            "Resource": "*"
        }
    ]
}

def create_bucket(bucket_name):
    """
    Checks if an S3 bucket exists. If not, creates it.
    """
    logger.info(f"Checking if bucket '{bucket_name}' exists...")
    try:
        s3.head_bucket(Bucket=bucket_name)
        logger.info(f"Bucket '{bucket_name}' already exists.")
        return False
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            logger.info(f"Creating bucket '{bucket_name}'...")
            s3.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={'LocationConstraint': REGION}
            )
            return True
        else:
            logger.error(f"Error checking/creating bucket: {e}")
            raise

def upload_file(s3_client, bucket_name, file_path):
    """
    Uploads a local file to the specified S3 bucket.
    """
    file_name = os.path.basename(file_path)
    try:
        logger.info(f"Uploading file '{file_name}' to bucket '{bucket_name}'...")
        s3_client.upload_file(file_path, bucket_name, file_name)
        logger.info(f"File '{file_name}' uploaded successfully.")
    except ClientError as e:
        logger.error(f"Failed to upload file: {e}")
        raise

def get_or_create_lambda_role():
    """
    Retrieves or creates an IAM role for Lambda execution.
    """
    try:
        logger.info(f"Checking for existing IAM role '{LAMBDA_ROLE_NAME}'...")
        role = iam.get_role(RoleName=LAMBDA_ROLE_NAME)
        logger.info(f"IAM role '{LAMBDA_ROLE_NAME}' already exists.")
        return role['Role']['Arn']
    except iam.exceptions.NoSuchEntityException:
        logger.info(f"Creating IAM role '{LAMBDA_ROLE_NAME}'...")
        role = iam.create_role(
            RoleName=LAMBDA_ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(LAMBDA_TRUST_POLICY),
            Description='Lambda role with S3 and CloudWatch permissions'
        )
        logger.info("Attaching inline policy...")
        iam.put_role_policy(
            RoleName=LAMBDA_ROLE_NAME,
            PolicyName=LAMBDA_POLICY_NAME,
            PolicyDocument=json.dumps(LAMBDA_EXECUTION_POLICY)
        )
        logger.info("Waiting for IAM role to propagate...")
        time.sleep(10)
        return role['Role']['Arn']

def create_lambda_zip_in_memory(source_file):
    """
    Creates a ZIP of the Lambda code file in memory.
    """
    logger.info(f"Reading Lambda function code from '{source_file}'...")
    with open(source_file, 'r') as f:
        code = f.read()

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'a', zipfile.ZIP_DEFLATED) as zip_file:
        zip_file.writestr('lambda_function.py', code)
    zip_buffer.seek(0)
    return zip_buffer.read()

def create_lambda_function(lambda_name, lambda_file_path, role_arn):
    """
    Creates a new Lambda function or fetches existing one.
    """
    try:
        logger.info(f"Checking if Lambda function '{lambda_name}' exists...")
        response = lambda_client.get_function(FunctionName=lambda_name)
        logger.info(f"Lambda function '{lambda_name}' already exists.")
        return response['Configuration']['FunctionArn']
    except lambda_client.exceptions.ResourceNotFoundException:
        logger.info(f"Creating Lambda function '{lambda_name}'...")
        zip_data = create_lambda_zip_in_memory(lambda_file_path)
        response = lambda_client.create_function(
            FunctionName=lambda_name,
            Runtime='python3.8',
            Role=role_arn,
            Handler='lambda_function.lambda_handler',
            Code={'ZipFile': zip_data},
            Timeout=30,
            MemorySize=128
        )
        logger.info(f"Lambda function '{lambda_name}' created successfully.")
        return response['FunctionArn']

def add_lambda_permission_to_s3(bucket_name, lambda_function_name):
    """
    Grants S3 permission to invoke the Lambda function.
    """
    logger.info("Adding S3 invoke permission to Lambda (if needed)...")
    try:
        lambda_client.add_permission(
            FunctionName=lambda_function_name,
            Principal='s3.amazonaws.com',
            StatementId='S3InvokePermission',
            Action='lambda:InvokeFunction',
            SourceArn=f'arn:aws:s3:::{bucket_name}'
        )
        logger.info("S3 permission added to Lambda function.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceConflictException':
            logger.info("S3 permission already exists.")
        else:
            logger.error(f"Error adding permission: {e}")
            raise
    logger.info("Waiting for Lambda permission to propagate...")
    time.sleep(5)

def add_s3_event_notification(bucket_name, lambda_function_arn):
    """
    Configures the S3 bucket to trigger Lambda on .txt file upload.
    """
    logger.info(f"Configuring S3 bucket '{bucket_name}' to trigger Lambda...")
    logger.info(f"Lambda ARN used: {lambda_function_arn}")
    try:
        notification_configuration = {
            'LambdaFunctionConfigurations': [
                {
                    'LambdaFunctionArn': lambda_function_arn,
                    'Events': ['s3:ObjectCreated:*'],
                    'Filter': {
                        'Key': {
                            'FilterRules': [{'Name': 'suffix', 'Value': '.txt'}]
                        }
                    }
                }
            ]
        }
        s3.put_bucket_notification_configuration(
            Bucket=bucket_name,
            NotificationConfiguration=notification_configuration
        )
        logger.info("S3 event trigger configuration completed.")
    except ClientError as e:
        logger.error(f"Failed to configure S3 event: {e}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create an S3 bucket, Lambda function, and connect them.")
    parser.add_argument('--bucket', required=True, help='S3 bucket name')
    parser.add_argument('--file', required=True, help='Path to the file to upload')
    parser.add_argument('--lambda-name', required=True, help='Name for the Lambda function')

    args = parser.parse_args()

    bucket_name = args.bucket
    file_path = args.file
    lambda_name = args.lambda_name
    lambda_code_path = os.path.join(os.path.dirname(__file__), 'triggerlambda.py')

    logger.info("Starting process...")

    create_bucket(bucket_name)
    lambda_role_arn = get_or_create_lambda_role()
    lambda_arn = create_lambda_function(lambda_name, lambda_code_path, lambda_role_arn)
    add_lambda_permission_to_s3(bucket_name, lambda_name)
    add_s3_event_notification(bucket_name, lambda_arn)
    upload_file(s3, bucket_name, file_path)

    logger.info("Process completed successfully.")
