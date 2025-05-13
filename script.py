import boto3
import botocore
from botocore.exceptions import ClientError
import logging
import os
import io
import zipfile
import argparse

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# AWS clients
s3 = boto3.client('s3')
cloudformation = boto3.client('cloudformation')

# Hardcoded settings
LAMBDA_BUCKET = "lambdacodezip"  # Must be globally unique
LAMBDA_FILE = "triggerlambda.py"
LAMBDA_ZIP_KEY = "lambda-code.zip"
REGION = "ap-south-1"

def create_bucket(bucket_name, region):
    """Create an S3 bucket if it doesn't exist."""
    try:
        s3.head_bucket(Bucket=bucket_name)
        logger.info(f"Bucket '{bucket_name}' already exists.")
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            logger.info(f"Bucket '{bucket_name}' does not exist. Creating...")
            s3.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={'LocationConstraint': region}
            )
        else:
            logger.error(f"Error checking/creating bucket: {e}")
            raise

def create_zip_in_memory(file_path):
    """Create zip file in memory from a given file."""
    memory_file = io.BytesIO()
    with zipfile.ZipFile(memory_file, 'w', zipfile.ZIP_DEFLATED) as zf:
        filename = os.path.basename(file_path)
        zf.write(file_path, arcname=filename)
    memory_file.seek(0)
    return memory_file

def upload_lambda_code(bucket_name, zip_key, lambda_file_path):
    """Zip the lambda function and upload to Lambda code bucket."""
    zip_stream = create_zip_in_memory(lambda_file_path)
    s3.upload_fileobj(zip_stream, bucket_name, zip_key)
    logger.info(f"Lambda code uploaded as '{zip_key}' to bucket '{bucket_name}'")

def upload_file_to_s3(bucket_name, file_path):
    """Upload any file to the specified S3 bucket."""
    file_name = os.path.basename(file_path)
    s3.upload_file(file_path, bucket_name, file_name)
    logger.info(f"Uploaded file '{file_name}' to bucket '{bucket_name}'")

def create_cloudformation_stack(s3_bucket_name, lambda_code_bucket, lambda_code_key):
    """Create a CloudFormation stack to deploy the Lambda trigger."""
    with open("lambda_s3_trigger.yaml", "r") as f:
        template_body = f.read()

    parameters = [
        {'ParameterKey': 'S3BucketName', 'ParameterValue': s3_bucket_name},
        {'ParameterKey': 'LambdaCodeS3Bucket', 'ParameterValue': lambda_code_bucket},
        {'ParameterKey': 'LambdaCodeS3Key', 'ParameterValue': lambda_code_key}
    ]

    try:
        response = cloudformation.create_stack(
            StackName='LambdaS3Stack',
            TemplateBody=template_body,
            Parameters=parameters,
            Capabilities=['CAPABILITY_NAMED_IAM']
        )
        logger.info(f"CloudFormation stack creation started: {response['StackId']}")
    except ClientError as e:
        logger.error(f"Failed to create CloudFormation stack: {e}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Deploy Lambda S3 trigger setup.")
    parser.add_argument('--bucket', required=True, help='Main S3 bucket for object uploads')
    parser.add_argument('--file', required=True, help='File to upload to main bucket')
    args = parser.parse_args()

    user_bucket = args.bucket
    upload_file_path = args.file

    # Step 1: Create the user-provided bucket
    create_bucket(user_bucket, REGION)

    # Step 2: Create and upload Lambda function zip to the hardcoded bucket
    create_bucket(LAMBDA_BUCKET, REGION)
    upload_lambda_code(LAMBDA_BUCKET, LAMBDA_ZIP_KEY, LAMBDA_FILE)

    # Step 3: Deploy CloudFormation stack
    create_cloudformation_stack(user_bucket, LAMBDA_BUCKET, LAMBDA_ZIP_KEY)

    # Step 4: Upload a file to the main bucket (to trigger Lambda)
    upload_file_to_s3(user_bucket, upload_file_path)




# import boto3
# import botocore
# from botocore.exceptions import ClientError
# import logging
# import os
# import io
# import zipfile
# import argparse
# import random

# # Logging setup
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# logger = logging.getLogger(__name__)

# # AWS clients
# s3 = boto3.client('s3')
# cloudformation = boto3.client('cloudformation')

# # Hardcoded values
# lambda_code_bucket = "my-lambda-code-bucket"  # Replace with your Lambda code bucket name
# lambda_file_path = "path/to/your/triggerlambda.py"  # Hardcoded path to your Lambda function script
# region = "ap-south-1"  # Replace with the desired AWS region for the bucket

# def create_bucket(bucket_name, region):
#     """
#     Creates an S3 bucket if it does not exist.
#     Returns True if the bucket was created, False if it already existed.
#     """
#     try:
#         s3.head_bucket(Bucket=bucket_name)
#         logging.info(f"Bucket '{bucket_name}' already exists.")
#         return False
#     except ClientError as e:
#         if e.response['Error']['Code'] == '404':
#             logging.info(f"Bucket '{bucket_name}' does not exist. Creating now...")
#             s3.create_bucket(
#                 Bucket=bucket_name,
#                 CreateBucketConfiguration={'LocationConstraint': region}
#             )
#             return True
#         else:
#             logging.error(f"Error checking or creating bucket: {e}")
#             raise e

# def create_zip_in_memory(lambda_file_path):
#     """
#     Creates a ZIP file in memory containing the Lambda function code (e.g., triggerlambda.py).
#     Returns the in-memory zip file as a byte stream.
#     """
#     memory_file = io.BytesIO()

#     with zipfile.ZipFile(memory_file, 'w', zipfile.ZIP_DEFLATED) as zipf:
#         file_name = os.path.basename(lambda_file_path)
#         zipf.write(lambda_file_path, arcname=file_name)

#     memory_file.seek(0)
#     return memory_file

# def upload_lambda_zip(s3_client, bucket_name, lambda_code_key, lambda_file_path):
#     """
#     Uploads the Lambda zip (created in memory) to the specified S3 bucket.
#     """
#     try:
#         memory_zip = create_zip_in_memory(lambda_file_path)
#         s3_client.upload_fileobj(memory_zip, bucket_name, lambda_code_key)
#         logger.info(f"Lambda function code uploaded to bucket '{bucket_name}' with key '{lambda_code_key}'.")
#     except botocore.exceptions.ClientError as e:
#         logger.error(f"Failed to upload Lambda function zip to bucket '{bucket_name}': {e}")
#         raise

# def create_cloudformation_stack(bucket_name, lambda_code_key):
#     """
#     Creates a CloudFormation stack that links the Lambda function to S3.
#     """
#     with open('lambda_s3_trigger.yaml', 'r') as file:
#         template_body = file.read()

#     parameters = [
#         {'ParameterKey': 'S3BucketName', 'ParameterValue': bucket_name},
#         {'ParameterKey': 'LambdaCodeS3Bucket', 'ParameterValue': lambda_code_bucket},  # Use the hardcoded value
#         {'ParameterKey': 'LambdaCodeS3Key', 'ParameterValue': lambda_code_key}
#     ]

#     try:
#         response = cloudformation.create_stack(
#             StackName=f'lambda-s3-stack-{random.randint(1000, 9999)}',  # Random suffix to make stack name unique
#             TemplateBody=template_body,
#             Parameters=parameters,
#             Capabilities=['CAPABILITY_NAMED_IAM']  # If the stack creates IAM resources
#         )
#         logger.info(f"CloudFormation stack creation initiated: {response['StackId']}")
#     except Exception as e:
#         logger.error(f"Error creating CloudFormation stack: {e}")
#         raise

# def upload_file_to_first_bucket(s3_client, bucket_name, file_path):
#     """
#     Uploads the specified file to the first S3 bucket (provided via CLI).
#     """
#     try:
#         file_name = os.path.basename(file_path)
#         s3_client.upload_file(file_path, bucket_name, file_name)
#         logger.info(f"File '{file_name}' uploaded to bucket '{bucket_name}'.")
#     except botocore.exceptions.ClientError as e:
#         logger.error(f"Failed to upload file '{file_path}' to bucket '{bucket_name}': {e}")
#         raise

# if __name__ == "__main__":
#     parser = argparse.ArgumentParser(description="Create S3 buckets, upload Lambda zip, and deploy CloudFormation stack.")
#     parser.add_argument('--bucket', required=True, help='S3 bucket name (trigger bucket for Lambda)')
#     parser.add_argument('--lambda-code-key', required=True, help='S3 key for Lambda function code')
#     parser.add_argument('--file', required=True, help='Path to the file to upload to the first S3 bucket')

#     args = parser.parse_args()

#     bucket_name = args.bucket
#     lambda_code_key = args.lambda_code_key
#     file_path = args.file

#     # Step 1: Create the first S3 bucket (where the Lambda will be triggered on file upload)
#     create_bucket(bucket_name, region)

#     # Step 2: Create the second S3 bucket (for Lambda code) and upload the Lambda zip
#     create_bucket(lambda_code_bucket, region)  # Create the Lambda code bucket if it doesn't exist
#     upload_lambda_zip(s3, lambda_code_bucket, lambda_code_key, lambda_file_path)

#     # Step 3: Create CloudFormation stack to set up Lambda and trigger it on the first S3 bucket
#     create_cloudformation_stack(bucket_name, lambda_code_key)

#     # Step 4: Upload the file to the first S3 bucket (this will trigger the Lambda function)
#     upload_file_to_first_bucket(s3, bucket_name, file_path)
