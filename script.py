import boto3
import os
import json
import zipfile
import argparse
import subprocess
import logging
from botocore.exceptions import ClientError
import urllib3
import io

# Suppress SSL certificate warnings (useful in local/test environments)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Set up logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

def create_bucket(bucket_name, region='ap-south-1'):
    """
    Create an S3 bucket if it doesn't already exist.

    Parameters:
    bucket_name (str): Name of the S3 bucket.
    region (str): AWS region where the bucket will be created. Default is 'ap-south-1'.
    """
    s3 = boto3.client('s3', region_name=region, verify=False)
    try:
        s3.head_bucket(Bucket=bucket_name)
        logger.info(f"Bucket {bucket_name} already exists.")
    except ClientError:
        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={'LocationConstraint': region}
        )
        logger.info(f"Bucket {bucket_name} created.")

def add_s3_lambda_trigger(bucket_name, lambda_function_arn):
    """
    Add an S3 event trigger to invoke the Lambda function when objects are created.

    Parameters:
    bucket_name (str): Name of the S3 bucket.
    lambda_function_arn (str): ARN of the Lambda function to be triggered.
    """
    s3 = boto3.client('s3')
    notification_config = {
        'LambdaFunctionConfigurations': [
            {
                'LambdaFunctionArn': lambda_function_arn,
                'Events': ['s3:ObjectCreated:*']
            }
        ]
    }
    s3.put_bucket_notification_configuration(
        Bucket=bucket_name,
        NotificationConfiguration=notification_config
    )
    logger.info(f"Added S3 -> Lambda trigger for bucket {bucket_name}")

def zip_lambda():
    """
    Create an in-memory ZIP archive of the Lambda function code.

    Returns:
    BytesIO: The in-memory ZIP file object.
    """
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipf.write("lambda_function.py", arcname="lambda_function.py")
    zip_buffer.seek(0)  # Rewind the buffer to the beginning
    logger.info("Lambda code zipped into memory.")
    return zip_buffer

def upload_lambda_zip(zip_buffer, bucket, zip_key='lambda_function.zip'):
    """
    Upload the in-memory Lambda ZIP archive to S3.

    Parameters:
    zip_buffer (BytesIO): The in-memory ZIP file.
    bucket (str): The S3 bucket to upload the ZIP to.
    zip_key (str): The key name to use in S3. Default is 'lambda_function.zip'.
    """
    s3 = boto3.client('s3', verify=False)
    s3.upload_fileobj(zip_buffer, bucket, zip_key)
    logger.info(f"Uploaded Lambda zip to s3://{bucket}/{zip_key}")

# def zip_lambda():
#     """
#     Create a ZIP archive of the Lambda function code.

#     Returns:
#     str: The name of the ZIP file created.
#     """
#     zip_filename = "lambda_function.zip"
#     with zipfile.ZipFile(zip_filename, 'w') as zipf:
#         zipf.write("lambda_function.py")
#     logger.info("Lambda code zipped into lambda_function.zip.")
#     return zip_filename

# def upload_lambda_zip(zip_file, bucket):
#     """
#     Upload the Lambda function ZIP to S3.

#     Parameters:
#     zip_file (str): The name of the ZIP file to upload.
#     bucket (str): The S3 bucket to upload the ZIP file to.
#     """
#     s3 = boto3.client('s3', verify=False)
#     s3.upload_file(zip_file, bucket, zip_file)
#     logger.info(f"Uploaded Lambda zip to s3://{bucket}/{zip_file}")

def upload_glue_script(bucket):
    """
    Upload the Glue ETL script to S3.

    Parameters:
    bucket (str): The S3 bucket to upload the Glue script to.
    """
    s3 = boto3.client('s3', verify=False)
    s3.upload_file('glue_script.py', bucket, 'glue-scripts/glue_script.py')
    logger.info(f"Uploaded Glue script to s3://{bucket}/glue-scripts/glue_script.py")

def create_glue_role(role_name):
    """
    Create an IAM role for AWS Glue if it doesn't exist.

    Parameters:
    role_name (str): Name of the IAM role to create.
    """
    iam = boto3.client('iam', verify=False)
    try:
        iam.get_role(RoleName=role_name)
        logger.info(f"IAM role {role_name} already exists.")
    except ClientError:
        logger.info(f"Creating IAM role: {role_name}")
        assume_role_policy = {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"Service": "glue.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }]
        }
        iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(assume_role_policy)
        )
        iam.attach_role_policy(RoleName=role_name, PolicyArn='arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole')
        iam.attach_role_policy(RoleName=role_name, PolicyArn='arn:aws:iam::aws:policy/AmazonS3FullAccess')
        iam.attach_role_policy(RoleName=role_name, PolicyArn='arn:aws:iam::aws:policy/CloudWatchLogsFullAccess')
        logger.info(f"IAM role {role_name} created and policies attached.")

def create_glue_job(job_name, role_arn, script_location):
    """
    Create an AWS Glue job if it doesn't already exist.

    Parameters:
    job_name (str): The name of the Glue job.
    role_arn (str): ARN of the IAM role to associate with the Glue job.
    script_location (str): S3 location of the Glue ETL script.
    """
    glue = boto3.client('glue', verify=False)
    try:
        glue.get_job(JobName=job_name)
        logger.info(f"Glue job {job_name} already exists.")
    except glue.exceptions.EntityNotFoundException:
        glue.create_job(
            Name=job_name,
            Role=role_arn,
            Command={
                'Name': 'glueetl',
                'ScriptLocation': script_location,
                'PythonVersion': '3'
            },
            GlueVersion='4.0',
            MaxCapacity=2.0
        )
        logger.info(f"Glue job {job_name} created.")

def deploy_cloudformation(user_bucket):
    """
    Deploy the CloudFormation stack.

    Parameters:
    user_bucket (str): The S3 bucket where the user files are stored.
    """
    logger.info("Deploying CloudFormation stack...")
    subprocess.run([
        "aws", "cloudformation", "deploy",
        "--template-file", "cloudFormation_template.yaml",
        "--stack-name", "s3-lambda-glue-stack",
        "--capabilities", "CAPABILITY_NAMED_IAM",
        "--parameter-overrides",
        f"LambdaZipS3Bucket=ziplambdacodebucket",
        f"LambdaZipS3Key=lambda_function.zip",
        f"UserFileBucket={user_bucket}"
    ], check=True)

def upload_user_file(bucket, file_path):
    """
    Upload a user file to the specified S3 bucket.

    Parameters:
    bucket (str): The target S3 bucket.
    file_path (str): The local file path of the file to upload.
    """
    s3 = boto3.client('s3', verify=False)
    filename = os.path.basename(file_path)
    s3.upload_file(file_path, bucket, filename)
    logger.info(f"Uploaded file {filename} to bucket {bucket}")

def populate_dynamodb_table():
    """
    Prepopulate the DynamoDB table with initial configuration data.
    """
    dynamodb = boto3.resource('dynamodb', region_name='ap-south-1', verify=False)
    table = dynamodb.Table('GlueJobConfig')
    items = [
        {"fileType": "txt", "jobName": "MyTxtGlueJob"},
        {"fileType": "csv", "jobName": "MyCsvGlueJob"},
    ]
    for item in items:
        try:
            table.put_item(Item=item)
            logger.info(f"Inserted item: {item}")
        except Exception as e:
            logger.error(f"Error inserting item {item}: {e}")

def main():
    """
    Main function to execute the entire workflow: create buckets, upload files,
    create IAM roles, deploy CloudFormation, configure Lambda triggers, and upload files.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--bucket-name', required=True)
    parser.add_argument('--file-path', required=True)
    args = parser.parse_args()

    region = 'ap-south-1'
    user_bucket = args.bucket_name
    zip_bucket = 'ziplambdacodebucket'
    glue_script_bucket = zip_bucket
    glue_script_key = 'glue-scripts/glue_script.py'
    glue_script_location = f's3://{glue_script_bucket}/{glue_script_key}'

    create_bucket(zip_bucket, region)
    create_bucket(user_bucket, region)

    zip_buffer = zip_lambda()
    upload_lambda_zip(zip_buffer, zip_bucket)
    upload_glue_script(glue_script_bucket)

    create_glue_role("MyGlueServiceRole")
    role_arn = f"arn:aws:iam::640983357689:role/MyGlueServiceRole"

    create_glue_job("MyTxtGlueJob", role_arn, glue_script_location)
    create_glue_job("MyCsvGlueJob", role_arn, glue_script_location)

    deploy_cloudformation(user_bucket)

    # Get the Lambda function ARN from AWS
    lambda_client = boto3.client('lambda', region_name=region, verify=False)
    try:
        response = lambda_client.get_function(FunctionName='FileTypeDispatcherFunction')  # Replace with your Lambda function name
        lambda_arn = response['Configuration']['FunctionArn']

        # Add permission for S3 to invoke Lambda
        lambda_client.add_permission(
            FunctionName='FileTypeDispatcherFunction',
            StatementId='AllowS3Invoke',
            Action='lambda:InvokeFunction',
            Principal='s3.amazonaws.com',
            SourceArn=f'arn:aws:s3:::{user_bucket}',
            SourceAccount='640983357689'  # Replace with your actual AWS account ID
        )

        # Add the actual S3 trigger
        add_s3_lambda_trigger(user_bucket, lambda_arn)

    except Exception as e:
        logger.error(f"Error configuring S3 trigger: {e}")

    upload_user_file(user_bucket, args.file_path)

    # NEW: Prepopulate the DynamoDB table
    populate_dynamodb_table()

if __name__ == "__main__":
    main()
