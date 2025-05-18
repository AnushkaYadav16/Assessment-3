import boto3
import os
import io
import json
import zipfile
import argparse
import subprocess
import logging
from botocore.exceptions import ClientError
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def create_bucket(bucket_name, region='ap-south-1'):
    """Create an S3 bucket if it doesn't exist and apply a predefined policy."""
    s3 = boto3.client('s3', region_name=region, verify=False)

    try:
        s3.head_bucket(Bucket=bucket_name)
        logging.info(f"Bucket {bucket_name} already exists.")
    except ClientError:
        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={'LocationConstraint': region}
        )
        logging.info(f"Bucket {bucket_name} created.")

    bucket_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AllowGlueSuccessHandlerAthenaResultsWrite",
                "Effect": "Allow",
                "Principal": {
                    "AWS": "arn:aws:iam::640983357689:role/GlueSuccessHandlerRole"
                },
                "Action": [
                    "s3:PutObject",
                    "s3:ListBucket"
                ],
                "Resource": [
                    "arn:aws:s3:::outputathenabucket071611",
                    "arn:aws:s3:::outputathenabucket071611/athena-results/*"
                ]
            },
            {
                "Sid": "AllowGlueSuccessHandlerGetBucketLocation",
                "Effect": "Allow",
                "Principal": {
                    "AWS": "arn:aws:iam::640983357689:role/GlueSuccessHandlerRole"
                },
                "Action": "s3:GetBucketLocation",
                "Resource": "arn:aws:s3:::outputathenabucket071611"
            }
        ]
    }

    try:
        s3.put_bucket_policy(Bucket=bucket_name, Policy=json.dumps(bucket_policy))
        logging.info(f"Bucket policy applied on {bucket_name}.")
    except ClientError as e:
        logging.error(f"Failed to apply bucket policy: {e}")


def create_sqs_queue(queue_name='S3FileEventsQueue', region='ap-south-1', bucket_name=None):
    """Create an SQS queue and attach a policy to allow S3 notifications."""
    sqs = boto3.client('sqs', region_name=region, verify=False)
    response = sqs.create_queue(QueueName=queue_name)
    queue_url = response['QueueUrl']
    queue_arn = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=['QueueArn'])['Attributes']['QueueArn']

    if bucket_name:
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "AllowS3ToSendMessage",
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "SQS:SendMessage",
                    "Resource": queue_arn,
                    "Condition": {
                        "ArnEquals": {"aws:SourceArn": f"arn:aws:s3:::{bucket_name}"}
                    }
                }
            ]
        }
        sqs.set_queue_attributes(QueueUrl=queue_url, Attributes={'Policy': json.dumps(policy)})
        logging.info(f"SQS policy allowing S3 notifications added to {queue_name}")

    logging.info(f"SQS Queue created: {queue_name}, ARN: {queue_arn}")
    return queue_arn


def update_s3_notification_to_sqs(bucket_name, queue_arn):
    """Configure S3 to send object-created events to SQS."""
    s3 = boto3.client('s3', region_name='ap-south-1', verify=False)
    s3.put_bucket_notification_configuration(
        Bucket=bucket_name,
        NotificationConfiguration={
            'QueueConfigurations': [{
                'QueueArn': queue_arn,
                'Events': ['s3:ObjectCreated:*']
            }]
        }
    )
    logging.info(f"S3 bucket {bucket_name} now sends events to SQS.")


def add_sqs_trigger_to_lambda(function_name, sqs_arn):
    """Add SQS trigger to the specified Lambda function if not already present."""
    lambda_client = boto3.client('lambda', region_name='ap-south-1', verify=False)

    response = lambda_client.list_event_source_mappings(FunctionName=function_name, EventSourceArn=sqs_arn)
    if response['EventSourceMappings']:
        logging.info("SQS trigger already exists. Skipping creation.")
        return

    lambda_client.create_event_source_mapping(
        EventSourceArn=sqs_arn,
        FunctionName=function_name,
        BatchSize=1,
        Enabled=True
    )
    logging.info(f"Event source mapping created from {sqs_arn} to Lambda function {function_name}.")


def zip_lambda_in_memory(lambda_file):
    """Create a ZIP file in memory from the given Lambda source file."""
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.write(lambda_file, arcname=os.path.basename(lambda_file))
    zip_buffer.seek(0)
    return zip_buffer


def upload_lambda_zip_in_memory(zip_buffer, s3_key, bucket):
    """Upload in-memory ZIP buffer to S3."""
    s3 = boto3.client('s3', verify=False)
    s3.upload_fileobj(zip_buffer, bucket, s3_key)
    logging.info(f"Lambda zip uploaded to s3://{bucket}/{s3_key}")


def upload_glue_script(bucket):
    """Upload the Glue job script to S3."""
    s3 = boto3.client('s3', verify=False)
    s3.upload_file('glue_script.py', bucket, 'glue-scripts/glue_script.py')
    logging.info(f"Glue script uploaded to s3://{bucket}/glue-scripts/glue_script.py")


def create_glue_role(role_name):
    """Create an IAM role for Glue with necessary policies."""
    iam = boto3.client('iam', verify=False)
    try:
        iam.get_role(RoleName=role_name)
        logging.info(f"IAM role {role_name} already exists.")
    except ClientError:
        assume_role_policy = {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"Service": "glue.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }]
        }
        iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=json.dumps(assume_role_policy))
        iam.attach_role_policy(RoleName=role_name, PolicyArn='arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole')
        iam.attach_role_policy(RoleName=role_name, PolicyArn='arn:aws:iam::aws:policy/AmazonS3FullAccess')
        iam.attach_role_policy(RoleName=role_name, PolicyArn='arn:aws:iam::aws:policy/CloudWatchLogsFullAccess')
        logging.info(f"IAM role {role_name} created and policies attached.")


def create_glue_job(job_name, role_arn, script_location):
    """Create a Glue ETL job."""
    glue = boto3.client('glue', verify=False)
    try:
        glue.get_job(JobName=job_name)
        logging.info(f"Glue job {job_name} already exists.")
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
        logging.info(f"Glue job {job_name} created.")


def deploy_cloudformation(user_bucket):
    """Deploy CloudFormation stack."""
    logging.info("Deploying CloudFormation stack...")
    subprocess.run([
        "aws", "cloudformation", "deploy",
        "--template-file", "cloudFormation_template.yaml",
        "--stack-name", "s3-lambda-glue-stack",
        "--capabilities", "CAPABILITY_NAMED_IAM",
        "--parameter-overrides",
        f"LambdaZipS3Bucket=ziplambdacodebucket",
        f"DispatcherZipS3Key=file_type_dispatcher.zip",
        f"SuccessHandlerZipS3Key=glue_success_handler.zip",
        f"UserFileBucket={user_bucket}"
    ], check=True)
    logging.info("CloudFormation stack deployed.")


def upload_user_file(bucket, file_path):
    """Upload user file to S3."""
    s3 = boto3.client('s3', verify=False)
    filename = os.path.basename(file_path)
    s3.upload_file(file_path, bucket, filename)
    logging.info(f"User file {filename} uploaded to s3://{bucket}/")


def populate_dynamodb_table():
    """Insert predefined configuration into DynamoDB table."""
    dynamodb = boto3.resource('dynamodb', region_name='ap-south-1', verify=False)
    table = dynamodb.Table('GlueJobConfig')
    items = [
        {"fileType": "txt", "jobName": "MyTxtGlueJob"},
        {"fileType": "csv", "jobName": "MyCsvGlueJob"},
    ]
    for item in items:
        try:
            table.put_item(Item=item)
            logging.info(f"Inserted item into DynamoDB: {item}")
        except Exception as e:
            logging.error(f"Error inserting item {item}: {e}")


def create_failure_alert():
    """Create SNS topic and email subscription for Glue job failure alerts."""
    sns = boto3.client('sns', verify=False)
    topic_name = "glue-job-failure-alerts"
    topic_arn = sns.create_topic(Name=topic_name)['TopicArn']
    logging.info(f"SNS Topic created: {topic_arn}")

    response = sns.subscribe(
        TopicArn=topic_arn,
        Protocol='email',
        Endpoint='anushka.ydv1610@gmail.com'
    )
    logging.info("SNS email subscription requested. Please confirm via email.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--bucket-name', required=True)
    parser.add_argument('--file-path', required=True)
    args = parser.parse_args()

    region = 'ap-south-1'
    user_bucket = args.bucket_name
    zip_bucket = 'ziplambdacodebucket'
    output_bucket = 'outputathenabucket071611'
    glue_script_key = 'glue-scripts/glue_script.py'
    glue_script_location = f's3://{zip_bucket}/{glue_script_key}'

    create_bucket(zip_bucket, region)
    create_bucket(user_bucket, region)
    create_bucket(output_bucket, region)

    sqs_arn = create_sqs_queue(bucket_name=user_bucket)
    update_s3_notification_to_sqs(user_bucket, sqs_arn)

    zip_buf1 = zip_lambda_in_memory("lambda_function.py")
    upload_lambda_zip_in_memory(zip_buf1, "file_type_dispatcher.zip", zip_bucket)

    zip_buf2 = zip_lambda_in_memory("glue_success_handler.py")
    upload_lambda_zip_in_memory(zip_buf2, "glue_success_handler.zip", zip_bucket)


    upload_glue_script(zip_bucket)

    create_glue_role("MyGlueServiceRole")
    role_arn = "arn:aws:iam::640983357689:role/MyGlueServiceRole"

    create_glue_job("MyTxtGlueJob", role_arn, glue_script_location)
    create_glue_job("MyCsvGlueJob", role_arn, glue_script_location)

    deploy_cloudformation(user_bucket)
    add_sqs_trigger_to_lambda('FileTypeDispatcherFunction', sqs_arn)

    upload_user_file(user_bucket, args.file_path)
    populate_dynamodb_table()
    create_failure_alert()


if __name__ == "__main__":
    main()




