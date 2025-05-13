import boto3
import json
import os
import logging
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Clients
s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
glue = boto3.client('glue')
sns = boto3.client('sns')

# Environment variables (set in Lambda config)
DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE', 'FileConfigTable')
SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN', 'arn:aws:sns:your-region:account-id:GlueJobFailure')

def lambda_handler(event, context):
    logger.info(f"Received event: {json.dumps(event)}")

    for record in event['Records']:
        try:
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']
            logger.info(f"Processing file: s3://{bucket}/{key}")

            # Fetch file metadata
            obj = s3.head_object(Bucket=bucket, Key=key)
            size = obj['ContentLength']
            file_ext = key.split('.')[-1].lower()

            logger.info(f"File size: {size} bytes | Type: {file_ext}")

            # Read config from DynamoDB
            config = get_config(file_ext)
            if not config:
                logger.warning(f"No config found for file type: {file_ext}")
                continue

            glue_job_name = select_glue_job(config, size)
            if not glue_job_name:
                logger.warning(f"No matching Glue job found for size: {size}")
                continue

            # Start Glue job
            logger.info(f"Starting Glue job: {glue_job_name}")
            response = glue.start_job_run(
                JobName=glue_job_name,
                Arguments={
                    '--S3_INPUT_PATH': f's3://{bucket}/{key}',
                    '--SOURCE_FILE_TYPE': file_ext
                }
            )
            logger.info(f"Glue job started with Run ID: {response['JobRunId']}")

        except Exception as e:
            logger.error(f"Error processing file {key}: {e}")
            send_alert(f"Glue job trigger failed for {key}: {e}")

    return {"status": "ok"}

def get_config(file_ext):
    """
    Fetch configuration for file extension from DynamoDB.
    Expected schema: {file_type: 'csv', glue_jobs: [{job: 'smallJob', max_size: 500000}, ...]}
    """
    table = dynamodb.Table(DYNAMO_TABLE)
    try:
        response = table.get_item(Key={'file_type': file_ext})
        return response.get('Item')
    except ClientError as e:
        logger.error(f"Error reading config from DynamoDB: {e}")
        return None

def select_glue_job(config, size):
    """
    Choose appropriate Glue job based on file size.
    """
    glue_jobs = sorted(config.get('glue_jobs', []), key=lambda x: x['max_size'])
    for job in glue_jobs:
        if size <= job['max_size']:
            return job['job']
    return None

def send_alert(message):
    """
    Send failure alert via SNS.
    """
    try:
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=message,
            Subject="Glue Job Failure Alert"
        )
    except Exception as e:
        logger.error(f"Failed to send SNS alert: {e}")





# import json

# def lambda_handler(event, context):
#     print("Received event:", json.dumps(event, indent=2))

#     for record in event['Records']:
#         bucket = record['s3']['bucket']['name']
#         key = record['s3']['object']['key']
#         print(f"File {key} uploaded to bucket {bucket}")

#     return {"status": "success"}
