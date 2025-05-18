import json
import boto3
import logging
from boto3.dynamodb.conditions import Attr

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# AWS clients
dynamodb = boto3.resource('dynamodb')
glue_client = boto3.client('glue')


def is_glue_job_running(job_name):
    """
    Check if a given AWS Glue job is currently running, starting, or stopping.

    Args:
        job_name (str): The name of the Glue job.

    Returns:
        bool: True if job is running or starting; False otherwise.
    """
    try:
        runs = glue_client.get_job_runs(JobName=job_name, MaxResults=3)['JobRuns']
        for run in runs:
            state = run.get('JobRunState')
            if state in ['RUNNING', 'STARTING', 'STOPPING']:
                logger.info(f"Glue job '{job_name}' is currently in state: {state}")
                return True
        return False
    except Exception as e:
        logger.error(f"Error checking status of Glue job '{job_name}': {e}")
        # Fail-safe to avoid concurrent runs
        return True


def lambda_handler(event, context):
    """
    Lambda handler triggered by SQS messages from S3 events. 
    It scans DynamoDB for Glue job configuration and starts the job if not already running.

    Args:
        event (dict): The event payload from SQS.
        context (object): The Lambda context runtime methods and attributes.
    """
    logger.info(f"Event received from EventBridge/SQS: {json.dumps(event, indent=2)}")
    table = dynamodb.Table('GlueJobConfig')

    for record in event.get('Records', []):
        try:
            body = json.loads(record['body'])
            logger.info(f"Parsed SQS message body: {json.dumps(body, indent=2)}")

            for s3_record in body.get('Records', []):
                bucket = s3_record['s3']['bucket']['name']
                s3_key = s3_record['s3']['object']['key']
                file_type = s3_key.split('.')[-1]
                request_id = s3_record.get('responseElements', {}).get('x-amz-request-id', 'unknown')

                logger.info(f"Processing file: s3://{bucket}/{s3_key} | RequestId: {request_id} | File Type: {file_type}")

                # Query DynamoDB for matching Glue job
                response = table.scan(
                    FilterExpression=Attr('fileType').eq(file_type)
                )
                items = response.get('Items', [])

                if not items:
                    logger.warning(f"No Glue job configured for file type: {file_type}")
                    continue

                job_name = items[0]['jobName']
                logger.info(f"Glue job '{job_name}' matched for file type: {file_type}")

                if is_glue_job_running(job_name):
                    logger.warning(f"Glue job '{job_name}' is already running. Skipping new execution.")
                    continue

                # Start Glue job
                glue_response = glue_client.start_job_run(
                    JobName=job_name,
                    Arguments={
                        '--bucket_name': bucket,
                        '--key': s3_key
                    }
                )
                logger.info(f"Glue job '{job_name}' started with Run ID: {glue_response['JobRunId']}")

        except Exception as e:
            logger.error(f"Failed to process SQS record: {e}", exc_info=True)
            raise e  # Rethrow to trigger DLQ if configured












# import json
# import boto3
# import logging
# from boto3.dynamodb.conditions import Attr

# # Logging setup
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger()

# # Clients
# dynamodb = boto3.resource('dynamodb')
# glue_client = boto3.client('glue')

# def is_glue_job_running(job_name):
#     """
#     Returns True if the job is currently running or starting.
#     """
#     try:
#         runs = glue_client.get_job_runs(JobName=job_name, MaxResults=3)['JobRuns']
#         for run in runs:
#             if run['JobRunState'] in ['RUNNING', 'STARTING', 'STOPPING']:
#                 logger.info(f"Glue job '{job_name}' is currently in state: {run['JobRunState']}")
#                 return True
#         return False
#     except Exception as e:
#         logger.error(f"Error checking status of Glue job '{job_name}': {str(e)}")
#         return True  # Fail-safe: assume running to avoid collision

# def lambda_handler(event, context):
#     logger.info(f"Event received from EventBridge: {json.dumps(event, indent=2)}")
    
#     table = dynamodb.Table('GlueJobConfig')
    
#     for record in event['Records']:
#         try:
#             body = json.loads(record['body'])
#             logger.info(f"SQS message body: {json.dumps(body, indent=2)}")
            
#             if 'Records' in body:
#                 for s3_record in body['Records']:
#                     bucket = s3_record['s3']['bucket']['name']
#                     s3_key = s3_record['s3']['object']['key']
#                     file_type = s3_key.split('.')[-1]
#                     request_id = s3_record['responseElements']['x-amz-request-id']
                    
#                     logger.info(f"Bucket: {bucket}, RequestId: {request_id}, Key: {s3_key}, File Type: {file_type}")
                    
#                     response = table.scan(
#                         FilterExpression=Attr('fileType').eq(file_type)
#                     )
                    
#                     items = response.get('Items', [])
#                     if items:
#                         job_name = items[0]['jobName']
#                         logger.info(f"Found Glue job: {job_name}")

#                         if is_glue_job_running(job_name):
#                             logger.warning(f"Glue job '{job_name}' is already running. Skipping new execution.")
#                             continue
                        
#                         glue_response = glue_client.start_job_run(
#                             JobName=job_name,
#                             Arguments={
#                                 '--bucket_name': bucket,
#                                 '--key': s3_key
#                             }
#                         )
#                         logger.info(f"Started Glue job '{job_name}' with Run ID: {glue_response['JobRunId']}")
#                     else:
#                         logger.warning(f"No Glue job found for file type: {file_type}")
                        
#         except Exception as e:
#             logger.error(f"Failed to process SQS record: {str(e)}")
#             raise e  # Optional: triggers DLQ
