import boto3
import logging
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

glue = boto3.client('glue')
athena = boto3.client('athena')
sns = boto3.client('sns')


def start_crawler_if_not_running(crawler_name):
    """
    Start a Glue crawler if it is not already running.
    """
    try:
        response = glue.get_crawler(Name=crawler_name)
        state = response['Crawler']['State']

        if state == 'READY':
            glue.start_crawler(Name=crawler_name)
            logger.info(f"Started crawler: {crawler_name}")
        else:
            logger.info(f"Crawler '{crawler_name}' is already {state}, not starting.")
    except Exception as e:
        logger.error(f"Failed to start crawler '{crawler_name}': {e}", exc_info=True)


def get_latest_table_name(database_name, table_prefix):
    """
    Get the most recently created table name from Glue matching the prefix.
    """
    try:
        tables = glue.get_tables(DatabaseName=database_name)['TableList']
        filtered = [t for t in tables if t['Name'].startswith(table_prefix)]

        if not filtered:
            logger.warning(f"No tables found with prefix '{table_prefix}' in DB '{database_name}'")
            return None

        latest_table = sorted(filtered, key=lambda t: t['CreateTime'], reverse=True)[0]
        return latest_table['Name']

    except Exception as e:
        logger.error(f"Error retrieving tables: {e}", exc_info=True)
        return None


def lambda_handler(event, context):
    """
    Lambda function triggered by EventBridge for Glue job state change events.
    """
    logger.info(f"Received event: {event}")

    detail = event.get("detail", {})
    job_name = detail.get("jobName")
    state = detail.get("state")

    if not job_name:
        logger.warning("Missing job name in event details.")
        return

    if state == "SUCCEEDED":
        logger.info(f"Glue job '{job_name}' succeeded. Triggering crawler and updating Athena view.")

        if "csv" in job_name.lower():
            crawler_name = "MyCsvGlueCrawler"
            table_prefix = "csv"
        elif "txt" in job_name.lower():
            crawler_name = "MyTxtGlueCrawler"
            table_prefix = "txt"
        else:
            logger.warning(f"No crawler configured for job: {job_name}")
            return

        start_crawler_if_not_running(crawler_name)

        table_name = get_latest_table_name("processed_data_db", table_prefix)
        if not table_name:
            return

        view_name = f"processed_{table_prefix.strip('_')}"
        query = f"""
        CREATE OR REPLACE VIEW {view_name} AS
        SELECT * FROM processed_data_db.{table_name};
        """
        try:
            response = athena.start_query_execution(
                QueryString=query,
                QueryExecutionContext={'Database': 'processed_data_db'},
                ResultConfiguration={
                    'OutputLocation': 's3://outputathenabucket071611/athena-results/'
                }
            )
            logger.info(f"Athena view '{view_name}' creation started for table '{table_name}'. QueryExecutionId: {response['QueryExecutionId']}")
        except Exception as e:
            logger.error(f"Failed to execute Athena query: {e}", exc_info=True)

    elif state == "FAILED":
        logger.error(f"Glue job '{job_name}' failed.")
        sns_topic_arn = os.environ.get('SNS_TOPIC_ARN')
        if not sns_topic_arn:
            logger.warning("SNS_TOPIC_ARN environment variable is not set.")
            return

        message = f"Glue job failed.\nJob Name: {job_name}\nState: FAILED"
        try:
            sns.publish(
                TopicArn=sns_topic_arn,
                Subject="ALERT: Glue Job Failure",
                Message=message
            )
            logger.info("SNS alert published successfully.")
        except Exception as e:
            logger.error(f"Failed to send SNS alert: {e}", exc_info=True)
    else:
        logger.info(f"Glue job '{job_name}' is in state: {state}")