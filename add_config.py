import boto3
import logging
from botocore.exceptions import ClientError

REGION = 'ap-south-1'
TABLE_NAME = 'FileConfigTable'

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

dynamodb = boto3.resource('dynamodb', region_name=REGION)
dynamodb_client = boto3.client('dynamodb', region_name=REGION)


def create_table_if_not_exists():
    """
    Checks if the DynamoDB table exists. If not, it creates the table and waits until it becomes active.
    """
    try:
        table = dynamodb.Table(TABLE_NAME)
        table.load()  # Attempt to load table metadata
        logger.info(f" Table '{TABLE_NAME}' already exists.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            logger.info(f"Table '{TABLE_NAME}' does not exist. Creating...")
            try:
                dynamodb_client.create_table(
                    TableName=TABLE_NAME,
                    KeySchema=[{'AttributeName': 'file_type', 'KeyType': 'HASH'}],
                    AttributeDefinitions=[{'AttributeName': 'file_type', 'AttributeType': 'S'}],
                    BillingMode='PAY_PER_REQUEST'
                )
                logger.info("Waiting for table to become active...")
                waiter = dynamodb_client.get_waiter('table_exists')
                waiter.wait(TableName=TABLE_NAME)
                logger.info(f"Table '{TABLE_NAME}' is now active and ready.")
            except ClientError as ce:
                logger.error(f"Failed to create table: {ce}")
                raise
        else:
            logger.error(f"Unexpected error when checking/creating table: {e}")
            raise

def add_config_item():
    """
    Adds a sample configuration item for file type 'csv' with two glue job mappings.
    """
    try:
        table = dynamodb.Table(TABLE_NAME)
        response = table.put_item(
            Item={
                "file_type": "csv",
                "glue_jobs": [
                    {"job": "CSVJobSmall", "max_size": 500000},
                    {"job": "CSVJobLarge", "max_size": 5000000}
                ]
            }
        )
        logger.info("Configuration item for 'csv' added to the table.")
    except ClientError as e:
        logger.error(f"Failed to add config item: {e}")
        raise

if __name__ == "__main__":
    logger.info("Starting DynamoDB setup and config insertion...")
    create_table_if_not_exists()
    add_config_item()
    logger.info("Setup complete.")
