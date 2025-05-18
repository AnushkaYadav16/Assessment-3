import sys
import logging
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.context import SparkContext

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'bucket_name', 'key'])

bucket = args['bucket_name']
key = args['key']
input_path = f"s3://{bucket}/{key}"

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args['JOB_NAME'], args)

logger.info(f"Starting Glue ETL job for file: {input_path}")

try:
    if key.endswith(".csv"):
        df = spark.read.option("header", "true").csv(input_path)
        output_path = "s3://outputathenabucket071611/athena-results/csv"
    elif key.endswith(".txt"):
        df = spark.read.text(input_path)
        output_path = "s3://outputathenabucket071611/athena-results/txt"
    else:
        logger.error("Unsupported file type. Only .csv and .txt are supported.")
        job.commit()
        sys.exit(1)

    logger.info("Sample rows from the input data:")
    df.show(5)

    df.write.mode("overwrite").option("header", "true").csv(output_path)
    logger.info(f"Finished writing output to: {output_path}")

except Exception as e:
    logger.error(f"An error occurred during the ETL job: {e}", exc_info=True)
    raise

finally:
    job.commit()
    logger.info("Glue job committed successfully.")












