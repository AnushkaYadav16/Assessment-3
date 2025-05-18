import sys
import logging
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.context import SparkContext

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Parse input arguments passed from Glue/Lambda
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'bucket_name', 'key'])

bucket = args['bucket_name']
key = args['key']
input_path = f"s3://{bucket}/{key}"

# Initialize Spark and Glue contexts
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

# Initialize Glue job
job = Job(glue_context)
job.init(args['JOB_NAME'], args)

logger.info(f"Starting Glue ETL job for file: {input_path}")

try:
    # Determine file type and read accordingly
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

    # Write output for Glue crawler to pick up
    df.write.mode("overwrite").option("header", "true").csv(output_path)
    logger.info(f"Finished writing output to: {output_path}")

except Exception as e:
    logger.error(f"An error occurred during the ETL job: {e}", exc_info=True)
    raise

finally:
    job.commit()
    logger.info("Glue job committed successfully.")












# import sys
# from awsglue.context import GlueContext
# from awsglue.utils import getResolvedOptions
# from awsglue.job import Job
# from pyspark.context import SparkContext

# # Get the arguments passed from the Lambda trigger
# args = getResolvedOptions(sys.argv, ['JOB_NAME', 'bucket_name', 'key'])

# bucket = args['bucket_name']
# key = args['key']

# # Initialize Spark and Glue context
# sc = SparkContext()
# glueContext = GlueContext(sc)
# spark = glueContext.spark_session

# # Initialize Glue Job
# job = Job(glueContext)
# job.init(args['JOB_NAME'], args)

# # Log the file being processed
# print(f"Starting Glue ETL job for file: s3://{bucket}/{key}")

# # Load the data
# input_path = f"s3://{bucket}/{key}"
# if key.endswith(".csv"):
#     df = spark.read.option("header", "true").csv(input_path)
#     output_path = "s3://outputathenabucket071611/athena-results/csv"
# elif key.endswith(".txt"):
#     df = spark.read.text(input_path)
#     output_path = "s3://outputathenabucket071611/athena-results/txt"
# else:
#     print("Unsupported file type")
#     job.commit()
#     sys.exit(1)

# # Show sample rows
# df.show(5)

# # Write the output for crawler to pick up
# df.write.mode("overwrite").option("header", "true").csv(output_path)

# print(f"Finished processing and writing to {output_path}")

# # Commit the job to signal completion
# job.commit()

