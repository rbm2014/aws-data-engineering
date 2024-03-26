
# This is a Spark script to be executed in AWS Glue
# the objective is to show how to programatically transform a CSV file into Parquet
# Change YOUR_BUCKET_NAME with the working bucket for source and destination, 
# it does not need to be the same bucket as long as your Glue job has the correct IAM role to acess the bucket(s)
# 
# This code is based on the datasets provided in the AWS workshop below:
# https://catalog.us-east-1.prod.workshops.aws/athena-immersion-day/en-US/30-basics/300-view-datasets
#
# sample data can be found at:
#
# - s3://ws-assets-prod-iad-r-iad-ed304a55c2ca1aee/9981f1a1-abdc-49b5-8387-cb01d238bb78/v1/csv/customers.csv
# - s3://ws-assets-prod-iad-r-iad-ed304a55c2ca1aee/9981f1a1-abdc-49b5-8387-cb01d238bb78/v1/csv/sales.csv 
# 
# To download the files use the following commands in your local machine using AWS CLI:
# aws s3 cp s3://ws-assets-prod-iad-r-iad-ed304a55c2ca1aee/9981f1a1-abdc-49b5-8387-cb01d238bb78/v1/csv/customers.csv ./customers.csv
# aws s3 cp s3://ws-assets-prod-iad-r-iad-ed304a55c2ca1aee/9981f1a1-abdc-49b5-8387-cb01d238bb78/v1/csv/sales.csv ./sales.csv
# 
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
# to-do   
# source_path = "s3://{}/path/to/csv_file.csv".format(args['SOURCE_BUCKET'])
# destination_path = "s3://{}/path/to/parquet_data/".format(args['DESTINATION_BUCKET'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Step 1: Read CSV data from source S3 bucket
source_path = "s3://<YOUR_BUCKET_NAME>/sample_data_csv/sales.csv"
df = spark.read.csv(source_path, header=True, inferSchema=True)

# Step 2: Perform necessary transformations (if any)
# Example transformation: df = df.filter(df['some_column'] > 100)

# Step 3: Create Partitioned DataFrame
# Convert timestamp to date and Add columns for year and month

# Convert timestamp column to a date column
from pyspark.sql.functions import year, month
#df = df.withColumn("date_column", to_date(df["timestamp"], "yyyy-MM-dd HH:mm:ss.SSSSSS"))

# Add columns for year and month
df = df.withColumn("year", year("timestamp")) \
       .withColumn("month", month("timestamp"))

# Step 4: Write DataFrame to Parquet partitioned by year and month
destination_path = "s3://<YOUR_BUCKET_NAME>/spark_tests/"
df.write.partitionBy("year", "month").parquet(destination_path, mode="overwrite")

# Step 5: Commit the job
job.commit()