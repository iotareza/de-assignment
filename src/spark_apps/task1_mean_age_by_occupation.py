#!/usr/bin/env python3
"""
Spark application for Task 1: Find the mean age of users in each occupation
This script reads raw data from S3 and processes it
"""

import os
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create and configure Spark session"""
    spark = SparkSession.builder \
        .appName("Task1MeanAgeByOccupation") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    # Configure S3 settings
    s3_endpoint = os.getenv('S3_ENDPOINT', 'localhost:9000')
    s3_access_key = os.getenv('S3_ACCESS_KEY', 'minioadmin')
    s3_secret_key = os.getenv('S3_SECRET_KEY', 'minioadmin')
    bucket_name = os.getenv('S3_BUCKET_NAME', 'movielens-data')
    
    # Set Spark configuration for S3/MinIO
    spark.conf.set("spark.hadoop.fs.s3a.endpoint", f"http://{s3_endpoint}")
    spark.conf.set("spark.hadoop.fs.s3a.access.key", s3_access_key)
    spark.conf.set("spark.hadoop.fs.s3a.secret.key", s3_secret_key)
    spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
    spark.conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    spark.conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    spark.conf.set("spark.hadoop.fs.s3a.connection.maximum", "1000")
    spark.conf.set("spark.hadoop.fs.s3a.threads.max", "20")
    
    logger.info("Spark session created successfully with S3 configuration")
    return spark

def read_raw_data_from_s3(spark, bucket_name):
    """Read raw data files from S3"""
    try:
        # Define user schema
        user_schema = StructType([
            StructField("user_id", IntegerType(), False),
            StructField("age", IntegerType(), False),
            StructField("gender", StringType(), False),
            StructField("occupation", StringType(), False),
            StructField("zip_code", StringType(), False)
        ])
        
        # Read users data from S3
        users_path = f"s3a://{bucket_name}/raw_data/u.user"
        logger.info(f"Reading users data from: {users_path}")
        
        users_df = spark.read.csv(
            users_path,
            schema=user_schema,
            sep='|'
        )
        
        logger.info(f"Successfully read users data: {users_df.count()} records")
        return users_df
        
    except Exception as e:
        logger.error(f"Failed to read raw data from S3: {e}")
        raise

def write_dataframe_to_s3(spark, df, key, bucket_name):
    """Write DataFrame to S3 as Parquet using Spark's native S3 support"""
    try:
        # Write directly to S3 using Spark
        s3_path = f"s3a://{bucket_name}/{key}.parquet"
        df.write.mode("overwrite").parquet(s3_path)
        
        logger.info(f"Successfully wrote {key} to S3 using Spark native S3 support")
        
    except Exception as e:
        logger.error(f"Failed to write {key} to S3: {e}")
        raise

def task1_mean_age_by_occupation(spark, bucket_name):
    """Task 1: Find the mean age of users in each occupation"""
    try:
        logger.info("Starting Task 1: Mean age by occupation")
        
        # Load users data from S3
        users_df = read_raw_data_from_s3(spark, bucket_name)
        
        # Show some basic info
        logger.info(f"Loaded {users_df.count()} users")
        users_df.printSchema()
        
        # Process the data - calculate mean age by occupation
        result = users_df.groupBy("occupation") \
            .agg(avg("age").alias("mean_age")) \
            .orderBy("occupation")
        
        # Show the result
        logger.info("Mean age by occupation:")
        result.show()
        
        # Write result to S3
        write_dataframe_to_s3(spark, result, "task1_mean_age_by_occupation", bucket_name)
        
        logger.info("Task 1 completed: Mean age by occupation calculated and stored")
        return result
        
    except Exception as e:
        logger.error(f"Failed to complete Task 1: {e}")
        raise

def main():
    """Main function to run Task 1"""
    try:
        logger.info("Starting Task 1: Mean age by occupation")
        
        # Create Spark session
        spark = create_spark_session()
        
        # Get bucket name
        bucket_name = os.getenv('S3_BUCKET_NAME', 'movielens-data')
        
        # Run Task 1
        result = task1_mean_age_by_occupation(spark, bucket_name)
        
        # Stop Spark session
        spark.stop()
        
        logger.info("Task 1 completed successfully")
        
    except Exception as e:
        logger.error(f"Failed to run Task 1: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 