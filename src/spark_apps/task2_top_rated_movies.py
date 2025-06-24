#!/usr/bin/env python3
"""
Spark application for Task 2: Find the names of top 20 highest rated movies
This script reads raw data from S3 and processes it to find top rated movies
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
        .appName("Task2TopRatedMovies") \
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
        # Define ratings schema
        ratings_schema = StructType([
            StructField("user_id", IntegerType(), False),
            StructField("movie_id", IntegerType(), False),
            StructField("rating", IntegerType(), False),
            StructField("timestamp", LongType(), False)
        ])
        
        # Define movies schema
        movies_schema = StructType([
            StructField("movie_id", IntegerType(), False),
            StructField("title", StringType(), False),
            StructField("release_date", StringType(), False),
            StructField("video_release_date", StringType(), True),
            StructField("imdb_url", StringType(), True)
        ])
        
        # Read ratings data from S3
        ratings_path = f"s3a://{bucket_name}/raw_data/u.data"
        logger.info(f"Reading ratings data from: {ratings_path}")
        
        ratings_df = spark.read.csv(
            ratings_path,
            schema=ratings_schema,
            sep='\t'
        )
        
        # Read movies data from S3
        movies_path = f"s3a://{bucket_name}/raw_data/u.item"
        logger.info(f"Reading movies data from: {movies_path}")
        
        movies_df = spark.read.csv(
            movies_path,
            schema=movies_schema,
            sep='|'
        )
        
        logger.info(f"Successfully read ratings data: {ratings_df.count()} records")
        logger.info(f"Successfully read movies data: {movies_df.count()} records")
        
        return ratings_df, movies_df
        
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

def task2_top_rated_movies(spark, bucket_name):
    """Task 2: Find the names of top 20 highest rated movies (at least 35 times rated by Users)"""
    try:
        logger.info("Starting Task 2: Top rated movies")
        
        # Load data from S3
        ratings_df, movies_df = read_raw_data_from_s3(spark, bucket_name)
        
        # Show some basic info
        logger.info(f"Loaded {ratings_df.count()} ratings")
        logger.info(f"Loaded {movies_df.count()} movies")
        
        # Calculate average rating and count of ratings for each movie
        movie_stats = ratings_df.groupBy("movie_id") \
            .agg(
                avg("rating").alias("avg_rating"),
                count("rating").alias("rating_count")
            )
        
        # Filter movies with at least 35 ratings
        filtered_movies = movie_stats.filter(col("rating_count") >= 35)
        
        # Join with movies data to get movie titles
        result = filtered_movies.join(movies_df, "movie_id") \
            .select("movie_id", "title", "avg_rating", "rating_count") \
            .orderBy(col("avg_rating").desc(), col("rating_count").desc()) \
            .limit(20)
        
        # Show the result
        logger.info("Top 20 highest rated movies (at least 35 ratings):")
        result.show(truncate=False)
        
        # Write result to S3
        write_dataframe_to_s3(spark, result, "task2_top_rated_movies", bucket_name)
        
        logger.info("Task 2 completed: Top rated movies calculated and stored")
        return result
        
    except Exception as e:
        logger.error(f"Failed to complete Task 2: {e}")
        raise

def main():
    """Main function to run Task 2"""
    try:
        logger.info("Starting Task 2: Top rated movies")
        
        # Create Spark session
        spark = create_spark_session()
        
        # Get bucket name
        bucket_name = os.getenv('S3_BUCKET_NAME', 'movielens-data')
        
        # Run Task 2
        result = task2_top_rated_movies(spark, bucket_name)
        
        # Stop Spark session
        spark.stop()
        
        logger.info("Task 2 completed successfully")
        
    except Exception as e:
        logger.error(f"Failed to run Task 2: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 