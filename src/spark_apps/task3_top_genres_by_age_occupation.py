#!/usr/bin/env python3
"""
Spark application for Task 3: Find the top genres rated by users of each occupation in every age group
This script reads raw data from S3 and processes it to find top genres by age group and occupation
"""

import os
import sys
import logging
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create and configure Spark session"""
    spark = SparkSession.builder \
        .appName("Task3TopGenresByAgeOccupation") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    # Configure S3 settings (same as task2)
    s3_endpoint = os.getenv('S3_ENDPOINT', 'localhost:9000')
    s3_access_key = os.getenv('S3_ACCESS_KEY', 'minioadmin')
    s3_secret_key = os.getenv('S3_SECRET_KEY', 'minioadmin')
    bucket_name = os.getenv('S3_BUCKET_NAME', 'movielens-data')
    
    # Set Spark configuration for S3/MinIO (same as task2)
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

def read_users_data(spark, bucket_name):
    """Read users data from S3"""
    logger.info("Reading users data...")
    
    # Define user schema
    user_schema = StructType([
        StructField("user_id", IntegerType(), False),
        StructField("age", IntegerType(), False),
        StructField("gender", StringType(), False),
        StructField("occupation", StringType(), False),
        StructField("zip_code", StringType(), False)
    ])
    
    # Read users data (u.user) using CSV reader with pipe separator
    users_path = f"s3a://{bucket_name}/raw_data/u.user"
    logger.info(f"Reading users data from: {users_path}")
    
    users_df = spark.read.csv(
        users_path,
        schema=user_schema,
        sep='|'
    )
    
    logger.info(f"Users data loaded: {users_df.count()} records")
    return users_df

def read_ratings_data(spark, bucket_name):
    """Read ratings data from S3"""
    logger.info("Reading ratings data...")
    
    # Define ratings schema
    ratings_schema = StructType([
        StructField("user_id", IntegerType(), False),
        StructField("movie_id", IntegerType(), False),
        StructField("rating", IntegerType(), False),
        StructField("timestamp", LongType(), False)
    ])
    
    # Read ratings data (u.data) using CSV reader with tab separator
    ratings_path = f"s3a://{bucket_name}/raw_data/u.data"
    logger.info(f"Reading ratings data from: {ratings_path}")
    
    ratings_df = spark.read.csv(
        ratings_path,
        schema=ratings_schema,
        sep='\t'
    )
    
    logger.info(f"Ratings data loaded: {ratings_df.count()} records")
    return ratings_df

def read_movies_data(spark, bucket_name):
    """Read movies data from S3"""
    logger.info("Reading movies data...")
    
    # Define movies schema with all genre columns
    movies_schema = StructType([
        StructField("movie_id", IntegerType(), False),
        StructField("title", StringType(), False),
        StructField("release_date", StringType(), False),
        StructField("video_release_date", StringType(), True),
        StructField("imdb_url", StringType(), True),
        # Genre columns (19 genres starting from position 5)
        StructField("unknown", IntegerType(), True),
        StructField("action", IntegerType(), True),
        StructField("adventure", IntegerType(), True),
        StructField("animation", IntegerType(), True),
        StructField("children", IntegerType(), True),
        StructField("comedy", IntegerType(), True),
        StructField("crime", IntegerType(), True),
        StructField("documentary", IntegerType(), True),
        StructField("drama", IntegerType(), True),
        StructField("fantasy", IntegerType(), True),
        StructField("film_noir", IntegerType(), True),
        StructField("horror", IntegerType(), True),
        StructField("musical", IntegerType(), True),
        StructField("mystery", IntegerType(), True),
        StructField("romance", IntegerType(), True),
        StructField("sci_fi", IntegerType(), True),
        StructField("thriller", IntegerType(), True),
        StructField("war", IntegerType(), True),
        StructField("western", IntegerType(), True)
    ])
    
    # Read movies data (u.item) using CSV reader with pipe separator
    movies_path = f"s3a://{bucket_name}/raw_data/u.item"
    logger.info(f"Reading movies data from: {movies_path}")
    
    movies_df = spark.read.csv(
        movies_path,
        schema=movies_schema,
        sep='|',
        encoding='latin1'
    )
    
    logger.info(f"Movies data loaded: {movies_df.count()} records")
    return movies_df

def create_age_groups(users_df):
    """Create age groups based on the specified ranges"""
    logger.info("Creating age groups...")
    
    age_groups_df = users_df.withColumn(
        "age_group",
        when(col("age").between(20, 25), "20-25")
        .when(col("age").between(26, 35), "25-35")
        .when(col("age").between(36, 45), "35-45")
        .when(col("age") >= 45, "45 and older")
        .otherwise("Unknown")
    )
    
    return age_groups_df

def explode_genres(movies_df):
    """Explode movies to create one row per genre per movie"""
    logger.info("Exploding genres...")
    
    # Define genre columns (excluding unknown)
    genre_columns = [
        "action", "adventure", "animation", "children", "comedy", 
        "crime", "documentary", "drama", "fantasy", "film_noir", 
        "horror", "musical", "mystery", "romance", "sci_fi", 
        "thriller", "war", "western"
    ]
    
    # Create genre mapping
    genre_mapping = []
    for genre in genre_columns:
        genre_mapping.append(
            when(col(genre) == 1, lit(genre)).otherwise(lit(None))
        )
    
    # Explode genres
    movies_with_genres = movies_df.withColumn(
        "genre",
        coalesce(*genre_mapping)
    ).filter(col("genre").isNotNull())
    
    return movies_with_genres.select("movie_id", "title", "genre")

def process_top_genres(spark, bucket_name):
    """Main processing function"""
    logger.info("Starting top genres analysis...")
    
    try:
        # Read data
        users_df = read_users_data(spark, bucket_name)
        ratings_df = read_ratings_data(spark, bucket_name)
        movies_df = read_movies_data(spark, bucket_name)
        
        # Create age groups
        users_with_age_groups = create_age_groups(users_df)
        
        # Explode genres
        movies_with_genres = explode_genres(movies_df)
        
        # Join all data
        logger.info("Joining data...")
        joined_data = users_with_age_groups \
            .join(ratings_df, "user_id") \
            .join(movies_with_genres, "movie_id")
        
        # Calculate average ratings by age group, occupation, and genre
        logger.info("Calculating average ratings...")
        genre_ratings = joined_data \
            .groupBy("age_group", "occupation", "genre") \
            .agg(
                avg("rating").alias("avg_rating"),
                count("rating").alias("rating_count")
            ) \
            .filter(col("rating_count") >= 5)  # Minimum 5 ratings for meaningful average
        
        # Rank genres within each age group and occupation
        logger.info("Ranking genres...")
        window_spec = Window.partitionBy("age_group", "occupation") \
            .orderBy(col("avg_rating").desc(), col("rating_count").desc())
        
        ranked_genres = genre_ratings \
            .withColumn("rank", rank().over(window_spec)) \
            .filter(col("rank") <= 5)  # Top 5 genres per group
        
        # Show results
        logger.info("Top genres by age group and occupation:")
        ranked_genres.orderBy("age_group", "occupation", "rank").show(100, truncate=False)
        
        # Save results to S3
        logger.info("Saving results to S3...")
        ranked_genres.write \
            .mode("overwrite") \
            .parquet(f"s3a://{bucket_name}/processed_data/task3_top_genres_by_age_occupation")
        
        # Also save as CSV for easy viewing
        ranked_genres.write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(f"s3a://{bucket_name}/processed_data/task3_top_genres_by_age_occupation_csv")
        
        logger.info("Task 3 completed successfully!")
        return ranked_genres
        
    except Exception as e:
        logger.error(f"Error in process_top_genres: {str(e)}")
        raise

def main():
    """Main function"""
    try:
        logger.info("Starting Task 3: Top Genres by Age and Occupation")
        
        # Create Spark session
        spark = create_spark_session()
        
        # Get bucket name
        bucket_name = os.getenv('S3_BUCKET_NAME', 'movielens-data')
        
        # Process data
        results = process_top_genres(spark, bucket_name)
        
        # Stop Spark session
        spark.stop()
        
        logger.info("Task 3 completed successfully!")
        
    except Exception as e:
        logger.error(f"Failed to run Task 3: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 