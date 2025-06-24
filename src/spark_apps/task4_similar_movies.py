#!/usr/bin/env python3
"""
Spark application for Task 4: Find top 10 similar movies based on user ratings
This script reads raw data from S3 and processes it to find similar movies using correlation analysis
"""

import os
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
import numpy as np

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create and configure Spark session"""
    spark = SparkSession.builder \
        .appName("Task4SimilarMovies") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    # Configure S3 settings
    spark.conf.set("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    spark.conf.set("spark.hadoop.fs.s3a.access.key", "minioadmin")
    spark.conf.set("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
    spark.conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    spark.conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    
    return spark

def load_data(spark):
    """Load ratings and movies data from S3"""
    logger.info("Loading data from S3...")
    
    # Load ratings data
    ratings_df = spark.read.csv(
        "s3a://movielens-data/raw_data/u.data",
        sep='\t',
        schema=StructType([
            StructField("user_id", IntegerType(), False),
            StructField("movie_id", IntegerType(), False),
            StructField("rating", IntegerType(), False),
            StructField("timestamp", LongType(), False)
        ])
    )
    
    # Load movies data
    movies_df = spark.read.csv(
        "s3a://movielens-data/raw_data/u.item",
        sep='|',
        encoding='latin1',
        schema=StructType([
            StructField("movie_id", IntegerType(), False),
            StructField("title", StringType(), False),
            StructField("release_date", StringType(), True),
            StructField("video_release_date", StringType(), True),
            StructField("imdb_url", StringType(), True)
        ] + [StructField(f"genre_{i}", IntegerType(), True) for i in range(19)])
    )
    
    logger.info(f"Loaded {ratings_df.count()} ratings and {movies_df.count()} movies")
    return ratings_df, movies_df

def calculate_movie_similarity(spark, ratings_df, movies_df, similarity_threshold=0.3, cooccurrence_threshold=10):
    """
    Calculate movie similarity based on user ratings
    Using Pearson correlation and co-occurrence analysis
    """
    logger.info("Calculating movie similarities...")
    logger.info(f"Using similarity threshold: {similarity_threshold}, co-occurrence threshold: {cooccurrence_threshold}")
    
    # Create user-movie rating matrix
    rating_matrix = ratings_df.groupBy("user_id").pivot("movie_id").agg(avg("rating")).fillna(0)
    
    # Get movie IDs that have sufficient ratings
    movie_counts = ratings_df.groupBy("movie_id").count()
    popular_movies = movie_counts.filter(col("count") >= cooccurrence_threshold).select("movie_id")
    
    logger.info(f"Found {popular_movies.count()} movies with at least {cooccurrence_threshold} ratings")
    
    # Filter rating matrix to only include popular movies
    movie_columns = [col for col in rating_matrix.columns if col != "user_id"]
    # Convert string column names to integers for the join
    movie_id_ints = [int(col) for col in movie_columns]
    
    popular_movie_columns = popular_movies.join(
        spark.createDataFrame([(movie_id,) for movie_id in movie_id_ints], ["movie_id"]),
        "movie_id"
    ).select("movie_id").collect()
    
    # Convert back to strings for column selection (to match rating_matrix column names)
    popular_movie_cols = [str(row.movie_id) for row in popular_movie_columns]
    logger.info(f"Processing {len(popular_movie_cols)} popular movies for similarity calculation")
    
    if len(popular_movie_cols) < 2:
        logger.warning("Not enough popular movies found for similarity calculation")
        return spark.createDataFrame([], ["movie_id", "similar_movie_id", "similarity_score", "cooccurrence_count"])
    
    filtered_rating_matrix = rating_matrix.select("user_id", *popular_movie_cols)
    
    # Calculate correlation matrix
    assembler = VectorAssembler(inputCols=popular_movie_cols, outputCol="features")
    vector_df = assembler.transform(filtered_rating_matrix)
    
    # Calculate correlation matrix
    correlation_matrix = Correlation.corr(vector_df, "features", "pearson")
    correlation_array = correlation_matrix.collect()[0][0].toArray()
    
    logger.info(f"Correlation matrix calculated with shape: {correlation_array.shape}")
    
    # Create similarity pairs
    similarities = []
    valid_pairs = 0
    total_pairs = 0
    
    for i in range(len(popular_movie_cols)):
        for j in range(i + 1, len(popular_movie_cols)):
            total_pairs += 1
            movie1_id_str = popular_movie_cols[i]
            movie2_id_str = popular_movie_cols[j]
            # Convert string movie IDs to integers for filtering
            movie1_id_int = int(movie1_id_str)
            movie2_id_int = int(movie2_id_str)
            # Convert numpy float64 to Python float for PySpark compatibility
            similarity = float(correlation_array[i][j])
            
            if not np.isnan(similarity) and similarity >= similarity_threshold:
                # Calculate co-occurrence
                cooccurrence = ratings_df.filter(
                    (col("movie_id") == movie1_id_int) | (col("movie_id") == movie2_id_int)
                ).groupBy("user_id").agg(
                    countDistinct("movie_id").alias("movies_rated")
                ).filter(col("movies_rated") == 2).count()
                
                if cooccurrence >= cooccurrence_threshold:
                    similarities.append((movie1_id_int, movie2_id_int, similarity, cooccurrence))
                    similarities.append((movie2_id_int, movie1_id_int, similarity, cooccurrence))
                    valid_pairs += 1
    
    logger.info(f"Found {valid_pairs} valid similar movie pairs out of {total_pairs} total pairs")
    
    if not similarities:
        logger.warning("No similar movie pairs found. Consider lowering thresholds.")
        # Return empty DataFrame with proper schema
        return spark.createDataFrame([], ["movie_id", "similar_movie_id", "similarity_score", "cooccurrence_count"])
    
    return spark.createDataFrame(similarities, ["movie_id", "similar_movie_id", "similarity_score", "cooccurrence_count"])

def find_top_similar_movies(similarities_df, movies_df, target_movie_title="Usual Suspects, The (1995)", top_n=10):
    """Find top N similar movies for a given movie"""
    logger.info(f"Finding top {top_n} similar movies for: {target_movie_title}")
    
    # Get movie ID for target movie
    target_movie = movies_df.filter(col("title") == target_movie_title).first()
    if not target_movie:
        logger.error(f"Movie '{target_movie_title}' not found")
        return None
    
    target_movie_id = target_movie.movie_id
    
    # Find similar movies
    similar_movies = similarities_df.filter(col("movie_id") == target_movie_id) \
        .join(movies_df, similarities_df.similar_movie_id == movies_df.movie_id) \
        .select(
            "similar_movie_id",
            "title",
            "similarity_score",
            "cooccurrence_count"
        ) \
        .orderBy(col("similarity_score").desc()) \
        .limit(top_n)
    
    return similar_movies

def save_results(similarities_df, output_path):
    """Save similarity results to S3 using Spark's native S3 support"""
    logger.info(f"Saving results to {output_path}")
    
    try:
        # Save all similarities as Parquet using Spark
        similarities_df.write.mode("overwrite").parquet(f"{output_path}/movie_similarities")
        
        # Save as CSV using Spark's native CSV writer
        similarities_df.write.mode("overwrite").option("header", "true").csv(f"{output_path}/movie_similarities_csv")
        
        logger.info("Results saved successfully using Spark native S3 support")
        
    except Exception as e:
        logger.error(f"Failed to save results to S3: {e}")
        raise

def main():
    """Main function"""
    try:
        # Create Spark session
        spark = create_spark_session()
        logger.info("Spark session created successfully")
        
        # Load data
        ratings_df, movies_df = load_data(spark)
        
        # Calculate similarities
        similarities_df = calculate_movie_similarity(
            spark, 
            ratings_df, 
            movies_df, 
            similarity_threshold=0.7, 
            cooccurrence_threshold=30
        )
        
        # Check if we have any similarities
        if similarities_df.count() == 0:
            logger.warning("No movie similarities found. Saving empty results.")
            # Create empty output directories
            save_results(similarities_df, "s3a://movielens-data/processed_data/task4")
            logger.info("Task 4 completed with no similarities found")
            return
        
        # Save all similarities
        save_results(similarities_df, "s3a://movielens-data/processed_data/task4")
        
        # Find top similar movies for "Usual Suspects, The (1995)"
        top_similar = find_top_similar_movies(similarities_df, movies_df)
        
        if top_similar and top_similar.count() > 0:
            logger.info("Top 10 similar movies for 'Usual Suspects, The (1995)':")
            top_similar.show(truncate=False)
            
            # Save top similar movies using Spark's native S3 support
            top_similar.write.mode("overwrite").parquet("s3a://movielens-data/processed_data/task4/top_similar_usual_suspects")
            top_similar.write.mode("overwrite").option("header", "true").csv("s3a://movielens-data/processed_data/task4/top_similar_usual_suspects_csv")
        else:
            logger.warning("No similar movies found for 'Usual Suspects, The (1995)'")
        
        logger.info("Task 4 completed successfully")
        
    except Exception as e:
        logger.error(f"Error in Task 4: {str(e)}")
        raise
    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    main() 