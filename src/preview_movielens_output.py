#!/usr/bin/env python3
"""
Script to preview output data from MovieLens Analytics Pipeline
This script reads and displays the results from all 4 tasks in the pipeline
Uses only pandas and boto3 for simplicity and efficiency
"""

import os
import sys
import logging
import pandas as pd
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import json
from datetime import datetime
import io

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MovieLensOutputPreview:
    """Class to preview MovieLens pipeline output data using pandas"""
    
    def __init__(self, s3_endpoint='localhost:9000', s3_access_key='minioadmin', 
                 s3_secret_key='minioadmin', bucket_name='movielens-data'):
        """Initialize the preview class with S3/MinIO configuration"""
        self.s3_endpoint = s3_endpoint
        self.s3_access_key = s3_access_key
        self.s3_secret_key = s3_secret_key
        self.bucket_name = bucket_name
        
        # Initialize S3 client
        self.s3_client = boto3.client(
            's3',
            endpoint_url=f'http://{s3_endpoint}',
            aws_access_key_id=s3_access_key,
            aws_secret_access_key=s3_secret_key,
            region_name='us-east-1'  # Default region for MinIO
        )
        
        logger.info(f"Initialized MovieLens Output Preview for bucket: {bucket_name}")
    
    def check_s3_connection(self):
        """Test S3/MinIO connection"""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info(f"Successfully connected to S3/MinIO bucket: {self.bucket_name}")
            return True
        except (ClientError, NoCredentialsError) as e:
            logger.error(f"Failed to connect to S3/MinIO: {e}")
            return False
    
    def list_s3_objects(self, prefix=''):
        """List objects in S3 bucket with given prefix"""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            objects = response.get('Contents', [])
            return [obj['Key'] for obj in objects]
        except Exception as e:
            logger.error(f"Failed to list S3 objects: {e}")
            return []
    
    def read_parquet_from_s3(self, key):
        """Read Parquet file from S3/MinIO using pandas"""
        try:
            logger.info(f"Reading Parquet file: {key}")
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
            df = pd.read_parquet(io.BytesIO(response['Body'].read()))
            logger.info(f"Successfully read {len(df)} records from {key}")
            return df
        except Exception as e:
            logger.error(f"Failed to read {key}: {e}")
            return None
    
    def read_csv_from_s3(self, key, sep='\t', encoding='utf-8'):
        """Read CSV file from S3/MinIO using pandas"""
        try:
            logger.info(f"Reading CSV file: {key}")
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
            df = pd.read_csv(io.BytesIO(response['Body'].read()), sep=sep, encoding=encoding)
            logger.info(f"Successfully read {len(df)} records from {key}")
            return df
        except Exception as e:
            logger.error(f"Failed to read {key}: {e}")
            return None
    
    def preview_task1_output(self):
        """Preview Task 1: Mean age by occupation"""
        logger.info("\n" + "="*60)
        logger.info("TASK 1: MEAN AGE BY OCCUPATION")
        logger.info("="*60)
        
        try:
            # List files in the task1 directory
            task1_files = self.list_s3_objects('task1_mean_age_by_occupation')
            
            if task1_files:
                # Try to read the first parquet file found
                for file_key in task1_files:
                    if file_key.endswith('.parquet'):
                        df = self.read_parquet_from_s3(file_key)
                        break
                else:
                    logger.info("No Parquet files found in Task 1 directory")
                    return None
            else:
                logger.info("Task 1 directory not found")
                return None
            
            if df is not None:
                logger.info(f"Found {len(df)} occupation records")
                logger.info("Columns: " + ", ".join(df.columns.tolist()))
                
                logger.info("\nMean age by occupation:")
                logger.info(df.to_string(index=False))
                
                return df
            else:
                logger.info("Task 1 data not available. This might indicate that the pipeline hasn't run yet.")
                return None
            
        except Exception as e:
            logger.error(f"Failed to read Task 1 output: {e}")
            return None
    
    def preview_task2_output(self):
        """Preview Task 2: Top rated movies"""
        logger.info("\n" + "="*60)
        logger.info("TASK 2: TOP RATED MOVIES")
        logger.info("="*60)
        
        try:
            # List files in the task2 directory
            task2_files = self.list_s3_objects('task2_top_rated_movies')
            
            if task2_files:
                # Try to read the first parquet file found
                for file_key in task2_files:
                    if file_key.endswith('.parquet'):
                        df = self.read_parquet_from_s3(file_key)
                        break
                else:
                    logger.info("No Parquet files found in Task 2 directory")
                    return None
            else:
                logger.info("Task 2 directory not found")
                return None
            
            if df is not None:
                logger.info(f"Found {len(df)} top rated movies")
                logger.info("Columns: " + ", ".join(df.columns.tolist()))
                
                logger.info("\nTop 20 highest rated movies (at least 35 ratings):")
                logger.info(df.to_string(index=False))
                
                return df
            else:
                logger.info("Task 2 data not available. This might indicate that the pipeline hasn't run yet.")
                return None
            
        except Exception as e:
            logger.error(f"Failed to read Task 2 output: {e}")
            return None
    
    def preview_task3_output(self):
        """Preview Task 3: Top genres by age and occupation"""
        logger.info("\n" + "="*60)
        logger.info("TASK 3: TOP GENRES BY AGE AND OCCUPATION")
        logger.info("="*60)
        
        try:
            # List files in the task3 directory
            task3_files = self.list_s3_objects('processed_data/task3_top_genres_by_age_occupation')
            
            if task3_files:
                # Try to read the first parquet file found
                for file_key in task3_files:
                    if file_key.endswith('.parquet'):
                        df = self.read_parquet_from_s3(file_key)
                        break
                else:
                    logger.info("No Parquet files found in Task 3 directory")
                    return None
            else:
                logger.info("Task 3 directory not found")
                return None
            
            if df is not None:
                logger.info(f"Found {len(df)} genre records by age and occupation")
                logger.info("Columns: " + ", ".join(df.columns.tolist()))
                
                logger.info("\nSample data (first 15 records):")
                logger.info(df.head(15).to_string(index=False))
                
                # Group by age_group and occupation if columns exist
                if 'age_group' in df.columns and 'occupation' in df.columns:
                    logger.info("\nDetailed results (showing top 3 genres per age-occupation group):")
                    group_count = 0
                    for (age_group, occupation), group_df in df.groupby(['age_group', 'occupation']):
                        if group_count >= 3:  # Limit to 3 age-occupation groups
                            break
                        logger.info(f"\n  Age Group: {age_group}, Occupation: {occupation}")
                        top_genres = group_df.head(3)
                        logger.info(top_genres.to_string(index=False))
                        group_count += 1
                
                return df
            else:
                logger.info("Task 3 data not available. This might indicate that the pipeline hasn't run yet.")
                return None
            
        except Exception as e:
            logger.error(f"Failed to read Task 3 output: {e}")
            return None
    
    def preview_task4_output(self):
        """Preview Task 4: Similar movies"""
        logger.info("\n" + "="*60)
        logger.info("TASK 4: SIMILAR MOVIES")
        logger.info("="*60)
        
        try:
            # List files in the task4 directory
            task4_files = self.list_s3_objects('processed_data/task4/')
            
            similarities_df = None
            top_similar_df = None
            
            # Try to read movie similarities
            for file_key in task4_files:
                if 'movie_similarities' in file_key and file_key.endswith('.parquet'):
                    similarities_df = self.read_parquet_from_s3(file_key)
                    break
            
            if similarities_df is not None:
                logger.info(f"Found {len(similarities_df)} movie similarity pairs")
                logger.info("Columns: " + ", ".join(similarities_df.columns.tolist()))
                
                logger.info("\nSample movie similarities (first 10 records):")
                logger.info(similarities_df.head(10).to_string(index=False))
            
            # Try to read top similar movies for "Usual Suspects"
            for file_key in task4_files:
                if 'top_similar_usual_suspects' in file_key and file_key.endswith('.parquet'):
                    top_similar_df = self.read_parquet_from_s3(file_key)
                    break
            
            if top_similar_df is not None:
                logger.info(f"\nTop similar movies for 'Usual Suspects, The (1995)':")
                logger.info("Columns: " + ", ".join(top_similar_df.columns.tolist()))
                
                logger.info("\nDetailed similar movies for 'Usual Suspects':")
                logger.info(top_similar_df.to_string(index=False))
            
            if similarities_df is None and top_similar_df is None:
                logger.info("Task 4 data not available. This might indicate that the pipeline hasn't run yet.")
            
            return similarities_df, top_similar_df
            
        except Exception as e:
            logger.error(f"Failed to read Task 4 output: {e}")
            return None, None
    
    def preview_raw_data(self):
        """Preview raw data statistics"""
        logger.info("\n" + "="*60)
        logger.info("RAW DATA OVERVIEW")
        logger.info("="*60)
        
        try:
            # Check what raw data files exist
            raw_files = self.list_s3_objects('raw_data/')
            logger.info(f"Raw data files found: {len(raw_files)}")
            
            for file_key in raw_files:
                logger.info(f"  - {file_key}")
            
            # Try to read and show basic stats for each file
            # Users data
            users_df = self.read_csv_from_s3('raw_data/u.user', sep='|')
            if users_df is not None:
                logger.info(f"\nUsers data: {len(users_df)} records")
                logger.info("Columns: " + ", ".join(users_df.columns.tolist()))
                logger.info("Sample users:")
                logger.info(users_df.head(5).to_string(index=False))
            
            # Movies data
            movies_df = self.read_csv_from_s3('raw_data/u.item', sep='|', encoding='latin1')
            if movies_df is not None:
                logger.info(f"\nMovies data: {len(movies_df)} records")
                logger.info("Columns: " + ", ".join(movies_df.columns.tolist()))
                logger.info("Sample movies:")
                logger.info(movies_df.head(5).to_string(index=False))
            
            # Ratings data
            ratings_df = self.read_csv_from_s3('raw_data/u.data', sep='\t')
            if ratings_df is not None:
                logger.info(f"\nRatings data: {len(ratings_df)} records")
                logger.info("Columns: " + ", ".join(ratings_df.columns.tolist()))
                logger.info("Sample ratings:")
                logger.info(ratings_df.head(5).to_string(index=False))
                
        except Exception as e:
            logger.error(f"Failed to preview raw data: {e}")
    
    def preview_all_outputs(self):
        """Preview all pipeline outputs"""
        logger.info("Starting MovieLens Analytics Pipeline Output Preview")
        logger.info(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Check S3 connection
        if not self.check_s3_connection():
            logger.error("Cannot proceed without S3/MinIO connection")
            return False
        
        # Preview raw data
        self.preview_raw_data()
        
        # Preview each task output
        task1_df = self.preview_task1_output()
        task2_df = self.preview_task2_output()
        task3_df = self.preview_task3_output()
        task4_similarities, task4_top_similar = self.preview_task4_output()
        
        # Summary
        logger.info("\n" + "="*60)
        logger.info("SUMMARY")
        logger.info("="*60)
        
        tasks_status = {
            "Task 1 - Mean Age by Occupation": task1_df is not None,
            "Task 2 - Top Rated Movies": task2_df is not None,
            "Task 3 - Top Genres by Age/Occupation": task3_df is not None,
            "Task 4 - Similar Movies": task4_similarities is not None
        }
        
        for task_name, status in tasks_status.items():
            status_str = "✓ SUCCESS" if status else "✗ FAILED"
            logger.info(f"  {task_name}: {status_str}")
        
        success_count = sum(tasks_status.values())
        total_count = len(tasks_status)
        logger.info(f"\nOverall Status: {success_count}/{total_count} tasks completed successfully")
        
        if success_count == total_count:
            logger.info("🎉 All MovieLens analytics tasks completed successfully!")
        else:
            logger.warning("⚠️  Some tasks may have failed or data not available")
            logger.info("This is normal if the pipeline hasn't run yet.")
        
        return success_count == total_count

def main():
    """Main function"""
    try:
        # Get configuration from environment variables or use defaults
        s3_endpoint = os.getenv('S3_ENDPOINT', 'localhost:9000')
        s3_access_key = os.getenv('S3_ACCESS_KEY', 'minioadmin')
        s3_secret_key = os.getenv('S3_SECRET_KEY', 'minioadmin')
        bucket_name = os.getenv('S3_BUCKET_MOVIELENS', 'movielens-data')
        
        # Create preview instance
        preview = MovieLensOutputPreview(
            s3_endpoint=s3_endpoint,
            s3_access_key=s3_access_key,
            s3_secret_key=s3_secret_key,
            bucket_name=bucket_name
        )
        
        # Run preview
        success = preview.preview_all_outputs()
        
        # Exit with appropriate code
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        logger.info("Preview interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 