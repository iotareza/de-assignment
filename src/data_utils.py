#!/usr/bin/env python3
"""
Utility functions for downloading and uploading MovieLens data
"""

import os
import sys
import requests
import zipfile
import logging
import tempfile
import boto3
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_s3_client():
    """Create S3 client for direct S3 operations"""
    s3_endpoint = os.getenv('S3_ENDPOINT', 'localhost:9000')
    s3_access_key = os.getenv('S3_ACCESS_KEY', 'minioadmin')
    s3_secret_key = os.getenv('S3_SECRET_KEY', 'minioadmin')
    
    s3_client = boto3.client(
        's3',
        endpoint_url=f'http://{s3_endpoint}',
        aws_access_key_id=s3_access_key,
        aws_secret_access_key=s3_secret_key,
        region_name='us-east-1'  # Default region for MinIO
    )
    
    return s3_client

def ensure_bucket_exists(s3_client, bucket_name):
    """Ensure S3 bucket exists"""
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        logger.info(f"Bucket {bucket_name} already exists")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            logger.info(f"Creating bucket {bucket_name}")
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            raise

def download_and_upload_movielens_data():
    """Download MovieLens data and upload to S3"""
    try:
        s3_client = create_s3_client()
        bucket_name = os.getenv('S3_BUCKET_NAME', 'movielens-data')
        
        # Ensure bucket exists
        ensure_bucket_exists(s3_client, bucket_name)
        
        # Create temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            # Download the dataset
            url = "https://files.grouplens.org/datasets/movielens/ml-100k.zip"
            zip_path = os.path.join(temp_dir, "ml-100k.zip")
            
            logger.info(f"Downloading MovieLens dataset from {url}")
            response = requests.get(url, stream=True)
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            downloaded_size = 0
            
            with open(zip_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
                    downloaded_size += len(chunk)
                    if total_size > 0:
                        progress = (downloaded_size / total_size) * 100
                        logger.info(f"Download progress: {progress:.1f}%")
            
            # Verify download
            file_size = os.path.getsize(zip_path)
            logger.info(f"Downloaded file size: {file_size} bytes")
            
            if file_size == 0:
                raise Exception("Downloaded file is empty")
            
            # Extract the zip file
            logger.info("Extracting zip file")
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)
            
            # Upload extracted files to S3
            extracted_dir = os.path.join(temp_dir, "ml-100k")
            if not os.path.exists(extracted_dir):
                raise Exception(f"Extracted directory {extracted_dir} does not exist")
            
            # List and upload files
            extracted_files = os.listdir(extracted_dir)
            logger.info(f"Extracted files: {extracted_files}")
            
            # Required files for the analytics tasks
            required_files = ['u.data', 'u.user', 'u.item']
            uploaded_files = []
            
            for file_name in extracted_files:
                file_path = os.path.join(extracted_dir, file_name)
                s3_key = f"raw_data/{file_name}"
                
                logger.info(f"Uploading {file_name} to s3://{bucket_name}/{s3_key}")
                s3_client.upload_file(file_path, bucket_name, s3_key)
                
                # Verify upload
                try:
                    s3_client.head_object(Bucket=bucket_name, Key=s3_key)
                    logger.info(f"Successfully uploaded {file_name}")
                    uploaded_files.append(file_name)
                except ClientError:
                    raise Exception(f"Failed to verify upload of {file_name}")
            
            # Verify all required files were uploaded
            missing_files = [f for f in required_files if f not in uploaded_files]
            if missing_files:
                raise Exception(f"Missing required files: {missing_files}")
            
            logger.info(f"All required files uploaded successfully: {required_files}")
        
        logger.info("All files uploaded to S3 successfully")
        return True
        
    except Exception as e:
        logger.error(f"Failed to download and upload MovieLens data: {e}")
        raise

def verify_s3_data():
    """Verify that the data was uploaded to S3"""
    try:
        s3_client = create_s3_client()
        bucket_name = os.getenv('S3_BUCKET_NAME', 'movielens-data')
        
        # Check for required files
        required_files = ['raw_data/u.data', 'raw_data/u.user', 'raw_data/u.item']
        
        for s3_key in required_files:
            try:
                s3_client.head_object(Bucket=bucket_name, Key=s3_key)
                logger.info(f"Verified {s3_key} exists in S3")
            except ClientError:
                raise Exception(f"Required file {s3_key} not found in S3")
        
        logger.info("All required files verified in S3")
        return True
        
    except Exception as e:
        logger.error(f"Failed to verify S3 data: {e}")
        raise 