#!/usr/bin/env python3
"""
Python script to download and upload MovieLens data to S3
This script can be run independently or imported as a module
"""

import os
import sys
import logging
from data_utils import download_and_upload_movielens_data, verify_s3_data

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """Main function to download and upload MovieLens data"""
    try:
        logger.info("Starting MovieLens data download and upload process")
        
        # Download and upload data to S3
        download_and_upload_movielens_data()
        
        # Verify the data was uploaded
        verify_s3_data()
        
        logger.info("MovieLens data download and upload completed successfully")
        
    except Exception as e:
        logger.error(f"Failed to download and upload MovieLens data: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 