"""
S3 Storage class for handling S3/MinIO operations using connection pool
"""

import json
import logging
import os
from datetime import datetime
from typing import Dict, Any, Optional
from .connection_pool import get_s3_client
import io

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class S3Storage:
    """S3/MinIO storage handler using connection pool"""
    
    def __init__(self):
        self.bucket_name = os.getenv('S3_BUCKET_NAME', 'news-data')
        self._ensure_bucket_exists()
    
    def _ensure_bucket_exists(self):
        """Ensure the S3 bucket exists"""
        try:
            s3_client = get_s3_client()
            if not s3_client:
                logger.error("S3 client not available")
                return
            
            # Check if bucket exists
            if hasattr(s3_client, 'bucket_exists'):  # MinIO
                if not s3_client.bucket_exists(self.bucket_name):
                    s3_client.make_bucket(self.bucket_name)
                    logger.info(f"Created MinIO bucket: {self.bucket_name}")
                else:
                    logger.info(f"MinIO bucket exists: {self.bucket_name}")
            else:  # AWS S3
                try:
                    s3_client.head_bucket(Bucket=self.bucket_name)
                    logger.info(f"S3 bucket exists: {self.bucket_name}")
                except:
                    s3_client.create_bucket(Bucket=self.bucket_name)
                    logger.info(f"Created S3 bucket: {self.bucket_name}")
                    
        except Exception as e:
            logger.error(f"Error ensuring bucket exists: {e}")
    
    def save_raw_data(self, data: Dict[str, Any], timestamp: str) -> str:
        """
        Save raw extracted data to S3
        Returns: S3 key for the saved data
        """
        try:
            s3_client = get_s3_client()
            if not s3_client:
                logger.error("S3 client not available")
                return ""
            
            key = f"raw_data/{timestamp}_extracted_data.json"
            
            if hasattr(s3_client, 'put_object'):  # MinIO
                s3_client.put_object(
                    bucket_name=self.bucket_name,
                    object_name=key,
                    data=json.dumps(data, indent=2, ensure_ascii=False),
                    length=len(json.dumps(data, indent=2, ensure_ascii=False)),
                    content_type='application/json'
                )
            else:  # AWS S3
                s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=key,
                    Body=json.dumps(data, indent=2, ensure_ascii=False),
                    ContentType='application/json'
                )
            
            logger.info(f"Saved raw data to S3: {key}")
            return key
            
        except Exception as e:
            logger.error(f"Error saving raw data to S3: {e}")
            return ""
    
    def save_processed_data(self, data: Dict[str, Any], timestamp: str) -> str:
        """
        Save processed data to S3
        Returns: S3 key for the saved data
        """
        try:
            s3_client = get_s3_client()
            if not s3_client:
                logger.error("S3 client not available")
                return ""
            
            key = f"processed_data/{timestamp}_processed_data.json"
            
            if hasattr(s3_client, 'put_object'):  # MinIO
                s3_client.put_object(
                    bucket_name=self.bucket_name,
                    object_name=key,
                    data=json.dumps(data, indent=2, ensure_ascii=False),
                    length=len(json.dumps(data, indent=2, ensure_ascii=False)),
                    content_type='application/json'
                )
            else:  # AWS S3
                s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=key,
                    Body=json.dumps(data, indent=2, ensure_ascii=False),
                    ContentType='application/json'
                )
            
            logger.info(f"Saved processed data to S3: {key}")
            return key
            
        except Exception as e:
            logger.error(f"Error saving processed data to S3: {e}")
            return ""
    
    def save_sentiment_data(self, data: Dict[str, Any], timestamp: str) -> str:
        """
        Save sentiment analysis data to S3
        Returns: S3 key for the saved data
        """
        try:
            s3_client = get_s3_client()
            if not s3_client:
                logger.error("S3 client not available")
                return ""
            
            key = f"sentiment_data/{timestamp}_sentiment_data.json"
            
            if hasattr(s3_client, 'put_object'):  # MinIO
                s3_client.put_object(
                    bucket_name=self.bucket_name,
                    object_name=key,
                    data=json.dumps(data, indent=2, ensure_ascii=False),
                    length=len(json.dumps(data, indent=2, ensure_ascii=False)),
                    content_type='application/json'
                )
            else:  # AWS S3
                s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=key,
                    Body=json.dumps(data, indent=2, ensure_ascii=False),
                    ContentType='application/json'
                )
            
            logger.info(f"Saved sentiment data to S3: {key}")
            return key
            
        except Exception as e:
            logger.error(f"Error saving sentiment data to S3: {e}")
            return ""
    
    def load_data(self, key: str) -> Optional[Dict[str, Any]]:
        """
        Load data from S3
        Returns: Loaded data or None if error
        """
        try:
            s3_client = get_s3_client()
            if not s3_client:
                logger.error("S3 client not available")
                return None
            
            if hasattr(s3_client, 'get_object'):  # MinIO
                response = s3_client.get_object(
                    bucket_name=self.bucket_name,
                    object_name=key
                )
                data = json.loads(response.read().decode('utf-8'))
            else:  # AWS S3
                response = s3_client.get_object(
                    Bucket=self.bucket_name,
                    Key=key
                )
                data = json.loads(response['Body'].read().decode('utf-8'))
            
            logger.info(f"Loaded data from S3: {key}")
            return data
            
        except Exception as e:
            logger.error(f"Error loading data from S3: {e}")
            return None
    
    def delete_data(self, key: str) -> bool:
        """
        Delete data from S3
        Returns: True if successful, False otherwise
        """
        try:
            s3_client = get_s3_client()
            if not s3_client:
                logger.error("S3 client not available")
                return False
            
            if hasattr(s3_client, 'remove_object'):  # MinIO
                s3_client.remove_object(
                    bucket_name=self.bucket_name,
                    object_name=key
                )
            else:  # AWS S3
                s3_client.delete_object(
                    Bucket=self.bucket_name,
                    Key=key
                )
            
            logger.info(f"Deleted data from S3: {key}")
            return True
            
        except Exception as e:
            logger.error(f"Error deleting data from S3: {e}")
            return False
    
    def list_data(self, prefix: str = "") -> list:
        """
        List data in S3 with optional prefix
        Returns: List of object keys
        """
        try:
            s3_client = get_s3_client()
            if not s3_client:
                logger.error("S3 client not available")
                return []
            
            keys = []
            
            if hasattr(s3_client, 'list_objects'):  # MinIO
                objects = s3_client.list_objects(
                    bucket_name=self.bucket_name,
                    prefix=prefix,
                    recursive=True
                )
                for obj in objects:
                    keys.append(obj.object_name)
            else:  # AWS S3
                response = s3_client.list_objects_v2(
                    Bucket=self.bucket_name,
                    Prefix=prefix
                )
                if 'Contents' in response:
                    for obj in response['Contents']:
                        keys.append(obj['Key'])
            
            logger.info(f"Listed {len(keys)} objects from S3 with prefix: {prefix}")
            return keys
            
        except Exception as e:
            logger.error(f"Error listing data from S3: {e}")
            return []
    
    def get_data_size(self, key: str) -> int:
        """
        Get size of data in S3
        Returns: Size in bytes, -1 if error
        """
        try:
            s3_client = get_s3_client()
            if not s3_client:
                logger.error("S3 client not available")
                return -1
            
            if hasattr(s3_client, 'stat_object'):  # MinIO
                stat = s3_client.stat_object(
                    bucket_name=self.bucket_name,
                    object_name=key
                )
                return stat.size
            else:  # AWS S3
                response = s3_client.head_object(
                    Bucket=self.bucket_name,
                    Key=key
                )
                return response['ContentLength']
            
        except Exception as e:
            logger.error(f"Error getting data size from S3: {e}")
            return -1
    
    def data_exists(self, key: str) -> bool:
        """
        Check if data exists in S3
        Returns: True if exists, False otherwise
        """
        try:
            s3_client = get_s3_client()
            if not s3_client:
                logger.error("S3 client not available")
                return False
            
            if hasattr(s3_client, 'stat_object'):  # MinIO
                s3_client.stat_object(
                    bucket_name=self.bucket_name,
                    object_name=key
                )
                return True
            else:  # AWS S3
                s3_client.head_object(
                    Bucket=self.bucket_name,
                    Key=key
                )
                return True
            
        except:
            return False
    
    def save_data(self, data: Dict[str, Any], key: str) -> bool:
        """
        Save data to S3 with a custom key
        Returns: True if successful, False otherwise
        """
        try:
            s3_client = get_s3_client()
            if not s3_client:
                logger.error("S3 client not available")
                return False
            
            json_bytes = json.dumps(data, indent=2, ensure_ascii=False).encode('utf-8')
            if hasattr(s3_client, 'put_object'):  # MinIO
                s3_client.put_object(
                    bucket_name=self.bucket_name,
                    object_name=key,
                    data=io.BytesIO(json_bytes),
                    length=len(json_bytes),
                    content_type='application/json'
                )
            else:  # AWS S3
                s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=key,
                    Body=json_bytes,
                    ContentType='application/json'
                )
            
            logger.info(f"Saved data to S3: {key}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving data to S3: {e}")
            return False 