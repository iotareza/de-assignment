"""
Configuration file for scalable news extraction system
"""
from typing import List, Dict, Any
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Stock configurations
STOCKS = [
    {
        'name': 'HDFC',
        'keywords': ['HDFC', 'HDFC Bank', 'HDFC Limited'],
        'priority': 'high'
    },
    {
        'name': 'Tata Motors',
        'keywords': ['Tata Motors', 'Tata', 'JLR'],
        'priority': 'high'
    },
    {
        'name': 'Reliance',
        'keywords': ['Reliance', 'RIL', 'Jio'],
        'priority': 'medium'
    },
    {
        'name': 'TCS',
        'keywords': ['TCS', 'Tata Consultancy Services'],
        'priority': 'medium'
    }
]

# News source configurations
NEWS_SOURCES = [
    {
        'name': 'YourStory',
        'base_url': 'https://yourstory.com',
        'list_maker_class': 'YourStoryListMaker',
        'enabled': True,
        'rate_limit': 2,  # seconds between requests
        'max_articles_per_stock': 5
    },
    {
        'name': 'Finshots',
        'base_url': 'https://finshots.in',
        'list_maker_class': 'FinshotsListMaker',
        'enabled': True,
        'rate_limit': 1,
        'max_articles_per_stock': 5,
        'api_key': '8067c49caa4ce48ca16b4c4445'
    },
    {
        'name': 'MoneyControl',
        'base_url': 'https://www.moneycontrol.com',
        'list_maker_class': 'MoneyControlListMaker',
        'enabled': False,  # Disabled for now
        'rate_limit': 3,
        'max_articles_per_stock': 5
    }
]

# Processing configurations
PROCESSING_CONFIG = {
    'batch_size': 10,  # Number of stocks to process in parallel
    'max_workers': 4,  # Maximum number of worker processes
    'timeout': 30,  # Request timeout in seconds
    'retry_attempts': 3,
    'retry_delay': 5
}

# Storage configurations
STORAGE_CONFIG = {
    's3_bucket': os.getenv('S3_BUCKET', 'news-sentiment-data'),
    'local_temp_dir': 'data/temp',
    'bloom_filter_size': 10000,  # Size of bloom filter for URL deduplication
    'bloom_filter_error_rate': 0.01  # 1% false positive rate
}

# Database configurations
DATABASE_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', 5432),
    'database': os.getenv('DB_NAME', 'sentiment_db'),
    'username': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'password')
}

# Airflow configurations
AIRFLOW_CONFIG = {
    'parallel_tasks': 4,  # Number of parallel tasks in Airflow
    'task_timeout': 3600,  # 1 hour timeout per task
    'dag_schedule': '0 19 * * 1-5',  # 7 PM every working day
    'catchup': False
}

class Config:
    # S3/MinIO Configuration
    S3_ENDPOINT = os.getenv('S3_ENDPOINT', 'localhost:9000')
    S3_ACCESS_KEY = os.getenv('S3_ACCESS_KEY', 'minioadmin')
    S3_SECRET_KEY = os.getenv('S3_SECRET_KEY', 'minioadmin')
    S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME', 'news-data')
    S3_USE_SSL = os.getenv('S3_USE_SSL', 'false').lower() == 'true'
    S3_REGION = os.getenv('S3_REGION', 'us-east-1')
    S3_MAX_POOL_CONNECTIONS = int(os.getenv('S3_MAX_POOL_CONNECTIONS', '10'))
    
    # PostgreSQL Configuration
    POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
    POSTGRES_PORT = int(os.getenv('POSTGRES_PORT', '5432'))
    POSTGRES_DB = os.getenv('POSTGRES_DB', 'sentiment_db')
    POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
    POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'postgres')
    POSTGRES_MIN_CONNECTIONS = int(os.getenv('POSTGRES_MIN_CONNECTIONS', '1'))
    POSTGRES_MAX_CONNECTIONS = int(os.getenv('POSTGRES_MAX_CONNECTIONS', '10'))
    POSTGRES_CONNECTION_TIMEOUT = int(os.getenv('POSTGRES_CONNECTION_TIMEOUT', '30'))
    
    # Redis Configuration
    REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
    REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))
    REDIS_DB = int(os.getenv('REDIS_DB', '0'))
    REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', None)
    REDIS_CONNECT_TIMEOUT = int(os.getenv('REDIS_CONNECT_TIMEOUT', '5'))
    REDIS_TIMEOUT = int(os.getenv('REDIS_TIMEOUT', '5'))
    REDIS_MAX_CONNECTIONS = int(os.getenv('REDIS_MAX_CONNECTIONS', '10'))
    
    # Airflow Configuration
    AIRFLOW_HOME = os.getenv('AIRFLOW_HOME', '/opt/airflow')
    
    # Application Configuration
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    
    @classmethod
    def get_postgres_connection_string(cls):
        """Get PostgreSQL connection string"""
        return f"postgresql://{cls.POSTGRES_USER}:{cls.POSTGRES_PASSWORD}@{cls.POSTGRES_HOST}:{cls.POSTGRES_PORT}/{cls.POSTGRES_DB}"
    
    @classmethod
    def get_s3_client_config(cls):
        """Get S3 client configuration"""
        return {
            'endpoint_url': f"http{'s' if cls.S3_USE_SSL else ''}://{cls.S3_ENDPOINT}",
            'aws_access_key_id': cls.S3_ACCESS_KEY,
            'aws_secret_access_key': cls.S3_SECRET_KEY,
            'use_ssl': cls.S3_USE_SSL
        }
    
    @classmethod
    def get_postgres_config(cls):
        """Get PostgreSQL configuration for connection pool"""
        return {
            'host': cls.POSTGRES_HOST,
            'port': cls.POSTGRES_PORT,
            'database': cls.POSTGRES_DB,
            'user': cls.POSTGRES_USER,
            'password': cls.POSTGRES_PASSWORD,
            'min_connections': cls.POSTGRES_MIN_CONNECTIONS,
            'max_connections': cls.POSTGRES_MAX_CONNECTIONS,
            'connection_timeout': cls.POSTGRES_CONNECTION_TIMEOUT
        }
    
    @classmethod
    def get_redis_config(cls):
        """Get Redis configuration"""
        return {
            'host': cls.REDIS_HOST,
            'port': cls.REDIS_PORT,
            'db': cls.REDIS_DB,
            'password': cls.REDIS_PASSWORD,
            'decode_responses': True,
            'socket_connect_timeout': cls.REDIS_CONNECT_TIMEOUT,
            'socket_timeout': cls.REDIS_TIMEOUT,
            'retry_on_timeout': True
        }
    
    @classmethod
    def get_s3_config(cls):
        """Get S3/MinIO configuration"""
        return {
            'endpoint_url': cls.S3_ENDPOINT,
            'access_key': cls.S3_ACCESS_KEY,
            'secret_key': cls.S3_SECRET_KEY,
            'bucket_name': cls.S3_BUCKET_NAME,
            'use_ssl': cls.S3_USE_SSL,
            'region': cls.S3_REGION,
            'max_pool_connections': cls.S3_MAX_POOL_CONNECTIONS
        }

def get_stock_config(stock_name: str) -> Dict[str, Any]:
    """Get configuration for a specific stock"""
    for stock in STOCKS:
        if stock['name'] == stock_name:
            return stock
    return None

def get_source_config(source_name: str) -> Dict[str, Any]:
    """Get configuration for a specific news source"""
    for source in NEWS_SOURCES:
        if source['name'] == source_name:
            return source
    return None

def get_enabled_sources() -> List[Dict[str, Any]]:
    """Get list of enabled news sources"""
    return [source for source in NEWS_SOURCES if source['enabled']]

def get_stock_source_combinations() -> List[tuple]:
    """Get all stock-source combinations for parallel processing"""
    combinations = []
    enabled_sources = get_enabled_sources()
    
    for stock in STOCKS:
        for source in enabled_sources:
            combinations.append((stock['name'], source['name']))
    
    return combinations 