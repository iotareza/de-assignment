"""
Connection Pool Manager for PostgreSQL, S3, and Redis
Centralized connection management with pooling
"""

import os
import logging
import redis
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool
from minio import Minio
import boto3
from botocore.config import Config
from typing import Optional, Dict, Any
from contextlib import contextmanager
import threading
from datetime import datetime
from .config import Config as AppConfig

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ConnectionPoolManager:
    """Centralized connection pool manager for all storage services"""
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        """Singleton pattern to ensure only one connection pool manager exists"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(ConnectionPoolManager, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize connection pools"""
        if hasattr(self, '_initialized'):
            return
        
        self._initialized = True
        self._postgres_pool = None
        self._redis_client = None
        self._s3_client = None
        self._minio_client = None
        
        # Load configuration from centralized config
        self.config = {
            'postgres': AppConfig.get_postgres_config(),
            'redis': AppConfig.get_redis_config(),
            's3': AppConfig.get_s3_config()
        }
        
        # Initialize connection pools
        self._initialize_postgres_pool()
        self._initialize_redis_client()
        self._initialize_s3_client()
        
        logger.info("Connection Pool Manager initialized successfully")
    
    def _initialize_postgres_pool(self):
        """Initialize PostgreSQL connection pool"""
        try:
            pg_config = self.config['postgres']
            
            # Create connection pool
            self._postgres_pool = SimpleConnectionPool(
                minconn=pg_config['min_connections'],
                maxconn=pg_config['max_connections'],
                host=pg_config['host'],
                port=pg_config['port'],
                database=pg_config['database'],
                user=pg_config['user'],
                password=pg_config['password'],
                cursor_factory=RealDictCursor
            )
            
            logger.info(f"PostgreSQL connection pool initialized: {pg_config['min_connections']}-{pg_config['max_connections']} connections")
            
        except Exception as e:
            logger.error(f"Failed to initialize PostgreSQL connection pool: {e}")
            self._postgres_pool = None
    
    def reinitialize_postgres_pool(self):
        """Reinitialize PostgreSQL connection pool if it was closed"""
        if self._postgres_pool is None or self._postgres_pool.closed:
            logger.info("Reinitializing PostgreSQL connection pool")
            self._initialize_postgres_pool()
        return self._postgres_pool is not None
    
    def _initialize_redis_client(self):
        """Initialize Redis client with connection pooling"""
        try:
            redis_config = self.config['redis']
            
            # Redis client handles connection pooling internally
            self._redis_client = redis.Redis(
                host=redis_config['host'],
                port=redis_config['port'],
                db=redis_config['db'],
                password=redis_config['password'],
                decode_responses=redis_config['decode_responses'],
                socket_connect_timeout=redis_config['socket_connect_timeout'],
                socket_timeout=redis_config['socket_timeout'],
                retry_on_timeout=redis_config['retry_on_timeout'],
                connection_pool=redis.ConnectionPool(
                    host=redis_config['host'],
                    port=redis_config['port'],
                    db=redis_config['db'],
                    password=redis_config['password'],
                    decode_responses=redis_config['decode_responses'],
                    max_connections=AppConfig.REDIS_MAX_CONNECTIONS
                )
            )
            
            # Test connection
            self._redis_client.ping()
            logger.info(f"Redis client initialized: {redis_config['host']}:{redis_config['port']}")
            
        except Exception as e:
            logger.error(f"Failed to initialize Redis client: {e}")
            self._redis_client = None
    
    def _initialize_s3_client(self):
        """Initialize S3/MinIO client"""
        try:
            s3_config = self.config['s3']
            
            # Determine if using MinIO or AWS S3
            if 'localhost' in s3_config['endpoint_url'] or 'minio' in s3_config['endpoint_url']:
                # MinIO client
                self._minio_client = Minio(
                    endpoint=s3_config['endpoint_url'].replace('http://', '').replace('https://', ''),
                    access_key=s3_config['access_key'],
                    secret_key=s3_config['secret_key'],
                    secure=s3_config['use_ssl']
                )
                
                # Test connection
                self._minio_client.list_buckets()
                logger.info(f"MinIO client initialized: {s3_config['endpoint_url']}")
                
            else:
                # AWS S3 client
                self._s3_client = boto3.client(
                    's3',
                    endpoint_url=f"http{'s' if s3_config['use_ssl'] else ''}://{s3_config['endpoint_url']}",
                    aws_access_key_id=s3_config['access_key'],
                    aws_secret_access_key=s3_config['secret_key'],
                    region_name=s3_config['region'],
                    config=Config(
                        max_pool_connections=s3_config['max_pool_connections'],
                        retries={'max_attempts': 3}
                    )
                )
                
                # Test connection
                self._s3_client.list_buckets()
                logger.info(f"S3 client initialized: {s3_config['endpoint_url']}")
                
        except Exception as e:
            logger.error(f"Failed to initialize S3/MinIO client: {e}")
            self._s3_client = None
            self._minio_client = None
    
    @contextmanager
    def get_postgres_connection(self):
        """Get PostgreSQL connection from pool with automatic cleanup"""
        conn = None
        try:
            if self._postgres_pool is None or self._postgres_pool.closed:
                # Try to reinitialize the pool
                if not self.reinitialize_postgres_pool():
                    raise Exception("PostgreSQL connection pool not initialized and could not be reinitialized")
            
            conn = self._postgres_pool.getconn()
            yield conn
            
        except Exception as e:
            logger.error(f"PostgreSQL connection error: {e}")
            raise
        finally:
            if conn:
                self._postgres_pool.putconn(conn)
    
    def get_redis_client(self) -> Optional[redis.Redis]:
        """Get Redis client"""
        if self._redis_client is None:
            logger.error("Redis client not initialized")
            return None
        return self._redis_client
    
    def get_s3_client(self):
        """Get S3/MinIO client"""
        if self._s3_client is not None:
            return self._s3_client
        elif self._minio_client is not None:
            return self._minio_client
        else:
            logger.error("S3/MinIO client not initialized")
            return None
    
    def get_config(self) -> Dict[str, Any]:
        """Get current configuration"""
        return self.config.copy()
    
    def test_connections(self) -> Dict[str, bool]:
        """Test all connections and return status"""
        status = {}
        
        # Test PostgreSQL
        try:
            with self.get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1 as test_value")
                    result = cursor.fetchone()
                    if result and result['test_value'] == 1:
                        status['postgres'] = True
                    else:
                        status['postgres'] = False
        except Exception as e:
            logger.error(f"PostgreSQL connection test failed: {e}")
            status['postgres'] = False
        
        # Test Redis
        try:
            redis_client = self.get_redis_client()
            if redis_client:
                redis_client.ping()
                status['redis'] = True
            else:
                status['redis'] = False
        except Exception as e:
            logger.error(f"Redis connection test failed: {e}")
            status['redis'] = False
        
        # Test S3/MinIO
        try:
            s3_client = self.get_s3_client()
            if s3_client:
                if hasattr(s3_client, 'list_buckets'):  # MinIO
                    s3_client.list_buckets()
                else:  # AWS S3
                    s3_client.list_buckets()
                status['s3'] = True
            else:
                status['s3'] = False
        except Exception as e:
            logger.error(f"S3/MinIO connection test failed: {e}")
            status['s3'] = False
        
        return status
    
    def close_all_connections(self):
        """Close all connections and cleanup resources"""
        try:
            # Close PostgreSQL pool
            if self._postgres_pool:
                self._postgres_pool.closeall()
                logger.info("PostgreSQL connection pool closed")
            
            # Close Redis client
            if self._redis_client:
                self._redis_client.close()
                logger.info("Redis client closed")
            
            # S3/MinIO clients don't need explicit closing
            
            logger.info("All connections closed successfully")
            
        except Exception as e:
            logger.error(f"Error closing connections: {e}")
    
    def get_connection_stats(self) -> Dict[str, Any]:
        """Get connection pool statistics"""
        stats = {
            'timestamp': datetime.now().isoformat(),
            'postgres': {},
            'redis': {},
            's3': {}
        }
        
        # PostgreSQL stats
        if self._postgres_pool:
            try:
                # Get pool size and usage
                pool_size = getattr(self._postgres_pool, '_maxconn', 0)
                used_connections = len(getattr(self._postgres_pool, '_used', set()))
                available_connections = pool_size - used_connections
                
                stats['postgres'] = {
                    'pool_size': pool_size,
                    'available_connections': available_connections,
                    'used_connections': used_connections
                }
            except Exception as e:
                logger.error(f"Error getting PostgreSQL stats: {e}")
                stats['postgres'] = {'status': 'unavailable'}
        
        # Redis stats
        if self._redis_client:
            try:
                info = self._redis_client.info()
                stats['redis'] = {
                    'connected_clients': info.get('connected_clients', 0),
                    'used_memory': info.get('used_memory_human', 'N/A'),
                    'total_commands_processed': info.get('total_commands_processed', 0)
                }
            except Exception as e:
                logger.error(f"Error getting Redis stats: {e}")
                stats['redis'] = {'status': 'unavailable'}
        
        # S3 stats
        if self._s3_client or self._minio_client:
            stats['s3'] = {
                'client_type': 'minio' if self._minio_client else 's3',
                'endpoint': self.config['s3']['endpoint_url']
            }
        
        return stats

# Global instance
connection_pool = ConnectionPoolManager()

# Convenience functions for easy access
def get_postgres_connection():
    """Get PostgreSQL connection from pool"""
    return connection_pool.get_postgres_connection()

def get_redis_client():
    """Get Redis client"""
    return connection_pool.get_redis_client()

def get_s3_client():
    """Get S3/MinIO client"""
    return connection_pool.get_s3_client()

def test_connections():
    """Test all connections"""
    return connection_pool.test_connections()

def get_connection_stats():
    """Get connection statistics"""
    return connection_pool.get_connection_stats()

def close_all_connections():
    """Close all connections"""
    connection_pool.close_all_connections()

def reinitialize_postgres_pool():
    """Reinitialize PostgreSQL connection pool if it was closed"""
    return connection_pool.reinitialize_postgres_pool() 