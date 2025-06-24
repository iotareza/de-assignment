import logging
from typing import List, Dict, Any
from datetime import datetime
import json
import os
import hashlib
import redis
import psycopg2
from psycopg2.extras import RealDictCursor
from config import STORAGE_CONFIG, DATABASE_CONFIG, STOCKS

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BloomFilter:
    """Bloom Filter implementation for URL deduplication"""
    
    def __init__(self, size: int, error_rate: float = 0.01):
        self.size = size
        self.error_rate = error_rate
        self.bit_array = [False] * size
        self.num_hashes = self._optimal_hash_count(size, error_rate)
        self.count = 0
    
    def _optimal_hash_count(self, size: int, error_rate: float) -> int:
        """Calculate optimal number of hash functions"""
        return int(-(size * error_rate) / (1 - error_rate))
    
    def _hash_functions(self, item: str) -> List[int]:
        """Generate multiple hash values for an item"""
        hashes = []
        for i in range(self.num_hashes):
            hash_value = hashlib.md5(f"{item}{i}".encode()).hexdigest()
            hashes.append(int(hash_value, 16) % self.size)
        return hashes
    
    def add(self, item: str) -> None:
        """Add an item to the bloom filter"""
        for hash_value in self._hash_functions(item):
            self.bit_array[hash_value] = True
        self.count += 1
    
    def contains(self, item: str) -> bool:
        """Check if an item might be in the bloom filter"""
        for hash_value in self._hash_functions(item):
            if not self.bit_array[hash_value]:
                return False
        return True
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert bloom filter to dictionary for Redis storage"""
        return {
            'size': self.size,
            'error_rate': self.error_rate,
            'bit_array': self.bit_array,
            'num_hashes': self.num_hashes,
            'count': self.count
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BloomFilter':
        """Create bloom filter from dictionary"""
        bloom_filter = cls(data['size'], data['error_rate'])
        bloom_filter.bit_array = data['bit_array']
        bloom_filter.num_hashes = data['num_hashes']
        bloom_filter.count = data['count']
        return bloom_filter

class PreExtractorFilter:
    """PreExtractor filter that filters URLs using database and Redis bloom filter"""
    
    def __init__(self):
        # Initialize Redis connection
        self.redis_client = redis.Redis(
            host=os.getenv('REDIS_HOST', 'localhost'),
            port=int(os.getenv('REDIS_PORT', 6379)),
            db=0,
            decode_responses=True
        )
        
        # Initialize database connection
        self.db_connection = None
        self._init_database()
    
    def _init_database(self):
        """Initialize database connection and create schema if needed"""
        try:
            self.db_connection = psycopg2.connect(
                host=DATABASE_CONFIG['host'],
                port=DATABASE_CONFIG['port'],
                database=DATABASE_CONFIG['database'],
                user=DATABASE_CONFIG['username'],
                password=DATABASE_CONFIG['password']
            )
            
            # Create URL tracking table if it doesn't exist
            self._create_url_tracking_table()
            
        except Exception as e:
            logger.error(f"Failed to initialize database connection: {e}")
    
    def _create_url_tracking_table(self):
        """Create URL tracking table in database"""
        try:
            with self.db_connection.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS url_tracking (
                        id SERIAL PRIMARY KEY,
                        stock_name VARCHAR(100) NOT NULL,
                        url TEXT NOT NULL,
                        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(stock_name, url)
                    )
                """)
                self.db_connection.commit()
                logger.info("URL tracking table created/verified")
        except Exception as e:
            logger.error(f"Failed to create URL tracking table: {e}")
    
    def filter_stock_urls(self, stock_name: str) -> Dict[str, Any]:
        """Filter URLs for a specific stock using database and bloom filter"""
        logger.info(f"Filtering URLs for stock: {stock_name}")
        
        # Load URL collection for this stock
        url_collection = self._load_url_collection(stock_name)
        
        if not url_collection or not url_collection.get('urls'):
            logger.warning(f"No URL collection found for stock: {stock_name}")
            return {
                'stock_name': stock_name,
                'filtered_urls': [],
                'filtering_timestamp': datetime.now().isoformat(),
                'total_urls': 0,
                'filtered_count': 0
            }
        
        urls = url_collection['urls']
        logger.info(f"Filtering {len(urls)} URLs for {stock_name}")
        
        # Get or create bloom filter for this stock
        bloom_filter = self._get_or_create_bloom_filter(stock_name)
        
        # Filter URLs
        filtered_urls = []
        bloom_filter_updated = False
        
        for url_data in urls:
            url = url_data['url']
            
            # Check bloom filter first (fast)
            if bloom_filter.contains(url):
                logger.debug(f"URL found in bloom filter (possibly duplicate): {url}")
                continue
            
            # Check database (slower but definitive)
            if self._is_url_in_database(stock_name, url):
                logger.debug(f"URL found in database (duplicate): {url}")
                # Add to bloom filter to avoid future database lookups
                bloom_filter.add(url)
                bloom_filter_updated = True
                continue
            
            # URL is new, add to filtered list
            filtered_urls.append(url_data)
            # Add to bloom filter
            bloom_filter.add(url)
            bloom_filter_updated = True
        
        # Save updated bloom filter to Redis
        if bloom_filter_updated:
            self._save_bloom_filter_to_redis(stock_name, bloom_filter)
        
        # Cache filtered URLs in Redis
        self._cache_urls_in_redis(stock_name, filtered_urls)
        
        result = {
            'stock_name': stock_name,
            'filtered_urls': filtered_urls,
            'filtering_timestamp': datetime.now().isoformat(),
            'total_urls': len(urls),
            'filtered_count': len(filtered_urls),
            'duplicates_removed': len(urls) - len(filtered_urls)
        }
        
        # Save filtering result
        self._save_filtering_result(result, stock_name)
        
        logger.info(f"URL filtering completed for {stock_name}: {len(filtered_urls)} filtered from {len(urls)} total")
        return result
    
    def _load_url_collection(self, stock_name: str) -> Dict[str, Any]:
        """Load URL collection for a specific stock"""
        collection_dir = os.path.join(STORAGE_CONFIG['local_temp_dir'], 'url_collections')
        filename = f"{stock_name.lower().replace(' ', '_')}_url_collection.json"
        filepath = os.path.join(collection_dir, filename)
        
        if os.path.exists(filepath):
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Error loading URL collection from {filepath}: {e}")
        
        return {}
    
    def _get_or_create_bloom_filter(self, stock_name: str) -> BloomFilter:
        """Get bloom filter from Redis or create new one"""
        redis_key = f"bloom_filter:{stock_name}"
        
        try:
            # Try to get from Redis
            bloom_data = self.redis_client.get(redis_key)
            if bloom_data:
                bloom_dict = json.loads(bloom_data)
                logger.info(f"Loaded bloom filter from Redis for {stock_name}")
                return BloomFilter.from_dict(bloom_dict)
        except Exception as e:
            logger.warning(f"Failed to load bloom filter from Redis for {stock_name}: {e}")
        
        # Create new bloom filter
        bloom_filter = BloomFilter(
            STORAGE_CONFIG['bloom_filter_size'],
            STORAGE_CONFIG['bloom_filter_error_rate']
        )
        logger.info(f"Created new bloom filter for {stock_name}")
        return bloom_filter
    
    def _save_bloom_filter_to_redis(self, stock_name: str, bloom_filter: BloomFilter):
        """Save bloom filter to Redis"""
        try:
            redis_key = f"bloom_filter:{stock_name}"
            bloom_data = json.dumps(bloom_filter.to_dict())
            self.redis_client.setex(redis_key, 86400, bloom_data)  # 24 hours TTL
            logger.info(f"Saved bloom filter to Redis for {stock_name}")
        except Exception as e:
            logger.error(f"Failed to save bloom filter to Redis for {stock_name}: {e}")
    
    def _is_url_in_database(self, stock_name: str, url: str) -> bool:
        """Check if URL exists in database"""
        if not self.db_connection:
            logger.warning("Database connection not available, skipping database check")
            return False
        
        try:
            with self.db_connection.cursor() as cursor:
                cursor.execute(
                    "SELECT 1 FROM url_tracking WHERE stock_name = %s AND url = %s",
                    (stock_name, url)
                )
                return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"Error checking URL in database: {e}")
            return False
    
    def _cache_urls_in_redis(self, stock_name: str, urls: List[Dict[str, Any]]):
        """Cache filtered URLs in Redis"""
        try:
            redis_key = f"filtered_urls:{stock_name}"
            urls_data = json.dumps(urls)
            self.redis_client.setex(redis_key, 3600, urls_data)  # 1 hour TTL
            logger.info(f"Cached {len(urls)} filtered URLs in Redis for {stock_name}")
        except Exception as e:
            logger.error(f"Failed to cache URLs in Redis for {stock_name}: {e}")
    
    def _save_filtering_result(self, result: Dict[str, Any], stock_name: str):
        """Save filtering result to temporary file"""
        temp_dir = os.path.join(STORAGE_CONFIG['local_temp_dir'], 'filtered_urls')
        os.makedirs(temp_dir, exist_ok=True)
        
        filename = f"{stock_name.lower().replace(' ', '_')}_filtered_urls.json"
        filepath = os.path.join(temp_dir, filename)
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved filtering result to {filepath}")
        except Exception as e:
            logger.error(f"Failed to save filtering result: {e}")
    
    def mark_url_as_processed(self, stock_name: str, url: str):
        """Mark URL as processed in database"""
        if not self.db_connection:
            logger.warning("Database connection not available, cannot mark URL as processed")
            return
        
        try:
            with self.db_connection.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO url_tracking (stock_name, url) VALUES (%s, %s) ON CONFLICT (stock_name, url) DO NOTHING",
                    (stock_name, url)
                )
                self.db_connection.commit()
                logger.debug(f"Marked URL as processed: {stock_name} - {url}")
        except Exception as e:
            logger.error(f"Error marking URL as processed: {e}")

# Global function for Airflow
def filter_stock_urls(stock_name: str) -> Dict[str, Any]:
    """
    Main function called by Airflow to filter URLs for a specific stock
    """
    filter_obj = PreExtractorFilter()
    return filter_obj.filter_stock_urls(stock_name)

if __name__ == "__main__":
    # Test the preExtractor filter
    filter_obj = PreExtractorFilter()
    
    # Test with HDFC
    result = filter_obj.filter_stock_urls('HDFC')
    print(f"Filtered {result['filtered_count']} URLs from {result['total_urls']} total for {result['stock_name']}") 