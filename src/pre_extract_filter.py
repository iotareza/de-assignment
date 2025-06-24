import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import json
import os
import hashlib
import math
from .connection_pool import get_postgres_connection, get_redis_client
from .config import STORAGE_CONFIG

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
        # k = (m/n) * ln(2) where m is size and n is expected number of elements
        # For a given error rate p, optimal k = -(ln(p) / ln(2))
        k = int(-(math.log(error_rate) / math.log(2)))
        # Ensure k is at least 1 and not more than size
        return max(1, min(k, size))
    
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

class PreExtractFilter:
    """PreExtract filter that filters URLs using database and persistent Redis bloom filter"""
    
    def __init__(self):
        # Initialize database schema
        self._init_database()
    
    def _init_database(self):
        """Initialize database connection and create schema if needed"""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    # Create URL tracking table if it doesn't exist
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS url_tracking (
                            id SERIAL PRIMARY KEY,
                            stock_name VARCHAR(100) NOT NULL,
                            url TEXT NOT NULL,
                            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            UNIQUE(stock_name, url)
                        )
                    """)
                    conn.commit()
                    logger.info("URL tracking table created/verified")
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
    
    def filter_urls(self, urls: List[str], stock_name: str) -> List[str]:
        """Filter URLs using database and persistent bloom filter for a specific stock"""
        logger.info(f"Filtering {len(urls)} URLs for stock: {stock_name}")
        
        # Get or create stock-specific bloom filter
        bloom_filter = self._get_or_create_bloom_filter(stock_name)
        
        # Filter URLs
        filtered_urls = []
        bloom_filter_updated = False
        
        for url in urls:
            # Create stock-specific key for bloom filter
            stock_url_key = f"{stock_name}:{url}"
            
            # Check database first (definitive check)
            if self._is_url_in_database(url, stock_name):
                logger.debug(f"URL found in database for {stock_name} (duplicate): {url}")
                # Add to bloom filter to avoid future database lookups
                bloom_filter.add(stock_url_key)
                bloom_filter_updated = True
                continue
            
            # Check bloom filter (fast check for potential duplicates)
            if bloom_filter.contains(stock_url_key):
                logger.debug(f"URL found in bloom filter for {stock_name} (possibly duplicate): {url}")
                continue
            
            # URL is new for this stock, add to filtered list
            filtered_urls.append(url)
            # Add to bloom filter for future checks
            bloom_filter.add(stock_url_key)
            bloom_filter_updated = True
        
        # Save updated bloom filter to Redis (persistent - no TTL)
        if bloom_filter_updated:
            self._save_bloom_filter_to_redis(bloom_filter, stock_name)
        
        logger.info(f"URL filtering completed for {stock_name}: {len(filtered_urls)} filtered from {len(urls)} total")
        return filtered_urls
    
    def _get_or_create_bloom_filter(self, stock_name: str) -> BloomFilter:
        """Get stock-specific bloom filter from Redis or create new one"""
        redis_client = get_redis_client()
        if not redis_client:
            logger.warning("Redis client not available, creating new bloom filter")
            return BloomFilter(
                STORAGE_CONFIG['bloom_filter_size'],
                STORAGE_CONFIG['bloom_filter_error_rate']
            )
        
        redis_key = f"bloom_filter:{stock_name}"
        
        try:
            # Try to get from Redis
            bloom_data = redis_client.get(redis_key)
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
    
    def _save_bloom_filter_to_redis(self, bloom_filter: BloomFilter, stock_name: str):
        """Save stock-specific bloom filter to Redis (persistent - no TTL)"""
        redis_client = get_redis_client()
        if not redis_client:
            logger.error("Redis client not available, cannot save bloom filter")
            return
        
        try:
            redis_key = f"bloom_filter:{stock_name}"
            bloom_data = json.dumps(bloom_filter.to_dict())
            redis_client.set(redis_key, bloom_data)  # No TTL - persistent
            logger.info(f"Saved bloom filter to Redis for {stock_name} (persistent)")
        except Exception as e:
            logger.error(f"Failed to save bloom filter to Redis for {stock_name}: {e}")
    
    def _is_url_in_database(self, url: str, stock_name: str) -> bool:
        """Check if URL exists in database for specific stock"""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT 1 FROM url_tracking WHERE stock_name = %s AND url = %s",
                        (stock_name, url)
                    )
                    return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"Error checking URL in database for {stock_name}: {e}")
            return False
    
    def mark_url_as_processed(self, url: str, stock_name: str):
        """Mark URL as processed in database for specific stock"""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "INSERT INTO url_tracking (stock_name, url) VALUES (%s, %s) ON CONFLICT (stock_name, url) DO NOTHING",
                        (stock_name, url)
                    )
                    conn.commit()
                    logger.debug(f"Marked URL as processed for {stock_name}: {url}")
        except Exception as e:
            logger.error(f"Error marking URL as processed for {stock_name}: {e}")

# Convenience function for easy access
def filter_urls(urls: List[str], stock_name: str) -> List[str]:
    """Filter URLs using pre-extract filter"""
    filter_instance = PreExtractFilter()
    return filter_instance.filter_urls(urls, stock_name)

if __name__ == "__main__":
    # Test the preExtract filter
    filter_obj = PreExtractFilter()
    
    # Test with sample URLs for different stocks
    test_urls = [
        "https://yourstory.com/article1",
        "https://finshots.in/article2",
        "https://yourstory.com/article3"
    ]
    
    # Test filtering for HDFC
    filtered_hdfc = filter_obj.filter_urls(test_urls, "HDFC")
    print(f"Filtered {len(filtered_hdfc)} URLs for HDFC from {len(test_urls)} total")
    
    # Test filtering for Tata Motors (same URLs, should still be available)
    filtered_tata = filter_obj.filter_urls(test_urls, "Tata Motors")
    print(f"Filtered {len(filtered_tata)} URLs for Tata Motors from {len(test_urls)} total") 