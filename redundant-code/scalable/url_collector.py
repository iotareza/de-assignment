import logging
from typing import List, Dict, Any
from datetime import datetime
import json
import os
from config import get_stock_config, get_enabled_sources, STORAGE_CONFIG
from data_extractor import YourStoryListMaker, FinshotsListMaker

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class URLCollector:
    """Collects all URLs for each stock from all sources without filtering"""
    
    def __init__(self):
        # Initialize list makers
        self.yourstory_list_maker = YourStoryListMaker()
        self.finshots_list_maker = FinshotsListMaker()
    
    def collect_stock_urls(self, stock_name: str) -> Dict[str, Any]:
        """Collect all URLs for a specific stock from all sources"""
        logger.info(f"Collecting URLs for stock: {stock_name}")
        
        # Get stock configuration
        stock_config = get_stock_config(stock_name)
        if not stock_config:
            logger.error(f"Invalid stock: {stock_name}")
            return {'urls': [], 'collection_timestamp': datetime.now().isoformat()}
        
        all_urls = []
        
        # Get all enabled sources
        enabled_sources = get_enabled_sources()
        
        # Collect URLs from each source for each keyword
        for source in enabled_sources:
            source_name = source['name']
            list_maker = self._get_list_maker(source_name)
            
            if not list_maker:
                logger.warning(f"No list maker found for source: {source_name}")
                continue
            
            for keyword in stock_config['keywords']:
                try:
                    logger.info(f"Collecting URLs for {stock_name} ({keyword}) from {source_name}")
                    urls = list_maker.get_relevant_urls(keyword)
                    
                    # Add metadata to each URL
                    for url in urls:
                        url_data = {
                            'url': url,
                            'stock_name': stock_name,
                            'keyword': keyword,
                            'source_name': source_name,
                            'collection_timestamp': datetime.now().isoformat()
                        }
                        all_urls.append(url_data)
                    
                    logger.info(f"Collected {len(urls)} URLs for {stock_name} ({keyword}) from {source_name}")
                    
                except Exception as e:
                    logger.error(f"Error collecting URLs for {stock_name} ({keyword}) from {source_name}: {str(e)}")
        
        result = {
            'stock_name': stock_name,
            'urls': all_urls,
            'collection_timestamp': datetime.now().isoformat(),
            'total_urls': len(all_urls)
        }
        
        # Save collection result
        self._save_collection_result(result, stock_name)
        
        logger.info(f"URL collection completed for {stock_name}: {len(all_urls)} URLs collected")
        return result
    
    def _get_list_maker(self, source_name: str):
        """Get the appropriate list maker for a source"""
        if source_name == 'YourStory':
            return self.yourstory_list_maker
        elif source_name == 'Finshots':
            return self.finshots_list_maker
        else:
            logger.warning(f"No list maker implemented for source: {source_name}")
            return None
    
    def _save_collection_result(self, result: Dict[str, Any], stock_name: str):
        """Save URL collection result to temporary file"""
        temp_dir = os.path.join(STORAGE_CONFIG['local_temp_dir'], 'url_collections')
        os.makedirs(temp_dir, exist_ok=True)
        
        filename = f"{stock_name.lower().replace(' ', '_')}_url_collection.json"
        filepath = os.path.join(temp_dir, filename)
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved URL collection result to {filepath}")
        except Exception as e:
            logger.error(f"Failed to save URL collection result: {e}")

# Global function for Airflow
def collect_stock_urls(stock_name: str) -> Dict[str, Any]:
    """
    Main function called by Airflow to collect URLs for a specific stock
    """
    collector = URLCollector()
    return collector.collect_stock_urls(stock_name)

if __name__ == "__main__":
    # Test the URL collector
    collector = URLCollector()
    
    # Test with HDFC
    result = collector.collect_stock_urls('HDFC')
    print(f"Collected {result['total_urls']} URLs for {result['stock_name']}") 