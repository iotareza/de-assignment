import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time
import random
import logging
from typing import List, Dict, Any
import json
import os
import urllib.parse
from abc import ABC, abstractmethod
from config import get_stock_config, get_source_config, STORAGE_CONFIG
from data_extractor import YourStoryListMaker, FinshotsListMaker, GenericDataExtractor

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ScalableDataExtractor:
    """Scalable data extractor without bloom filter (moved to preExtractor filter)"""
    
    def __init__(self):
        # Initialize list makers
        self.yourstory_list_maker = YourStoryListMaker()
        self.finshots_list_maker = FinshotsListMaker()
        self.generic_extractor = GenericDataExtractor()
        
    def extract_stock_source_data(self, stock_name: str, source_name: str) -> Dict[str, Any]:
        """Extract data for a specific stock from a specific source"""
        logger.info(f"Extracting data for {stock_name} from {source_name}")
        
        # Get configurations
        stock_config = get_stock_config(stock_name)
        source_config = get_source_config(source_name)
        
        if not stock_config or not source_config:
            logger.error(f"Invalid stock or source: {stock_name}, {source_name}")
            return {'articles': [], 'extraction_timestamp': datetime.now().isoformat()}
        
        # Get list maker for the source
        list_maker = self._get_list_maker(source_name)
        if not list_maker:
            logger.error(f"No list maker found for source: {source_name}")
            return {'articles': [], 'extraction_timestamp': datetime.now().isoformat()}
        
        articles = []
        
        # Extract articles for each keyword of the stock
        for keyword in stock_config['keywords']:
            try:
                # Get relevant URLs
                urls = list_maker.get_relevant_urls(keyword)
                logger.info(f"Found {len(urls)} URLs for {stock_name} ({keyword}) from {source_name}")
                
                # Extract content from URLs (no filtering here - handled by preExtractor filter)
                for url in urls[:source_config['max_articles_per_stock']]:
                    try:
                        article_data = self.generic_extractor.extract_from_url(url, source_name, keyword)
                        if article_data:
                            articles.append(article_data)
                        
                        # Respect rate limits
                        time.sleep(source_config['rate_limit'])
                        
                    except Exception as e:
                        logger.error(f"Error extracting article {url}: {str(e)}")
                
            except Exception as e:
                logger.error(f"Error extracting data for {stock_name} ({keyword}) from {source_name}: {str(e)}")
        
        result = {
            'stock_name': stock_name,
            'source_name': source_name,
            'articles': articles,
            'extraction_timestamp': datetime.now().isoformat(),
            'articles_count': len(articles)
        }
        
        # Save result to temp file for Airflow task communication
        self._save_temp_result(result, stock_name, source_name)
        
        logger.info(f"Extraction completed for {stock_name} from {source_name}: {len(articles)} articles")
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
    
    def _save_temp_result(self, result: Dict[str, Any], stock_name: str, source_name: str):
        """Save extraction result to temporary file"""
        temp_dir = os.path.join(STORAGE_CONFIG['local_temp_dir'], 'extractions')
        os.makedirs(temp_dir, exist_ok=True)
        
        filename = f"{stock_name.lower().replace(' ', '_')}_{source_name.lower()}_extraction.json"
        filepath = os.path.join(temp_dir, filename)
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved extraction result to {filepath}")
        except Exception as e:
            logger.error(f"Failed to save extraction result: {e}")

# Global function for Airflow
def extract_stock_source_data(stock_name: str, source_name: str) -> Dict[str, Any]:
    """
    Main function called by Airflow to extract data for a specific stock-source combination
    """
    extractor = ScalableDataExtractor()
    return extractor.extract_stock_source_data(stock_name, source_name)

if __name__ == "__main__":
    # Test the scalable extraction
    extractor = ScalableDataExtractor()
    
    # Test with HDFC and YourStory
    result = extractor.extract_stock_source_data('HDFC', 'YourStory')
    print(f"Extracted {len(result['articles'])} articles for HDFC from YourStory")
    
    # Test with HDFC and Finshots
    result = extractor.extract_stock_source_data('HDFC', 'Finshots')
    print(f"Extracted {len(result['articles'])} articles for HDFC from Finshots") 