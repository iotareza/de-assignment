import logging
from typing import List, Dict, Any
from datetime import datetime
import json
import os
import time
from config import STORAGE_CONFIG, get_source_config
from data_extractor import GenericDataExtractor
from pre_extractor_filter import PreExtractorFilter

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ContentExtractor:
    """Content extractor that extracts content from filtered URLs"""
    
    def __init__(self):
        self.generic_extractor = GenericDataExtractor()
        self.pre_extractor_filter = PreExtractorFilter()
    
    def extract_stock_content(self, stock_name: str) -> Dict[str, Any]:
        """Extract content from filtered URLs for a specific stock"""
        logger.info(f"Extracting content for stock: {stock_name}")
        
        # Load filtered URLs for this stock
        filtered_urls = self._load_filtered_urls(stock_name)
        
        if not filtered_urls or not filtered_urls.get('filtered_urls'):
            logger.warning(f"No filtered URLs found for stock: {stock_name}")
            return {
                'stock_name': stock_name,
                'articles': [],
                'extraction_timestamp': datetime.now().isoformat(),
                'total_urls': 0,
                'extracted_count': 0
            }
        
        url_data_list = filtered_urls['filtered_urls']
        logger.info(f"Extracting content from {len(url_data_list)} URLs for {stock_name}")
        
        articles = []
        successful_extractions = 0
        
        for url_data in url_data_list:
            try:
                url = url_data['url']
                source_name = url_data['source_name']
                keyword = url_data['keyword']
                
                # Get source configuration for rate limiting
                source_config = get_source_config(source_name)
                rate_limit = source_config.get('rate_limit', 1) if source_config else 1
                
                # Extract content
                article_data = self.generic_extractor.extract_from_url(url, source_name, keyword)
                
                if article_data:
                    # Add stock name to article data
                    article_data['stock_name'] = stock_name
                    articles.append(article_data)
                    successful_extractions += 1
                    
                    # Mark URL as processed in database
                    self.pre_extractor_filter.mark_url_as_processed(stock_name, url)
                    
                    logger.debug(f"Successfully extracted content from: {url}")
                else:
                    logger.warning(f"Failed to extract content from: {url}")
                
                # Respect rate limits
                time.sleep(rate_limit)
                
            except Exception as e:
                logger.error(f"Error extracting content from {url_data.get('url', 'unknown')}: {str(e)}")
        
        result = {
            'stock_name': stock_name,
            'articles': articles,
            'extraction_timestamp': datetime.now().isoformat(),
            'total_urls': len(url_data_list),
            'extracted_count': successful_extractions,
            'failed_count': len(url_data_list) - successful_extractions
        }
        
        # Save extraction result
        self._save_extraction_result(result, stock_name)
        
        logger.info(f"Content extraction completed for {stock_name}: {successful_extractions} articles extracted from {len(url_data_list)} URLs")
        return result
    
    def _load_filtered_urls(self, stock_name: str) -> Dict[str, Any]:
        """Load filtered URLs for a specific stock"""
        filtered_dir = os.path.join(STORAGE_CONFIG['local_temp_dir'], 'filtered_urls')
        filename = f"{stock_name.lower().replace(' ', '_')}_filtered_urls.json"
        filepath = os.path.join(filtered_dir, filename)
        
        if os.path.exists(filepath):
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Error loading filtered URLs from {filepath}: {e}")
        
        return {}
    
    def _save_extraction_result(self, result: Dict[str, Any], stock_name: str):
        """Save extraction result to temporary file"""
        temp_dir = os.path.join(STORAGE_CONFIG['local_temp_dir'], 'extractions')
        os.makedirs(temp_dir, exist_ok=True)
        
        filename = f"{stock_name.lower().replace(' ', '_')}_extraction.json"
        filepath = os.path.join(temp_dir, filename)
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved extraction result to {filepath}")
        except Exception as e:
            logger.error(f"Failed to save extraction result: {e}")

# Global function for Airflow
def extract_stock_content(stock_name: str) -> Dict[str, Any]:
    """
    Main function called by Airflow to extract content for a specific stock
    """
    extractor = ContentExtractor()
    return extractor.extract_stock_content(stock_name)

if __name__ == "__main__":
    # Test the content extractor
    extractor = ContentExtractor()
    
    # Test with HDFC
    result = extractor.extract_stock_content('HDFC')
    print(f"Extracted {result['extracted_count']} articles from {result['total_urls']} URLs for {result['stock_name']}") 