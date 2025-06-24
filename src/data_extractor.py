import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time
import random
import logging
from typing import List, Dict, Any
import urllib.parse
import hashlib
from abc import ABC, abstractmethod
from .pre_extract_filter import PreExtractFilter
from .s3_storage import S3Storage

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ListMaker(ABC):
    """Abstract base class for URL list makers"""
    
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
    
    @abstractmethod
    def get_relevant_urls(self, keyword: str) -> List[str]:
        """Get list of relevant URLs for the given keyword"""
        pass

class YourStoryListMaker(ListMaker):
    """List maker for YourStory website"""
    
    def get_relevant_urls(self, keyword: str) -> List[str]:
        """Get relevant URLs from YourStory for the given keyword"""
        urls = []
        
        # Approach 1: Try search functionality
        try:
            search_url = f"https://yourstory.com/search?q={urllib.parse.quote(keyword)}"
            logger.info(f"Searching YourStory for: {keyword}")
            response = self.session.get(search_url, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            urls = self._find_article_urls(soup)
            
        except Exception as e:
            logger.warning(f"Search approach failed for YourStory {keyword}: {str(e)}")
        
        # Approach 2: If search fails, try main page scraping
        if not urls:
            try:
                logger.info(f"Trying main page approach for YourStory: {keyword}")
                main_url = "https://yourstory.com"
                response = self.session.get(main_url, timeout=15)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.content, 'html.parser')
                urls = self._find_article_urls(soup)
                
            except Exception as e:
                logger.error(f"Main page approach also failed for YourStory {keyword}: {str(e)}")
        
        logger.info(f"Found {len(urls)} URLs from YourStory for {keyword}")
        return urls[:2]  # Limit to 10 URLs
    
    def _find_article_urls(self, soup: BeautifulSoup) -> List[str]:
        """Find article URLs from YourStory page content"""
        article_urls = []
        
        # Multiple selectors to try
        selectors = [
            'a[href*="/202"]',  # Articles with year in URL
            'a[href*="/story/"]',  # Story links
            'a[href*="/article/"]',  # Article links
            '.story-card a',  # Story card links
            '.article-card a',  # Article card links
            'h2 a',  # Headlines
            'h3 a',  # Sub-headlines
        ]
        
        for selector in selectors:
            links = soup.select(selector)
            for link in links:
                href = link.get('href', '')
                if href and not href.startswith('#'):
                    # Make URL absolute if relative
                    if href.startswith('/'):
                        href = f"https://yourstory.com{href}"
                    elif not href.startswith('http'):
                        href = f"https://yourstory.com/{href}"
                    
                    # Check if article might be relevant
                    if any(indicator in href.lower() for indicator in ['/202', '/story/', '/article/']):
                        article_urls.append(href)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_urls = []
        for url in article_urls:
            if url not in seen:
                seen.add(url)
                unique_urls.append(url)
        
        return unique_urls

class FinshotsListMaker(ListMaker):
    """List maker for Finshots website using their Ghost CMS API"""
    
    def get_relevant_urls(self, keyword: str) -> List[str]:
        """Get relevant URLs from Finshots for the given keyword using their API"""
        urls = []
        
        try:
            # Finshots Ghost CMS API
            api_url = "https://finshots.in/ghost/api/content/posts/?key=8067c49caa4ce48ca16b4c4445&limit=10000&fields=id%2Cslug%2Ctitle%2Cexcerpt%2Curl%2Cupdated_at%2Cvisibility&order=updated_at%20DESC"
            
            logger.info(f"Fetching Finshots articles via API for keyword: {keyword}")
            response = self.session.get(api_url, timeout=15)
            response.raise_for_status()
            
            data = response.json()
            posts = data.get('posts', [])
            
            logger.info(f"Found {len(posts)} total posts from Finshots API")
            
            # Filter posts that contain the keyword in title or excerpt
            keyword_lower = keyword.lower()
            for post in posts:
                title = post.get('title', '').lower()
                excerpt = post.get('excerpt', '').lower()
                
                if keyword_lower in title or keyword_lower in excerpt:
                    urls.append(post.get('url', ''))
            
            logger.info(f"Found {len(urls)} relevant URLs for keyword: {keyword}")
            
        except Exception as e:
            logger.error(f"Error fetching from Finshots API for {keyword}: {str(e)}")
        
        return urls[:2]  # Limit to 10 URLs

class GenericDataExtractor:
    """Generic data extractor that can extract content from any URL"""
    
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
    
    def extract_from_url(self, url: str, source: str, keyword: str) -> Dict[str, Any]:
        """Extract article content from a given URL"""
        try:
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract title
            title = self._extract_title(soup, source)
            
            # Extract content
            content = self._extract_content(soup, source)
            
            # Check if article is relevant to keyword
            if keyword.lower() in title.lower() or keyword.lower() in content.lower():
                # Generate content hash for unique identification
                content_hash = hashlib.md5(f"{title}{content}".encode()).hexdigest()
                
                # Store individual article content in S3
                s3_storage = S3Storage()
                
                # Create S3 key for individual article content
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                s3_content_key = f"article_content/{keyword}/{content_hash}_{timestamp}.json"
                
                # Store article content in S3
                article_content = {
                    'title': title,
                    'content': content,
                    'url': url,
                    'source': source,
                    'keyword': keyword,
                    'extraction_timestamp': datetime.now().isoformat()
                }
                s3_storage.save_data(article_content, s3_content_key)
                
                return {
                    'source': source,
                    'keyword': keyword,
                    'title': title,
                    'content': content[:2000] + "..." if len(content) > 2000 else content,  # Keep truncated content for metadata
                    'url': url,
                    'content_hash': content_hash,
                    's3_raw_content_path': s3_content_key,  # Raw content path
                    'extraction_timestamp': datetime.now().isoformat()
                }
            
        except Exception as e:
            logger.error(f"Error extracting article content from {url}: {str(e)}")
        
        return None
    
    def _extract_title(self, soup: BeautifulSoup, source: str) -> str:
        """Extract title from the page"""
        title_selectors = [
            'h1',
            '.story-title h1',
            '.article-title h1',
            '.post-title h1',
            '.entry-title h1',
            'title',
            '.headline h1',
            '[class*="title"] h1'
        ]
        
        for selector in title_selectors:
            title_elem = soup.select_one(selector)
            if title_elem:
                title = title_elem.get_text().strip()
                if title and title != "No title found":
                    return title
        
        return "No title found"
    
    def _extract_content(self, soup: BeautifulSoup, source: str) -> str:
        """Extract content from the page"""
        content_selectors = [
            '.story-content',
            '.article-content',
            '.post-content',
            '.entry-content',
            '.content',
            'article',
            '.story-body',
            '.article-body',
            '.post-body',
            '.entry-body',
            '[class*="content"]',
            '[class*="body"]'
        ]
        
        for selector in content_selectors:
            content_elem = soup.select_one(selector)
            if content_elem:
                # Remove script and style elements
                for script in content_elem(["script", "style"]):
                    script.decompose()
                content = content_elem.get_text().strip()
                if content and len(content) > 100:  # Ensure meaningful content
                    return content
        
        return "No content found"

class NewsExtractor:
    def __init__(self):
        # Initialize list makers for each source
        self.yourstory_list_maker = YourStoryListMaker()
        self.finshots_list_maker = FinshotsListMaker()
        
        # Initialize generic data extractor
        self.data_extractor = GenericDataExtractor()
        
        # Initialize pre-extract filter
        self.pre_extract_filter = PreExtractFilter()
        
        # self.s3_storage = S3Storage()
        
    def extract_news_data(self) -> Dict[str, Any]:
        """
        Main function to extract news data from both sources
        Returns: Dictionary containing extracted data and S3 key
        """
        logger.info("Starting news data extraction...")
        
        keywords = ['HDFC', 'Tata Motors']
        all_articles = []
        all_s3_paths = []
        
        for keyword in keywords:
            logger.info(f"Extracting articles for keyword: {keyword}")
            
            # Extract from YourStory
            yourstory_articles = self._extract_from_yourstory(keyword)
            print(yourstory_articles)
            all_articles.extend(yourstory_articles)
            
            # Extract from Finshots
            finshots_articles = self._extract_from_finshots(keyword)
            print(finshots_articles)
            all_articles.extend(finshots_articles)
            
            # Add delay to be respectful to the websites
            time.sleep(random.uniform(1, 3))
        
        # Collect S3 paths from all articles
        for article in all_articles:
            if article.get('s3_raw_content_path'):
                all_s3_paths.append(article['s3_raw_content_path'])
        
        # Save raw data (metadata + S3 paths) to S3
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        raw_data = {
            'articles': all_articles, 
            's3_content_paths': all_s3_paths,
            'extraction_timestamp': datetime.now().isoformat()
        }
        
        # Save raw data to S3
        s3_storage = S3Storage()
        s3_key = f"raw_data/raw_data_{timestamp}.json"
        s3_storage.save_data(raw_data, s3_key)
        
        logger.info(f"Extraction completed. Total articles found: {len(all_articles)}")
        logger.info(f"Total S3 content paths: {len(all_s3_paths)}")
        
        return {
            'articles': all_articles, 
            's3_content_paths': all_s3_paths,
            'extraction_timestamp': datetime.now().isoformat(),
            's3_raw_data_key': s3_key
        }
    
    def _extract_from_yourstory(self, keyword: str) -> List[Dict[str, Any]]:
        """Extract articles from YourStory using list maker and generic extractor"""
        articles = []
        
        # Get relevant URLs using list maker
        urls = self.yourstory_list_maker.get_relevant_urls(keyword)
        
        # Filter URLs using pre-extract filter for this stock
        filtered_urls = self.pre_extract_filter.filter_urls(urls, keyword)
        logger.info(f"YourStory: {len(filtered_urls)} URLs filtered from {len(urls)} total for {keyword}")
        
        # Extract content from each filtered URL using generic extractor
        for url in filtered_urls[:5]:  # Limit to 5 articles
            try:
                article_data = self.data_extractor.extract_from_url(url, 'YourStory', keyword)
                if article_data:
                    articles.append(article_data)
                    # Mark URL as processed for this stock
                    self.pre_extract_filter.mark_url_as_processed(url, keyword)
                time.sleep(random.uniform(1, 2))
            except Exception as e:
                logger.error(f"Error extracting article {url}: {str(e)}")
        
        logger.info(f"Found {len(articles)} articles from YourStory for {keyword}")
        return articles
    
    def _extract_from_finshots(self, keyword: str) -> List[Dict[str, Any]]:
        """Extract articles from Finshots using list maker and generic extractor"""
        articles = []
        
        # Get relevant URLs using list maker
        urls = self.finshots_list_maker.get_relevant_urls(keyword)
        
        # Filter URLs using pre-extract filter for this stock
        filtered_urls = self.pre_extract_filter.filter_urls(urls, keyword)
        logger.info(f"Finshots: {len(filtered_urls)} URLs filtered from {len(urls)} total for {keyword}")
        
        # Extract content from each filtered URL using generic extractor
        for url in filtered_urls[:5]:  # Limit to 5 articles
            try:
                article_data = self.data_extractor.extract_from_url(url, 'Finshots', keyword)
                if article_data:
                    articles.append(article_data)
                    # Mark URL as processed for this stock
                    self.pre_extract_filter.mark_url_as_processed(url, keyword)
                time.sleep(random.uniform(1, 2))
            except Exception as e:
                logger.error(f"Error extracting article {url}: {str(e)}")
        
        logger.info(f"Found {len(articles)} articles from Finshots for {keyword}")
        return articles

# Global function for Airflow
def extract_news_data(**context):
    """
    Main function called by Airflow to extract news data and save to S3
    """
    from datetime import datetime
    from .s3_storage import S3Storage
    
    extractor = NewsExtractor()
    result = extractor.extract_news_data()
    
    # The result already contains the S3 key from the extractor
    s3_key = result.get('s3_raw_data_key')
    
    # Pass S3 key to next task via XCom
    if context:
        context['task_instance'].xcom_push(key='extracted_data_s3_key', value=s3_key)
    
    return s3_key
