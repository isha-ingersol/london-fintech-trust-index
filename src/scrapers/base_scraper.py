"""
Base scraper class that provides common functionality for all data source scrapers
Includes rate limiting, error handling, and standard data structures
"""

import requests
import time
import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
from datetime import datetime
import json
import os
from bs4 import BeautifulSoup

# Import our configuration
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from config.config import HEADERS, DATA_SOURCES, RAW_DATA_DIR

class BaseScraper(ABC):
    """Base class for all data source scrapers"""
    
    def __init__(self, source_name: str):
        self.source_name = source_name
        self.config = DATA_SOURCES.get(source_name, {})
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        
        # Setup logging
        self.logger = logging.getLogger(f"scraper.{source_name}")
        logging.basicConfig(level=logging.INFO)
        
        # Rate limiting
        self.rate_limit = self.config.get('rate_limit', 1)
        self.last_request_time = 0
        
        # Data storage
        self.scraped_data = []
        self.metadata = {
            'source': source_name,
            'scraped_at': datetime.now().isoformat(),
            'total_records': 0,
            'errors': []
        }
    
    def _rate_limit_delay(self):
        """Ensure we don't exceed rate limits"""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit:
            time.sleep(self.rate_limit - elapsed)
        self.last_request_time = time.time()
    
    def _make_request(self, url: str, method: str = 'GET', **kwargs) -> Optional[requests.Response]:
        """Make a rate-limited HTTP request with error handling"""
        self._rate_limit_delay()
        
        try:
            self.logger.info(f"Making {method} request to: {url}")
            response = self.session.request(method, url, timeout=30, **kwargs)
            response.raise_for_status()
            return response
        
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request failed for {url}: {str(e)}")
            self.metadata['errors'].append({
                'url': url,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            })
            return None
    
    def _parse_html(self, html_content: str) -> BeautifulSoup:
        """Parse HTML content using BeautifulSoup"""
        return BeautifulSoup(html_content, 'html.parser')
    
    def _extract_metadata_from_page(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """Extract common metadata from any webpage"""
        metadata = {
            'url': url,
            'title': '',
            'description': '',
            'last_modified': '',
            'has_privacy_policy': False,
            'has_terms_of_service': False,
            'meta_tags_count': 0
        }
        
        # Extract title
        title_tag = soup.find('title')
        if title_tag:
            metadata['title'] = title_tag.get_text().strip()
        
        # Extract meta description
        desc_tag = soup.find('meta', attrs={'name': 'description'})
        if desc_tag:
            metadata['description'] = desc_tag.get('content', '').strip()
        
        # Check for privacy policy and terms links
        links = soup.find_all('a', href=True)
        for link in links:
            href = link['href'].lower()
            text = link.get_text().lower()
            
            if 'privacy' in href or 'privacy' in text:
                metadata['has_privacy_policy'] = True
            if 'terms' in href or 'terms' in text:
                metadata['has_terms_of_service'] = True
        
        # Count meta tags
        metadata['meta_tags_count'] = len(soup.find_all('meta'))
        
        return metadata
    
    def save_data(self, filename: str = None) -> str:
        """Save scraped data to JSON file"""
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"{self.source_name}_{timestamp}.json"
        
        filepath = os.path.join(RAW_DATA_DIR, filename)
        
        # Update metadata
        self.metadata['total_records'] = len(self.scraped_data)
        self.metadata['saved_at'] = datetime.now().isoformat()
        
        # Prepare final data structure
        output_data = {
            'metadata': self.metadata,
            'data': self.scraped_data
        }
        
        # Save to file
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"Data saved to: {filepath}")
        self.logger.info(f"Total records scraped: {self.metadata['total_records']}")
        
        return filepath
    
    @abstractmethod
    def scrape(self) -> List[Dict[str, Any]]:
        """Main scraping method - must be implemented by subclasses"""
        pass
    
    @abstractmethod
    def validate_data(self, record: Dict[str, Any]) -> bool:
        """Validate a single record - must be implemented by subclasses"""
        pass
    
    def get_summary(self) -> Dict[str, Any]:
        """Get summary statistics of scraped data"""
        return {
            'source': self.source_name,
            'total_records': len(self.scraped_data),
            'scraping_errors': len(self.metadata['errors']),
            'last_scraped': self.metadata.get('scraped_at'),
            'data_quality_score': self._calculate_basic_quality_score()
        }
    
    def _calculate_basic_quality_score(self) -> float:
        """Calculate a basic quality score based on completeness"""
        if not self.scraped_data:
            return 0.0
        
        total_fields = 0
        filled_fields = 0
        
        for record in self.scraped_data:
            for key, value in record.items():
                total_fields += 1
                if value is not None and str(value).strip() != '':
                    filled_fields += 1
        
        return filled_fields / total_fields if total_fields > 0 else 0.0