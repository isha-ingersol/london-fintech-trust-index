"""
Seedrs equity crowdfunding scraper
Collects campaign data from Seedrs platform for London-based fintech companies
"""

import re
from typing import Dict, List, Any
from urllib.parse import urljoin, urlparse
from .base_scraper import BaseScraper

class SeedrsScaper(BaseScraper):
    """Scraper for Seedrs equity crowdfunding platform"""
    
    def __init__(self):
        super().__init__('seedrs')
        self.base_url = self.config['base_url']
        self.campaigns_url = self.config['campaigns_url']
        
        # Fintech-related keywords to identify relevant campaigns
        self.fintech_keywords = [
            'fintech', 'financial', 'banking', 'payment', 'trading', 'investment',
            'cryptocurrency', 'blockchain', 'lending', 'insurance', 'wealth',
            'credit', 'api', 'digital bank', 'neobank', 'paytech'
        ]
    
    def scrape(self) -> List[Dict[str, Any]]:
        """Main scraping method for Seedrs campaigns"""
        self.logger.info("Starting Seedrs campaign scraping...")
        
        # Get the main campaigns page
        response = self._make_request(self.campaigns_url)
        if not response:
            return []
        
        soup = self._parse_html(response.text)
        
        # Extract campaign links (this is a simplified approach)
        # In a real scenario, you might need to handle pagination and more complex scraping
        campaign_links = self._extract_campaign_links(soup)
        
        self.logger.info(f"Found {len(campaign_links)} campaign links")
        
        # Scrape each campaign (limit to first 10 for demo purposes)
        for i, link in enumerate(campaign_links[:10]):
            self.logger.info(f"Scraping campaign {i+1}/{min(10, len(campaign_links))}: {link}")
            campaign_data = self._scrape_campaign_detail(link)
            
            if campaign_data and self._is_fintech_related(campaign_data):
                if self.validate_data(campaign_data):
                    self.scraped_data.append(campaign_data)
        
        return self.scraped_data
    
    def _extract_campaign_links(self, soup) -> List[str]:
        """Extract campaign detail page links from the campaigns listing page"""
        links = []
        
        # Look for campaign cards/links (this selector may need adjustment based on actual HTML)
        # Since we can't see the actual Seedrs HTML structure, this is a generic approach
        for link in soup.find_all('a', href=True):
            href = link['href']
            # Look for campaign-like URLs
            if '/campaigns/' in href or '/startups/' in href:
                full_url = urljoin(self.base_url, href)
                if full_url not in links:
                    links.append(full_url)
        
        return links
    
    def _scrape_campaign_detail(self, url: str) -> Dict[str, Any]:
        """Scrape detailed information from a single campaign page"""
        response = self._make_request(url)
        if not response:
            return {}
        
        soup = self._parse_html(response.text)
        
        # Extract campaign data
        campaign_data = {
            'url': url,
            'company_name': self._extract_text_by_selector(soup, 'h1, .company-name, .startup-name'),
            'tagline': self._extract_text_by_selector(soup, '.tagline, .description, .pitch'),
            'sector': self._extract_sector(soup),
            'location': self._extract_location(soup),
            'funding_target': self._extract_funding_info(soup, 'target'),
            'funding_raised': self._extract_funding_info(soup, 'raised'),
            'investors_count': self._extract_investors_count(soup),
            'campaign_status': self._extract_status(soup),
            'founded_year': self._extract_founded_year(soup),
            'employees': self._extract_employees_count(soup),
            'last_funding_round': self._extract_last_funding(soup),
            
            # Metadata for trust scoring
            'page_metadata': self._extract_metadata_from_page(soup, url),
            'data_completeness_fields': 0,  # Will be calculated in validation
            'has_financial_statements': self._has_financial_statements(soup),
            'has_business_plan': self._has_business_plan(soup),
            'regulatory_mentions': self._count_regulatory_mentions(soup),
            
            # Scraped timestamp
            'scraped_at': self.metadata['scraped_at']
        }
        
        return campaign_data
    
    def _extract_text_by_selector(self, soup, selector: str) -> str:
        """Extract text using CSS selector with fallback"""
        for sel in selector.split(', '):
            element = soup.select_one(sel.strip())
            if element:
                return element.get_text().strip()
        return ''
    
    def _extract_sector(self, soup) -> str:
        """Extract company sector/industry"""
        # Look for sector indicators
        for selector in ['.sector', '.industry', '.category', '.tags']:
            element = soup.select_one(selector)
            if element:
                return element.get_text().strip()
        
        # Fallback: look in meta tags
        meta_keywords = soup.find('meta', attrs={'name': 'keywords'})
        if meta_keywords:
            return meta_keywords.get('content', '')
        
        return ''
    
    def _extract_location(self, soup) -> str:
        """Extract company location"""
        # Look for location indicators
        location_patterns = ['london', 'uk', 'united kingdom', 'england']
        
        text_content = soup.get_text().lower()
        for pattern in location_patterns:
            if pattern in text_content:
                # Try to extract more specific location
                sentences = text_content.split('.')
                for sentence in sentences:
                    if pattern in sentence and len(sentence) < 200:
                        return sentence.strip().title()
        
        return 'London, UK'  # Default assumption for this demo
    
    def _extract_funding_info(self, soup, info_type: str) -> str:
        """Extract funding target or raised amount"""
        # Look for funding information
        text = soup.get_text()
        
        if info_type == 'target':
            patterns = [r'target[:\s]*£([\d,]+)', r'seeking[:\s]*£([\d,]+)']
        else:  # raised
            patterns = [r'raised[:\s]*£([\d,]+)', r'invested[:\s]*£([\d,]+)']
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return f"£{match.group(1)}"
        
        return ''
    
    def _extract_investors_count(self, soup) -> str:
        """Extract number of investors"""
        text = soup.get_text()
        pattern = r'(\d+)\s*investors?'
        match = re.search(pattern, text, re.IGNORECASE)
        return match.group(1) if match else ''
    
    def _extract_status(self, soup) -> str:
        """Extract campaign status"""
        status_indicators = soup.find_all(['span', 'div', 'p'], class_=re.compile(r'status|state'))
        for indicator in status_indicators:
            text = indicator.get_text().strip().lower()
            if any(word in text for word in ['live', 'active', 'closed', 'successful', 'ended']):
                return text.title()
        return 'Unknown'
    
    def _extract_founded_year(self, soup) -> str:
        """Extract company founding year"""
        text = soup.get_text()
        patterns = [r'founded[:\s]*(\d{4})', r'established[:\s]*(\d{4})', r'since[:\s]*(\d{4})']
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1)
        return ''
    
    def _extract_employees_count(self, soup) -> str:
        """Extract employee count"""
        text = soup.get_text()
        patterns = [r'(\d+)[+\s]*employees?', r'team[:\s]*(\d+)', r'staff[:\s]*(\d+)']
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1)
        return ''
    
    def _extract_last_funding(self, soup) -> str:
        """Extract information about last funding round"""
        text = soup.get_text()
        funding_rounds = ['series a', 'series b', 'seed', 'pre-seed', 'angel']
        
        for round_type in funding_rounds:
            if round_type in text.lower():
                return round_type.title()
        return ''
    
    def _has_financial_statements(self, soup) -> bool:
        """Check if financial statements are mentioned or linked"""
        text = soup.get_text().lower()
        financial_terms = ['financial statements', 'accounts', 'revenue', 'profit', 'loss', 'balance sheet']
        return any(term in text for term in financial_terms)
    
    def _has_business_plan(self, soup) -> bool:
        """Check if business plan is mentioned"""
        text = soup.get_text().lower()
        plan_terms = ['business plan', 'strategy', 'roadmap', 'projections', 'forecast']
        return any(term in text for term in plan_terms)
    
    def _count_regulatory_mentions(self, soup) -> int:
        """Count mentions of regulatory terms"""
        text = soup.get_text().lower()
        regulatory_terms = ['fca', 'regulation', 'compliance', 'licensed', 'authorized', 'gdpr', 'data protection']
        return sum(1 for term in regulatory_terms if term in text)
    
    def _is_fintech_related(self, campaign_data: Dict[str, Any]) -> bool:
        """Check if campaign is fintech-related based on keywords"""
        text_fields = [
            campaign_data.get('company_name', ''),
            campaign_data.get('tagline', ''),
            campaign_data.get('sector', ''),
            campaign_data.get('page_metadata', {}).get('description', '')
        ]
        
        combined_text = ' '.join(text_fields).lower()
        
        return any(keyword in combined_text for keyword in self.fintech_keywords)
    
    def validate_data(self, record: Dict[str, Any]) -> bool:
        """Validate a campaign record"""
        required_fields = ['company_name', 'url']
        
        # Check required fields
        for field in required_fields:
            if not record.get(field):
                self.logger.warning(f"Missing required field: {field}")
                return False
        
        # Calculate completeness score
        total_fields = len(record)
        filled_fields = sum(1 for v in record.values() if v and str(v).strip())
        record['data_completeness_fields'] = filled_fields / total_fields
        
        # Must have minimum data quality
        if record['data_completeness_fields'] < 0.3:  # 30% minimum
            self.logger.warning(f"Data too incomplete: {record['data_completeness_fields']}")
            return False
        
        return True