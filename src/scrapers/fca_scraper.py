"""
FCA (Financial Conduct Authority) data scraper
Collects regulatory data about London fintech firms from FCA registers
"""

import re
import json
from typing import Dict, List, Any
from urllib.parse import urljoin
from .base_scraper import BaseScraper

class FcaScraper(BaseScraper):
    """Scraper for FCA Financial Services Register"""
    
    def __init__(self):
        super().__init__('fca')
        self.base_url = self.config['base_url']
        self.api_url = self.config.get('api_url', '')
        
        # London postcodes and areas to filter for London-based firms
        self.london_postcodes = [
            'EC1', 'EC2', 'EC3', 'EC4', 'WC1', 'WC2', 'E1', 'E14', 'SW1', 'W1', 'N1'
        ]
        
        # Fintech-related business types and activities
        self.fintech_activities = [
            'payment services', 'electronic money', 'consumer credit', 'deposit taking',
            'investment services', 'insurance', 'peer-to-peer', 'crowdfunding',
            'digital wallet', 'cryptocurrency', 'blockchain', 'robo-advisor'
        ]
    
    def scrape(self) -> List[Dict[str, Any]]:
        """Main scraping method for FCA data"""
        self.logger.info("Starting FCA register scraping...")
        
        # For this demo, we'll scrape the public search interface
        # In a production system, you might use the FCA's data download service
        
        # Search for fintech-related firms in London
        search_terms = ['payment', 'electronic money', 'fintech', 'digital', 'peer-to-peer']
        
        for term in search_terms:
            self.logger.info(f"Searching FCA register for: {term}")
            firms = self._search_fca_register(term)
            
            for firm in firms:
                if self._is_london_based(firm) and self._is_fintech_related(firm):
                    detailed_firm = self._get_firm_details(firm)
                    if detailed_firm and self.validate_data(detailed_firm):
                        self.scraped_data.append(detailed_firm)
        
        # Remove duplicates based on FRN (Firm Reference Number)
        self._remove_duplicates()
        
        return self.scraped_data
    
    def _search_fca_register(self, search_term: str) -> List[Dict[str, Any]]:
        """Search the FCA register for firms matching a term"""
        search_url = f"{self.base_url}/search"
        
        # Simulate search request (in reality, this might be a POST request with form data)
        params = {
            'name': search_term,
            'status': 'Authorised'
        }
        
        response = self._make_request(search_url, params=params)
        if not response:
            return []
        
        soup = self._parse_html(response.text)
        firms = []
        
        # Extract firm listings from search results
        # This is a simplified approach - actual FCA site structure may differ
        firm_rows = soup.find_all(['tr', 'div'], class_=re.compile(r'firm|result|row'))
        
        for row in firm_rows:
            firm_data = self._extract_firm_from_row(row)
            if firm_data:
                firms.append(firm_data)
        
        return firms
    
    def _extract_firm_from_row(self, row_element) -> Dict[str, Any]:
        """Extract basic firm information from a search result row"""
        # Look for firm reference number (FRN)
        frn_pattern = r'(\d{6,8})'
        frn_match = re.search(frn_pattern, row_element.get_text())
        
        if not frn_match:
            return {}
        
        # Extract basic information
        text = row_element.get_text()
        links = row_element.find_all('a', href=True)
        
        firm_data = {
            'frn': frn_match.group(1),
            'name': '',
            'status': '',
            'address': '',
            'detail_url': ''
        }
        
        # Extract firm name (usually the first link or largest text)
        if links:
            firm_data['name'] = links[0].get_text().strip()
            firm_data['detail_url'] = urljoin(self.base_url, links[0]['href'])
        
        # Extract status
        if 'authorised' in text.lower():
            firm_data['status'] = 'Authorised'
        elif 'cancelled' in text.lower():
            firm_data['status'] = 'Cancelled'
        
        # Extract address (simplified)
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        for line in lines:
            if any(postcode in line.upper() for postcode in self.london_postcodes):
                firm_data['address'] = line
                break
        
        return firm_data
    
    def _get_firm_details(self, firm: Dict[str, Any]) -> Dict[str, Any]:
        """Get detailed information about a specific firm"""
        if not firm.get('detail_url'):
            return firm
        
        response = self._make_request(firm['detail_url'])
        if not response:
            return firm
        
        soup = self._parse_html(response.text)
        
        # Enhanced firm data with detailed information
        detailed_firm = {
            **firm,  # Include basic info
            'activities': self._extract_activities(soup),
            'permissions': self._extract_permissions(soup),
            'appointed_representatives': self._extract_appointed_reps(soup),
            'contact_details': self._extract_contact_details(soup),
            'incorporation_date': self._extract_incorporation_date(soup),
            'last_updated': self._extract_last_updated(soup),
            'regulatory_status': self._extract_regulatory_status(soup),
            'complaints_handling': self._has_complaints_handling(soup),
            'client_money_rules': self._has_client_money_rules(soup),
            'regulatory_notices': self._count_regulatory_notices(soup),
            
            # Metadata for trust scoring
            'page_metadata': self._extract_metadata_from_page(soup, firm['detail_url']),
            'data_completeness_score': 0,  # Will be calculated
            'regulatory_compliance_indicators': self._assess_compliance_indicators(soup),
            'transparency_score': self._assess_transparency(soup),
            
            'scraped_at': self.metadata['scraped_at']
        }
        
        return detailed_firm
    
    def _extract_activities(self, soup) -> List[str]:
        """Extract regulated activities"""
        activities = []
        
        # Look for activities section
        activities_section = soup.find(['div', 'section'], 
                                     text=re.compile(r'activities?', re.I))
        
        if activities_section:
            # Find the parent container
            container = activities_section.find_parent()
            if container:
                activity_items = container.find_all(['li', 'p', 'div'])
                for item in activity_items:
                    text = item.get_text().strip()
                    if text and len(text) > 10:  # Filter out short/empty items
                        activities.append(text)
        
        return activities
    
    def _extract_permissions(self, soup) -> List[str]:
        """Extract regulatory permissions"""
        permissions = []
        
        # Look for permissions section
        text = soup.get_text()
        permission_patterns = [
            r'permission to (.*?)(?:\n|\.)',
            r'authorised to (.*?)(?:\n|\.)',
            r'may (.*?)(?:\n|\.)'
        ]
        
        for pattern in permission_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            permissions.extend(matches)
        
        return [p.strip() for p in permissions if len(p.strip()) > 5]
    
    def _extract_appointed_reps(self, soup) -> int:
        """Count appointed representatives"""
        text = soup.get_text()
        ar_pattern = r'(\d+)\s*appointed representatives?'
        match = re.search(ar_pattern, text, re.IGNORECASE)
        return int(match.group(1)) if match else 0
    
    def _extract_contact_details(self, soup) -> Dict[str, str]:
        """Extract contact information"""
        contact = {
            'phone': '',
            'email': '',
            'website': ''
        }
        
        text = soup.get_text()
        
        # Phone number
        phone_pattern = r'(\+?44\s?[\d\s]{10,}|\b0[\d\s]{10,})'
        phone_match = re.search(phone_pattern, text)
        if phone_match:
            contact['phone'] = phone_match.group(1).strip()
        
        # Email
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        email_match = re.search(email_pattern, text)
        if email_match:
            contact['email'] = email_match.group()
        
        # Website
        website_links = soup.find_all('a', href=re.compile(r'http'))
        for link in website_links:
            href = link['href']
            if 'fca.org.uk' not in href:  # Exclude FCA's own links
                contact['website'] = href
                break
        
        return contact
    
    def _extract_incorporation_date(self, soup) -> str:
        """Extract incorporation date"""
        text = soup.get_text()
        date_patterns = [
            r'incorporated[:\s]*(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{4})',
            r'established[:\s]*(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{4})',
            r'(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{4})'
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1)
        return ''
    
    def _extract_last_updated(self, soup) -> str:
        """Extract when the record was last updated"""
        text = soup.get_text()
        update_pattern = r'last updated[:\s]*(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{4})'
        match = re.search(update_pattern, text, re.IGNORECASE)
        return match.group(1) if match else ''
    
    def _extract_regulatory_status(self, soup) -> str:
        """Extract detailed regulatory status"""
        status_indicators = soup.find_all(text=re.compile(r'status|condition|restriction'))
        
        for indicator in status_indicators:
            parent = indicator.parent
            if parent:
                text = parent.get_text().strip()
                if len(text) < 200:  # Avoid very long text
                    return text
        
        return 'Active'  # Default
    
    def _has_complaints_handling(self, soup) -> bool:
        """Check if firm has complaints handling procedures"""
        text = soup.get_text().lower()
        complaints_terms = ['complaints', 'dispute resolution', 'ombudsman', 'grievance']
        return any(term in text for term in complaints_terms)
    
    def _has_client_money_rules(self, soup) -> bool:
        """Check if firm follows client money protection rules"""
        text = soup.get_text().lower()
        client_money_terms = ['client money', 'client assets', 'segregation', 'protection']
        return any(term in text for term in client_money_terms)
    
    def _count_regulatory_notices(self, soup) -> int:
        """Count regulatory notices or warnings"""
        text = soup.get_text().lower()
        notice_terms = ['notice', 'warning', 'enforcement', 'fine', 'penalty', 'censure']
        return sum(1 for term in notice_terms if term in text)
    
    def _assess_compliance_indicators(self, soup) -> float:
        """Assess compliance indicators (0-1 score)"""
        indicators = [
            self._has_complaints_handling(soup),
            self._has_client_money_rules(soup),
            bool(self._extract_permissions(soup)),
            self._count_regulatory_notices(soup) == 0,  # No notices is good
            'authorised' in soup.get_text().lower()
        ]
        
        return sum(indicators) / len(indicators)
    
    def _assess_transparency(self, soup) -> float:
        """Assess transparency score based on information availability"""
        transparency_factors = [
            bool(self._extract_contact_details(soup)['phone']),
            bool(self._extract_contact_details(soup)['email']),
            bool(self._extract_contact_details(soup)['website']),
            bool(self._extract_last_updated(soup)),
            len(self._extract_activities(soup)) > 0,
            soup.find('meta', attrs={'name': 'description'}) is not None
        ]
        
        return sum(transparency_factors) / len(transparency_factors)
    
    def _is_london_based(self, firm: Dict[str, Any]) -> bool:
        """Check if firm is London-based"""
        address = firm.get('address', '').upper()
        return any(postcode in address for postcode in self.london_postcodes)
    
    def _is_fintech_related(self, firm: Dict[str, Any]) -> bool:
        """Check if firm is fintech-related"""
        searchable_text = ' '.join([
            firm.get('name', ''),
            firm.get('address', ''),
            ' '.join(firm.get('activities', []))
        ]).lower()
        
        return any(activity in searchable_text for activity in self.fintech_activities)
    
    def _remove_duplicates(self):
        """Remove duplicate firms based on FRN"""
        seen_frns = set()
        unique_firms = []
        
        for firm in self.scraped_data:
            frn = firm.get('frn')
            if frn and frn not in seen_frns:
                seen_frns.add(frn)
                unique_firms.append(firm)
        
        self.scraped_data = unique_firms
        self.logger.info(f"Removed {len(self.scraped_data) - len(unique_firms)} duplicates")
    
    def validate_data(self, record: Dict[str, Any]) -> bool:
        """Validate an FCA firm record"""
        required_fields = ['frn', 'name', 'status']
        
        # Check required fields
        for field in required_fields:
            if not record.get(field):
                self.logger.warning(f"Missing required field: {field}")
                return False
        
        # Validate FRN format (should be 6-8 digits)
        frn = record.get('frn', '')
        if not re.match(r'^\d{6,8}$', str(frn)):
            self.logger.warning(f"Invalid FRN format: {frn}")
            return False
        
        # Calculate completeness score
        total_fields = len(record)
        filled_fields = sum(1 for v in record.values() if v and str(v).strip())
        record['data_completeness_score'] = filled_fields / total_fields
        
        # Must have minimum completeness
        if record['data_completeness_score'] < 0.4:  # 40% minimum
            return False
        
        return True