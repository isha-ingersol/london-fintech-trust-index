"""
Metadata Auditor for London Fintech Trust Index
Analyzes and scores metadata quality of scraped data sources
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import re
import json
from urllib.parse import urlparse


class MetadataAuditor:
    """
    Audits metadata quality of fintech data sources
    Evaluates completeness, freshness, and structure of metadata
    """
    
    def __init__(self):
        """Initialize the metadata auditor with scoring weights"""
        self.scoring_weights = {
            'completeness': 0.3,
            'freshness': 0.25,
            'structure': 0.2,
            'accessibility': 0.15,
            'documentation': 0.1
        }
        
    def audit_metadata(self, data_dict: Dict[str, Any]) -> Dict[str, Any]:
        """
        Main function to audit all metadata aspects of a data source
        Returns comprehensive metadata quality scores and analysis
        """
        audit_results = {
            'source_name': data_dict.get('source_name', 'Unknown'),
            'audit_timestamp': datetime.now().isoformat(),
            'scores': {},
            'details': {},
            'overall_score': 0.0,
            'grade': 'F'
        }
        
        # Run individual audit components
        audit_results['scores']['completeness'] = self._audit_completeness(data_dict)
        audit_results['scores']['freshness'] = self._audit_freshness(data_dict)
        audit_results['scores']['structure'] = self._audit_structure(data_dict)
        audit_results['scores']['accessibility'] = self._audit_accessibility(data_dict)
        audit_results['scores']['documentation'] = self._audit_documentation(data_dict)
        
        # Calculate overall weighted score
        audit_results['overall_score'] = self._calculate_weighted_score(audit_results['scores'])
        audit_results['grade'] = self._assign_grade(audit_results['overall_score'])
        
        # Generate detailed analysis
        audit_results['details'] = self._generate_audit_details(data_dict, audit_results['scores'])
        
        return audit_results
    
    def _audit_completeness(self, data_dict: Dict[str, Any]) -> float:
        """
        Evaluate metadata completeness by checking presence of essential fields
        Returns score 0-100 based on how many required fields are present
        """
        required_fields = [
            'source_name', 'url', 'data_type', 'last_updated',
            'description', 'provider', 'data_format'
        ]
        
        optional_fields = [
            'contact_info', 'license', 'update_frequency',
            'data_size', 'api_version', 'rate_limits'
        ]
        
        # Check required fields (70% of score)
        required_present = sum(1 for field in required_fields if data_dict.get(field))
        required_score = (required_present / len(required_fields)) * 70
        
        # Check optional fields (30% of score)
        optional_present = sum(1 for field in optional_fields if data_dict.get(field))
        optional_score = (optional_present / len(optional_fields)) * 30
        
        return min(100.0, required_score + optional_score)
    
    def _audit_freshness(self, data_dict: Dict[str, Any]) -> float:
        """
        Evaluate data freshness based on last update timestamp
        More recent data gets higher scores with exponential decay
        """
        last_updated = data_dict.get('last_updated')
        if not last_updated:
            return 0.0
        
        try:
            # Parse various date formats
            if isinstance(last_updated, str):
                # Try common date formats
                for fmt in ['%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S']:
                    try:
                        last_update_date = datetime.strptime(last_updated, fmt)
                        break
                    except ValueError:
                        continue
                else:
                    return 0.0
            else:
                last_update_date = last_updated
            
            # Calculate days since last update
            days_old = (datetime.now() - last_update_date).days
            
            # Scoring logic: 100 for today, exponential decay
            if days_old <= 1:
                return 100.0
            elif days_old <= 7:
                return 90.0
            elif days_old <= 30:
                return 75.0
            elif days_old <= 90:
                return 50.0
            elif days_old <= 365:
                return 25.0
            else:
                return 10.0
                
        except Exception:
            return 0.0
    
    def _audit_structure(self, data_dict: Dict[str, Any]) -> float:
        """
        Evaluate data structure quality including format consistency and validation
        Checks for proper data types, naming conventions, and structure
        """
        score = 0.0
        max_score = 100.0
        
        # Check URL validity (20 points)
        url = data_dict.get('url', '')
        if self._is_valid_url(url):
            score += 20
        
        # Check data format specification (20 points)
        data_format = data_dict.get('data_format', '')
        if data_format and data_format.lower() in ['json', 'csv', 'xml', 'api', 'html']:
            score += 20
        
        # Check naming conventions (15 points)
        source_name = data_dict.get('source_name', '')
        if source_name and len(source_name) > 3 and not source_name.isdigit():
            score += 15
        
        # Check description quality (15 points)
        description = data_dict.get('description', '')
        if description and len(description.split()) >= 5:
            score += 15
        
        # Check provider information (15 points)
        provider = data_dict.get('provider', '')
        if provider and len(provider) > 2:
            score += 15
        
        # Check for nested structure quality (15 points)
        if isinstance(data_dict.get('metadata'), dict):
            score += 15
        
        return min(max_score, score)
    
    def _audit_accessibility(self, data_dict: Dict[str, Any]) -> float:
        """
        Evaluate how accessible the data source is (API availability, rate limits, auth)
        Higher scores for public APIs with clear access patterns
        """
        score = 0.0
        
        # Check if URL is accessible (40 points)
        url = data_dict.get('url', '')
        if url and self._is_valid_url(url):
            score += 40
        
        # Check for API documentation (30 points)
        if data_dict.get('api_documentation') or data_dict.get('data_format') == 'api':
            score += 30
        
        # Check rate limiting information (20 points)
        if data_dict.get('rate_limits') or data_dict.get('rate_limit_info'):
            score += 20
        
        # Check authentication requirements (10 points - higher score for public access)
        auth_required = data_dict.get('authentication_required', True)
        if not auth_required:
            score += 10
        
        return min(100.0, score)
    
    def _audit_documentation(self, data_dict: Dict[str, Any]) -> float:
        """
        Evaluate quality and completeness of documentation
        Checks for contact info, license, usage examples, and help resources
        """
        score = 0.0
        
        # Contact information available (25 points)
        if data_dict.get('contact_info') or data_dict.get('support_email'):
            score += 25
        
        # License information (25 points)
        if data_dict.get('license') or data_dict.get('terms_of_use'):
            score += 25
        
        # Update frequency specified (20 points)
        if data_dict.get('update_frequency'):
            score += 20
        
        # Usage examples or documentation links (20 points)
        if data_dict.get('examples') or data_dict.get('documentation_url'):
            score += 20
        
        # Data schema or field descriptions (10 points)
        if data_dict.get('schema') or data_dict.get('field_descriptions'):
            score += 10
        
        return min(100.0, score)
    
    def _calculate_weighted_score(self, scores: Dict[str, float]) -> float:
        """
        Calculate overall weighted metadata quality score
        Combines individual scores using predefined weights
        """
        weighted_sum = 0.0
        total_weight = 0.0
        
        for category, score in scores.items():
            if category in self.scoring_weights:
                weight = self.scoring_weights[category]
                weighted_sum += score * weight
                total_weight += weight
        
        return weighted_sum / total_weight if total_weight > 0 else 0.0
    
    def _assign_grade(self, score: float) -> str:
        """
        Convert numerical score to letter grade
        Uses standard grading scale A-F
        """
        if score >= 90:
            return 'A'
        elif score >= 80:
            return 'B'
        elif score >= 70:
            return 'C'
        elif score >= 60:
            return 'D'
        else:
            return 'F'
    
    def _is_valid_url(self, url: str) -> bool:
        """
        Validate URL format using urllib.parse
        Returns True if URL has valid scheme and netloc
        """
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except Exception:
            return False
    
    def _generate_audit_details(self, data_dict: Dict[str, Any], scores: Dict[str, float]) -> Dict[str, Any]:
        """
        Generate detailed audit findings and recommendations
        Provides actionable insights for improving metadata quality
        """
        details = {
            'strengths': [],
            'weaknesses': [],
            'recommendations': [],
            'missing_fields': [],
            'quality_indicators': {}
        }
        
        # Identify strengths
        for category, score in scores.items():
            if score >= 80:
                details['strengths'].append(f"Strong {category} (Score: {score:.1f})")
        
        # Identify weaknesses and recommendations
        if scores.get('completeness', 0) < 70:
            details['weaknesses'].append("Incomplete metadata")
            details['recommendations'].append("Add missing required fields")
        
        if scores.get('freshness', 0) < 60:
            details['weaknesses'].append("Outdated information")
            details['recommendations'].append("Update data more frequently")
        
        if scores.get('documentation', 0) < 50:
            details['weaknesses'].append("Poor documentation")
            details['recommendations'].append("Improve documentation and contact information")
        
        # Identify missing fields
        required_fields = ['source_name', 'url', 'data_type', 'last_updated', 'description', 'provider']
        for field in required_fields:
            if not data_dict.get(field):
                details['missing_fields'].append(field)
        
        # Quality indicators
        details['quality_indicators'] = {
            'has_contact_info': bool(data_dict.get('contact_info')),
            'has_license': bool(data_dict.get('license')),
            'has_api_docs': bool(data_dict.get('api_documentation')),
            'recently_updated': scores.get('freshness', 0) > 70
        }
        
        return details
    
    def batch_audit(self, data_sources: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """
        Audit multiple data sources in batch
        Returns dictionary mapping source names to audit results
        """
        results = {}
        
        for source_data in data_sources:
            source_name = source_data.get('source_name', f'source_{len(results)}')
            results[source_name] = self.audit_metadata(source_data)
        
        return results
    
    def generate_summary_report(self, audit_results: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """
        Generate summary statistics across all audited sources
        Provides overview of metadata quality across the entire dataset
        """
        if not audit_results:
            return {'error': 'No audit results provided'}
        
        scores = [result['overall_score'] for result in audit_results.values()]
        grades = [result['grade'] for result in audit_results.values()]
        
        summary = {
            'total_sources': len(audit_results),
            'average_score': np.mean(scores),
            'median_score': np.median(scores),
            'min_score': min(scores),
            'max_score': max(scores),
            'grade_distribution': {grade: grades.count(grade) for grade in set(grades)},
            'sources_above_threshold': sum(1 for score in scores if score >= 70),
            'improvement_needed': sum(1 for score in scores if score < 60),
            'top_performers': [
                name for name, result in audit_results.items() 
                if result['overall_score'] >= 85
            ],
            'needs_attention': [
                name for name, result in audit_results.items() 
                if result['overall_score'] < 60
            ]
        }
        
        return summary