"""
Trust Index Scoring Engine for London Fintech Data Trust Index
Calculates comprehensive trustworthiness scores for data sources
"""

import logging
import json
import os
from typing import Dict, List, Any, Tuple
from datetime import datetime
import math

# Import configuration
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from config.config import SCORING_WEIGHTS, QUALITY_THRESHOLDS, PROCESSED_DATA_DIR

class TrustIndexScorer:
    """Trust Index scoring engine for fintech data sources"""
    
    def __init__(self):
        self.logger = logging.getLogger("trust_scorer")
        logging.basicConfig(level=logging.INFO)
        
        # Scoring components weights
        self.weights = SCORING_WEIGHTS
        
        # Trust scores storage
        self.trust_scores = {}
        self.detailed_scores = {}
        
        # Scoring criteria definitions
        self.scoring_criteria = {
            'data_completeness': {
                'excellent': 0.95,
                'good': 0.80,
                'fair': 0.60,
                'poor': 0.40
            },
            'metadata_quality': {
                'excellent': 0.90,
                'good': 0.70,
                'fair': 0.50,
                'poor': 0.30
            },
            'regulatory_compliance': {
                'excellent': 0.95,
                'good': 0.75,
                'fair': 0.55,
                'poor': 0.35
            },
            'transparency': {
                'excellent': 0.90,
                'good': 0.70,
                'fair': 0.50,
                'poor': 0.30
            },
            'accessibility': {
                'excellent': 0.95,
                'good': 0.75,
                'fair': 0.55,
                'poor': 0.35
            }
        }
    
    def calculate_trust_score(self, source_name: str, audit_result: Dict[str, Any], 
                            raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate comprehensive trust score for a data source"""
        self.logger.info(f"Calculating trust score for {source_name}")
        
        # Extract scoring inputs
        scoring_inputs = self._extract_scoring_inputs(audit_result, raw_data)
        
        # Calculate individual dimension scores
        dimension_scores = {
            'data_completeness': self._score_data_completeness(scoring_inputs),
            'metadata_quality': self._score_metadata_quality(scoring_inputs),
            'regulatory_compliance': self._score_regulatory_compliance(scoring_inputs),
            'transparency': self._score_transparency(scoring_inputs),
            'accessibility': self._score_accessibility(scoring_inputs)
        }
        
        # Calculate weighted overall trust score
        overall_score = sum(
            dimension_scores[dimension] * self.weights[dimension]
            for dimension in dimension_scores.keys()
        )
        
        # Create detailed scoring result
        trust_result = {
            'source': source_name,
            'calculated_at': datetime.now().isoformat(),
            'overall_trust_score': round(overall_score, 3),
            'trust_grade': self._get_trust_grade(overall_score),
            'dimension_scores': dimension_scores,
            'dimension_weights': self.weights,
            'scoring_details': self._create_scoring_details(scoring_inputs, dimension_scores),
            'recommendations': self._generate_trust_recommendations(dimension_scores),
            'confidence_level': self._calculate_confidence_level(scoring_inputs),
            'score_components': self._break_down_score_components(dimension_scores)
        }
        
        # Store results
        self.trust_scores[source_name] = trust_result
        
        # Save detailed report
        self._save_trust_report(trust_result, source_name)
        
        return trust_result
    
    def _extract_scoring_inputs(self, audit_result: Dict[str, Any], 
                              raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract relevant inputs for trust scoring"""
        data_records = raw_data.get('data', [])
        metadata = raw_data.get('metadata', {})
        
        # Aggregate data for scoring
        scoring_inputs = {
            # Data completeness inputs
            'overall_completeness': audit_result.get('completeness', {}).get('overall_completeness', 0),
            'field_completeness': audit_result.get('completeness', {}).get('field_completeness', {}),
            'missing_cells': audit_result.get('completeness', {}).get('missing_cells', 0),
            
            # Data quality inputs
            'validity_score': audit_result.get('validity', {}).get('validity_score', 0),
            'consistency_score': audit_result.get('consistency', {}).get('consistency_score', 0),
            'uniqueness_score': audit_result.get('uniqueness', {}).get('uniqueness_score', 0),
            
            # Metadata inputs
            'metadata_completeness': audit_result.get('metadata_quality', {}).get('metadata_completeness', 0),
            'has_error_tracking': audit_result.get('metadata_quality', {}).get('has_error_tracking', False),
            'count_accuracy': audit_result.get('metadata_quality', {}).get('count_accuracy', 0),
            
            # Timeliness inputs
            'timeliness_score': audit_result.get('timeliness', {}).get('timeliness_score', 0),
            'age_days': audit_result.get('timeliness', {}).get('age_days', 999),
            'is_recent': audit_result.get('timeliness', {}).get('is_recent', False),
            
            # Source-specific inputs
            'total_records': audit_result.get('total_records', 0),
            'scraping_errors': len(metadata.get('errors', [])),
            'source_type': metadata.get('source', ''),
            
            # Regulatory and compliance indicators
            'regulatory_mentions': self._count_regulatory_mentions(data_records),
            'privacy_indicators': self._count_privacy_indicators(data_records),
            'compliance_indicators': self._assess_compliance_indicators(data_records),
            
            # Transparency indicators
            'update_frequency_indicators': self._assess_update_frequency(data_records, metadata),
            'documentation_quality': self._assess_documentation_quality(data_records),
            'contact_information_availability': self._assess_contact_availability(data_records),
            
            # Accessibility indicators
            'api_quality_indicators': self._assess_api_quality(data_records),
            'data_format_quality': self._assess_data_format_quality(audit_result),
            'ease_of_access': self._assess_ease_of_access(metadata)
        }
        
        return scoring_inputs
    
    def _score_data_completeness(self, inputs: Dict[str, Any]) -> float:
        """Score data completeness dimension"""
        base_score = inputs.get('overall_completeness', 0)
        
        # Adjust based on field-level completeness
        field_completeness = inputs.get('field_completeness', {})
        if field_completeness:
            # Calculate standard deviation of field completeness
            completeness_values = [field['non_empty_ratio'] for field in field_completeness.values()]
            if completeness_values:
                avg_completeness = sum(completeness_values) / len(completeness_values)
                std_dev = math.sqrt(sum((x - avg_completeness)**2 for x in completeness_values) / len(completeness_values))
                
                # Penalize high variation in completeness across fields
                consistency_penalty = min(0.1, std_dev)
                base_score = max(0, base_score - consistency_penalty)
        
        # Bonus for very high completeness
        if base_score >= 0.95:
            base_score = min(1.0, base_score + 0.05)
        
        return round(base_score, 3)
    
    def _score_metadata_quality(self, inputs: Dict[str, Any]) -> float:
        """Score metadata quality dimension"""
        metadata_completeness = inputs.get('metadata_completeness', 0)
        has_error_tracking = inputs.get('has_error_tracking', False)
        count_accuracy = inputs.get('count_accuracy', 0)
        
        # Base score from metadata completeness
        base_score = metadata_completeness * 0.6
        
        # Add points for error tracking
        if has_error_tracking:
            base_score += 0.2
        
        # Add points for count accuracy
        base_score += count_accuracy * 0.2
        
        # Bonus for documentation quality
        doc_quality = inputs.get('documentation_quality', 0)
        base_score += doc_quality * 0.1
        
        return round(min(1.0, base_score), 3)
    
    def _score_regulatory_compliance(self, inputs: Dict[str, Any]) -> float:
        """Score regulatory compliance dimension"""
        regulatory_mentions = inputs.get('regulatory_mentions', 0)
        privacy_indicators = inputs.get('privacy_indicators', 0)
        compliance_indicators = inputs.get('compliance_indicators', 0)
        
        # Base score from compliance indicators
        base_score = min(1.0, compliance_indicators)
        
        # Add points for regulatory mentions (capped)
        regulatory_score = min(0.3, regulatory_mentions * 0.05)
        base_score += regulatory_score
        
        # Add points for privacy indicators (capped)
        privacy_score = min(0.2, privacy_indicators * 0.1)
        base_score += privacy_score
        
        # Bonus for FCA-regulated sources
        if inputs.get('source_type') == 'fca':
            base_score += 0.2
        
        return round(min(1.0, base_score), 3)
    
    def _score_transparency(self, inputs: Dict[str, Any]) -> float:
        """Score transparency dimension"""
        timeliness_score = inputs.get('timeliness_score', 0)
        update_freq = inputs.get('update_frequency_indicators', 0)
        contact_availability = inputs.get('contact_information_availability', 0)
        
        # Base score from timeliness
        base_score = timeliness_score * 0.5
        
        # Add points for update frequency indicators
        base_score += update_freq * 0.3
        
        # Add points for contact information availability
        base_score += contact_availability * 0.2
        
        # Penalty for scraping errors (indicates poor transparency)
        error_count = inputs.get('scraping_errors', 0)
        if error_count > 0:
            error_penalty = min(0.2, error_count * 0.05)
            base_score = max(0, base_score - error_penalty)
        
        return round(base_score, 3)
    
    def _score_accessibility(self, inputs: Dict[str, Any]) -> float:
        """Score accessibility dimension"""
        api_quality = inputs.get('api_quality_indicators', 0)
        data_format_quality = inputs.get('data_format_quality', 0)
        ease_of_access = inputs.get('ease_of_access', 0)
        
        # Weighted combination of accessibility factors
        base_score = (api_quality * 0.4 + 
                     data_format_quality * 0.3 + 
                     ease_of_access * 0.3)
        
        # Bonus for high data quality scores (easier to use)
        validity_score = inputs.get('validity_score', 0)
        consistency_score = inputs.get('consistency_score', 0)
        quality_bonus = (validity_score + consistency_score) * 0.1
        
        base_score += quality_bonus
        
        return round(min(1.0, base_score), 3)
    
    def _count_regulatory_mentions(self, data_records: List[Dict[str, Any]]) -> int:
        """Count regulatory mentions across all records"""
        regulatory_terms = ['fca', 'regulation', 'compliance', 'licensed', 'authorized', 
                           'gdpr', 'data protection', 'regulatory', 'conduct authority']
        
        count = 0
        for record in data_records:
            record_text = ' '.join(str(v).lower() for v in record.values() if v)
            count += sum(1 for term in regulatory_terms if term in record_text)
        
        return count
    
    def _count_privacy_indicators(self, data_records: List[Dict[str, Any]]) -> int:
        """Count privacy-related indicators"""
        privacy_terms = ['privacy policy', 'privacy', 'data protection', 'gdpr', 
                        'cookie policy', 'terms of service', 'data processing']
        
        count = 0
        for record in data_records:
            # Check page metadata for privacy indicators
            page_metadata = record.get('page_metadata', {})
            if page_metadata.get('has_privacy_policy', False):
                count += 2
            if page_metadata.get('has_terms_of_service', False):
                count += 1
            
            # Check text content
            record_text = ' '.join(str(v).lower() for v in record.values() if v)
            count += sum(1 for term in privacy_terms if term in record_text)
        
        return count
    
    def _assess_compliance_indicators(self, data_records: List[Dict[str, Any]]) -> float:
        """Assess overall compliance indicators (0-1 score)"""
        if not data_records:
            return 0.0
        
        compliance_indicators = []
        
        for record in data_records:
            record_score = 0
            
            # Check for FCA registration (if applicable)
            if record.get('frn'):  # Has FCA Firm Reference Number
                record_score += 0.3
            
            # Check for regulatory status
            status = str(record.get('status', '')).lower()
            if 'authorised' in status or 'licensed' in status:
                record_score += 0.2
            
            # Check for compliance mentions
            compliance_mentions = record.get('regulatory_mentions', 0)
            if compliance_mentions > 0:
                record_score += min(0.2, compliance_mentions * 0.05)
            
            # Check for financial statements availability
            if record.get('has_financial_statements', False):
                record_score += 0.1
            
            # Check for business plan availability
            if record.get('has_business_plan', False):
                record_score += 0.1
            
            # Check for contact details (transparency indicator)
            contact_details = record.get('contact_details', {})
            if isinstance(contact_details, dict):
                if contact_details.get('email') or contact_details.get('phone'):
                    record_score += 0.1
            
            compliance_indicators.append(min(1.0, record_score))
        
        return sum(compliance_indicators) / len(compliance_indicators)
    
    def _assess_update_frequency(self, data_records: List[Dict[str, Any]], 
                               metadata: Dict[str, Any]) -> float:
        """Assess update frequency indicators (0-1 score)"""
        # Check if data appears to be regularly updated
        indicators = []
        
        # Check metadata for update information
        if metadata.get('scraped_at'):
            indicators.append(0.3)  # Has timestamp
        
        # Check for last updated information in records
        update_fields = ['last_updated', 'updated_at', 'last_modified']
        has_update_info = any(
            any(field in str(record.keys()).lower() for field in update_fields)
            for record in data_records
        )
        
        if has_update_info:
            indicators.append(0.4)
        
        # Check for version information
        has_version_info = any(
            'version' in str(record.values()).lower()
            for record in data_records
        )
        
        if has_version_info:
            indicators.append(0.3)
        
        return sum(indicators) if indicators else 0.0
    
    def _assess_documentation_quality(self, data_records: List[Dict[str, Any]]) -> float:
        """Assess documentation quality (0-1 score)"""
        if not data_records:
            return 0.0
        
        doc_scores = []
        
        for record in data_records:
            record_score = 0
            
            # Check for description fields
            desc_fields = ['description', 'tagline', 'pitch', 'summary']
            has_description = any(
                field in str(record.keys()).lower() and record.get(field)
                for field in desc_fields
            )
            if has_description:
                record_score += 0.3
            
            # Check page metadata quality
            page_metadata = record.get('page_metadata', {})
            if isinstance(page_metadata, dict):
                if page_metadata.get('title'):
                    record_score += 0.2
                if page_metadata.get('description'):
                    record_score += 0.2
                if page_metadata.get('meta_tags_count', 0) > 5:
                    record_score += 0.1
            
            # Check for detailed information
            non_empty_fields = sum(1 for v in record.values() if v and str(v).strip())
            total_fields = len(record)
            if total_fields > 0:
                field_ratio = non_empty_fields / total_fields
                record_score += field_ratio * 0.2
            
            doc_scores.append(min(1.0, record_score))
        
        return sum(doc_scores) / len(doc_scores)
    
    def _assess_contact_availability(self, data_records: List[Dict[str, Any]]) -> float:
        """Assess contact information availability (0-1 score)"""
        if not data_records:
            return 0.0
        
        contact_scores = []
        
        for record in data_records:
            contact_score = 0
            
            # Check for direct contact fields
            contact_details = record.get('contact_details', {})
            if isinstance(contact_details, dict):
                if contact_details.get('email'):
                    contact_score += 0.4
                if contact_details.get('phone'):
                    contact_score += 0.3
                if contact_details.get('website'):
                    contact_score += 0.2
            
            # Check for URL (way to contact)
            if record.get('url'):
                contact_score += 0.1
            
            contact_scores.append(min(1.0, contact_score))
        
        return sum(contact_scores) / len(contact_scores)
    
    def _assess_api_quality(self, data_records: List[Dict[str, Any]]) -> float:
        """Assess API quality indicators (0-1 score)"""
        # This is a simplified assessment since we're scraping, not using APIs
        # In a real scenario, you'd assess actual API documentation and endpoints
        
        api_indicators = []
        
        for record in data_records:
            api_score = 0
            
            # Check if source provides API documentation
            record_text = ' '.join(str(v).lower() for v in record.values() if v)
            
            if 'api' in record_text:
                api_score += 0.3
            if 'documentation' in record_text or 'docs' in record_text:
                api_score += 0.2
            if 'endpoint' in record_text:
                api_score += 0.2
            if 'swagger' in record_text or 'openapi' in record_text:
                api_score += 0.3
            
            api_indicators.append(min(1.0, api_score))
        
        return sum(api_indicators) / len(api_indicators) if api_indicators else 0.5
    
    def _assess_data_format_quality(self, audit_result: Dict[str, Any]) -> float:
        """Assess data format quality (0-1 score)"""
        # Base score from data validity and consistency
        validity_score = audit_result.get('validity', {}).get('validity_score', 0)
        consistency_score = audit_result.get('consistency', {}).get('consistency_score', 0)
        
        format_score = (validity_score + consistency_score) / 2
        
        # Bonus for structured data
        field_analysis = audit_result.get('field_analysis', {})
        if field_analysis:
            # Count fields with good data types
            well_typed_fields = 0
            total_fields = len(field_analysis)
            
            for field, analysis in field_analysis.items():
                data_type = analysis.get('data_type', '')
                fill_rate = analysis.get('fill_rate', 0)
                
                # Well-typed and well-filled fields get bonus
                if data_type in ['int64', 'float64', 'datetime64'] or fill_rate > 0.8:
                    well_typed_fields += 1
            
            if total_fields > 0:
                structure_bonus = (well_typed_fields / total_fields) * 0.2
                format_score += structure_bonus
        
        return round(min(1.0, format_score), 3)
    
    def _assess_ease_of_access(self, metadata: Dict[str, Any]) -> float:
        """Assess ease of data access (0-1 score)"""
        access_score = 0.5  # Base score
        
        # Check scraping success rate
        total_records = metadata.get('total_records', 0)
        errors = len(metadata.get('errors', []))
        
        if total_records > 0:
            success_rate = max(0, 1 - (errors / total_records))
            access_score = success_rate
        
        # Bonus for public data sources
        source_name = metadata.get('source', '').lower()
        if any(public_indicator in source_name for public_indicator in ['fca', 'open', 'public']):
            access_score += 0.2
        
        return round(min(1.0, access_score), 3)
    
    def _get_trust_grade(self, score: float) -> str:
        """Convert trust score to letter grade"""
        if score >= 0.9:
            return 'A+'
        elif score >= 0.8:
            return 'A'
        elif score >= 0.7:
            return 'B+'
        elif score >= 0.6:
            return 'B'
        elif score >= 0.5:
            return 'C+'
        elif score >= 0.4:
            return 'C'
        elif score >= 0.3:
            return 'D'
        else:
            return 'F'
    
    def _create_scoring_details(self, inputs: Dict[str, Any], 
                              dimension_scores: Dict[str, float]) -> Dict[str, Any]:
        """Create detailed scoring breakdown"""
        return {
            'data_completeness_details': {
                'overall_completeness': inputs.get('overall_completeness', 0),
                'missing_cells': inputs.get('missing_cells', 0),
                'field_level_variation': self._calculate_field_variation(inputs.get('field_completeness', {}))
            },
            'metadata_quality_details': {
                'metadata_completeness': inputs.get('metadata_completeness', 0),
                'error_tracking': inputs.get('has_error_tracking', False),
                'count_accuracy': inputs.get('count_accuracy', 0)
            },
            'regulatory_compliance_details': {
                'regulatory_mentions': inputs.get('regulatory_mentions', 0),
                'privacy_indicators': inputs.get('privacy_indicators', 0),
                'compliance_score': inputs.get('compliance_indicators', 0)
            },
            'transparency_details': {
                'timeliness_score': inputs.get('timeliness_score', 0),
                'age_days': inputs.get('age_days', 0),
                'update_indicators': inputs.get('update_frequency_indicators', 0)
            },
            'accessibility_details': {
                'api_quality': inputs.get('api_quality_indicators', 0),
                'data_format_quality': inputs.get('data_format_quality', 0),
                'ease_of_access': inputs.get('ease_of_access', 0)
            }
        }
    
    def _calculate_field_variation(self, field_completeness: Dict[str, Any]) -> float:
        """Calculate variation in field completeness"""
        if not field_completeness:
            return 0.0
        
        ratios = [field['non_empty_ratio'] for field in field_completeness.values()]
        if not ratios:
            return 0.0
        
        mean_ratio = sum(ratios) / len(ratios)
        variance = sum((x - mean_ratio)**2 for x in ratios) / len(ratios)
        return round(math.sqrt(variance), 3)
    
    def _generate_trust_recommendations(self, dimension_scores: Dict[str, float]) -> List[str]:
        """Generate recommendations for improving trust score"""
        recommendations = []
        
        # Check each dimension for improvement opportunities
        for dimension, score in dimension_scores.items():
            if score < 0.6:  # Poor score threshold
                if dimension == 'data_completeness':
                    recommendations.append("Improve data collection processes to reduce missing values")
                elif dimension == 'metadata_quality':
                    recommendations.append("Enhance metadata documentation and error tracking")
                elif dimension == 'regulatory_compliance':
                    recommendations.append("Increase regulatory compliance indicators and documentation")
                elif dimension == 'transparency':
                    recommendations.append("Provide more frequent updates and better communication")
                elif dimension == 'accessibility':
                    recommendations.append("Improve data format consistency and access methods")
        
        # Add general recommendations
        min_score = min(dimension_scores.values())
        if min_score < 0.5:
            recommendations.append("Consider comprehensive data governance review")
        
        if not recommendations:
            recommendations.append("Maintain current high standards and monitor for any degradation")
        
        return recommendations
    
    def _calculate_confidence_level(self, inputs: Dict[str, Any]) -> float:
        """Calculate confidence level in the trust score"""
        confidence_factors = []
        
        # More records = higher confidence
        total_records = inputs.get('total_records', 0)
        if total_records > 100:
            confidence_factors.append(1.0)
        elif total_records > 50:
            confidence_factors.append(0.8)
        elif total_records > 10:
            confidence_factors.append(0.6)
        else:
            confidence_factors.append(0.4)
        
        # Fewer errors = higher confidence
        error_count = inputs.get('scraping_errors', 0)
        if error_count == 0:
            confidence_factors.append(1.0)
        elif error_count < 5:
            confidence_factors.append(0.8)
        else:
            confidence_factors.append(0.6)
        
        # Recent data = higher confidence
        if inputs.get('is_recent', False):
            confidence_factors.append(1.0)
        else:
            age_days = inputs.get('age_days', 999)
            if age_days < 30:
                confidence_factors.append(0.8)
            else:
                confidence_factors.append(0.6)
        
        return round(sum(confidence_factors) / len(confidence_factors), 3)
    
    def _break_down_score_components(self, dimension_scores: Dict[str, float]) -> Dict[str, float]:
        """Break down the overall score into weighted components"""
        return {
            dimension: round(score * weight, 3)
            for dimension, score in dimension_scores.items()
            for weight in [self.weights[dimension]]
        }
    
    def _save_trust_report(self, trust_result: Dict[str, Any], source_name: str):
        """Save trust scoring report to file"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"trust_score_{source_name}_{timestamp}.json"
        filepath = os.path.join(PROCESSED_DATA_DIR, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(trust_result, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"Trust score report saved: {filepath}")
    
    def get_trust_ranking(self) -> List[Dict[str, Any]]:
        """Get sources ranked by trust score"""
        if not self.trust_scores:
            return []
        
        ranking = []
        for source, score_data in self.trust_scores.items():
            ranking.append({
                'source': source,
                'trust_score': score_data['overall_trust_score'],
                'trust_grade': score_data['trust_grade'],
                'confidence_level': score_data['confidence_level'],
                'top_strength': self._identify_top_strength(score_data['dimension_scores']),
                'main_weakness': self._identify_main_weakness(score_data['dimension_scores'])
            })
        
        # Sort by trust score (descending)
        ranking.sort(key=lambda x: x['trust_score'], reverse=True)
        
        return ranking
    
    def _identify_top_strength(self, dimension_scores: Dict[str, float]) -> str:
        """Identify the highest scoring dimension"""
        return max(dimension_scores.keys(), key=lambda k: dimension_scores[k])
    
    def _identify_main_weakness(self, dimension_scores: Dict[str, float]) -> str:
        """Identify the lowest scoring dimension"""
        return min(dimension_scores.keys(), key=lambda k: dimension_scores[k])
    
    def compare_sources(self, source_names: List[str] = None) -> Dict[str, Any]:
        """Compare trust scores across sources"""
        if source_names:
            sources_to_compare = {k: v for k, v in self.trust_scores.items() if k in source_names}
        else:
            sources_to_compare = self.trust_scores
        
        if len(sources_to_compare) < 2:
            return {'error': 'Need at least 2 sources to compare'}
        
        comparison = {
            'comparison_timestamp': datetime.now().isoformat(),
            'sources_compared': list(sources_to_compare.keys()),
            'score_comparison': {},
            'dimension_comparison': {},
            'ranking': [],
            'insights': []
        }
        
        # Overall score comparison
        for source, score_data in sources_to_compare.items():
            comparison['score_comparison'][source] = {
                'overall_score': score_data['overall_trust_score'],
                'grade': score_data['trust_grade'],
                'confidence': score_data['confidence_level']
            }
        
        # Dimension-by-dimension comparison
        dimensions = list(self.weights.keys())
        for dimension in dimensions:
            comparison['dimension_comparison'][dimension] = {
                source: score_data['dimension_scores'][dimension]
                for source, score_data in sources_to_compare.items()
            }
        
        # Create ranking
        comparison['ranking'] = self.get_trust_ranking()
        
        # Generate insights
        comparison['insights'] = self._generate_comparison_insights(sources_to_compare)
        
        return comparison
    
    def _generate_comparison_insights(self, sources: Dict[str, Any]) -> List[str]:
        """Generate insights from source comparison"""
        insights = []
        
        # Find best and worst performing sources
        scores = {source: data['overall_trust_score'] for source, data in sources.items()}
        best_source = max(scores.keys(), key=lambda k: scores[k])
        worst_source = min(scores.keys(), key=lambda k: scores[k])
        
        insights.append(f"Highest trust score: {best_source} ({scores[best_source]:.3f})")
        insights.append(f"Lowest trust score: {worst_source} ({scores[worst_source]:.3f})")
        
        # Analyze dimension patterns
        dimension_averages = {}
        for dimension in self.weights.keys():
            dimension_scores = [data['dimension_scores'][dimension] for data in sources.values()]
            dimension_averages[dimension] = sum(dimension_scores) / len(dimension_scores)
        
        strongest_dimension = max(dimension_averages.keys(), key=lambda k: dimension_averages[k])
        weakest_dimension = min(dimension_averages.keys(), key=lambda k: dimension_averages[k])
        
        insights.append(f"Strongest overall dimension: {strongest_dimension} ({dimension_averages[strongest_dimension]:.3f})")
        insights.append(f"Weakest overall dimension: {weakest_dimension} ({dimension_averages[weakest_dimension]:.3f})")
        
        # Score spread analysis
        score_range = max(scores.values()) - min(scores.values())
        if score_range > 0.3:
            insights.append("High variation in trust scores suggests significant quality differences")
        else:
            insights.append("Relatively consistent trust scores across sources")
        
        return insights
    
    def get_trust_summary(self) -> Dict[str, Any]:
        """Get summary of all trust scores"""
        if not self.trust_scores:
            return {'error': 'No trust scores calculated yet'}
        
        scores = [data['overall_trust_score'] for data in self.trust_scores.values()]
        
        return {
            'summary_timestamp': datetime.now().isoformat(),
            'total_sources': len(self.trust_scores),
            'average_trust_score': round(sum(scores) / len(scores), 3),
            'highest_score': max(scores),
            'lowest_score': min(scores),
            'score_range': round(max(scores) - min(scores), 3),
            'sources_by_grade': self._count_sources_by_grade(),
            'dimension_averages': self._calculate_dimension_averages()
        }
    
    def _count_sources_by_grade(self) -> Dict[str, int]:
        """Count sources by trust grade"""
        grade_counts = {}
        for data in self.trust_scores.values():
            grade = data['trust_grade']
            grade_counts[grade] = grade_counts.get(grade, 0) + 1
        return grade_counts
    
    def _calculate_dimension_averages(self) -> Dict[str, float]:
        """Calculate average scores for each dimension"""
        dimension_averages = {}
        
        for dimension in self.weights.keys():
            scores = [data['dimension_scores'][dimension] for data in self.trust_scores.values()]
            dimension_averages[dimension] = round(sum(scores) / len(scores), 3)
        
        return dimension_averages