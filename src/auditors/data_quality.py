"""
Data Quality Auditor for London Fintech Data Trust Index
Performs comprehensive data quality checks on scraped datasets
"""

import pandas as pd
import numpy as np
import json
import logging
from typing import Dict, List, Any, Tuple
from datetime import datetime, timedelta
import re
from collections import Counter

# Import configuration
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from config.config import QUALITY_THRESHOLDS, PROCESSED_DATA_DIR

class DataQualityAuditor:
    """Comprehensive data quality auditor for fintech datasets"""
    
    def __init__(self):
        self.logger = logging.getLogger("data_quality_auditor")
        logging.basicConfig(level=logging.INFO)
        
        # Quality metrics storage
        self.audit_results = {}
        self.quality_scores = {}
        
        # Data type patterns for validation
        self.data_patterns = {
            'email': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
            'phone': r'^(\+44|0)[\d\s\-\(\)]{10,}$',
            'url': r'^https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)$',
            'postcode': r'^[A-Z]{1,2}[0-9][A-Z0-9]?\s?[0-9][A-Z]{2}$',
            'currency': r'^£[\d,]+(\.\d{2})?$',
            'date': r'^\d{1,2}[\/\-]\d{1,2}[\/\-]\d{4}$',
            'frn': r'^\d{6,8}$'  # FCA Firm Reference Number
        }
    
    def audit_dataset(self, data_file_path: str, source_name: str) -> Dict[str, Any]:
        """Perform comprehensive audit of a dataset"""
        self.logger.info(f"Starting data quality audit for {source_name}")
        
        # Load the dataset
        dataset = self._load_dataset(data_file_path)
        if not dataset:
            return self._create_empty_audit_result(source_name)
        
        data_records = dataset.get('data', [])
        metadata = dataset.get('metadata', {})
        
        if not data_records:
            return self._create_empty_audit_result(source_name)
        
        # Convert to DataFrame for easier analysis
        df = pd.DataFrame(data_records)
        
        # Perform various quality checks
        audit_result = {
            'source': source_name,
            'audit_timestamp': datetime.now().isoformat(),
            'total_records': len(df),
            'total_fields': len(df.columns),
            
            # Core quality metrics
            'completeness': self._assess_completeness(df),
            'consistency': self._assess_consistency(df),
            'validity': self._assess_validity(df),
            'uniqueness': self._assess_uniqueness(df),
            'timeliness': self._assess_timeliness(metadata),
            
            # Metadata quality
            'metadata_quality': self._assess_metadata_quality(df, metadata),
            
            # Field-level analysis
            'field_analysis': self._analyze_fields(df),
            
            # Data profiling
            'data_profile': self._create_data_profile(df),
            
            # Quality issues
            'quality_issues': self._identify_quality_issues(df),
            
            # Overall scores
            'overall_quality_score': 0  # Will be calculated
        }
        
        # Calculate overall quality score
        audit_result['overall_quality_score'] = self._calculate_overall_score(audit_result)
        
        # Store results
        self.audit_results[source_name] = audit_result
        
        # Save audit report
        self._save_audit_report(audit_result, source_name)
        
        return audit_result
    
    def _load_dataset(self, file_path: str) -> Dict[str, Any]:
        """Load dataset from JSON file"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            self.logger.error(f"Failed to load dataset {file_path}: {str(e)}")
            return {}
    
    def _assess_completeness(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Assess data completeness"""
        total_cells = df.size
        missing_cells = df.isnull().sum().sum()
        empty_string_cells = (df == '').sum().sum()
        
        # Count cells with only whitespace
        whitespace_cells = 0
        for col in df.select_dtypes(include=['object']).columns:
            whitespace_cells += df[col].astype(str).str.strip().eq('').sum()
        
        total_missing = missing_cells + empty_string_cells + whitespace_cells
        completeness_ratio = 1 - (total_missing / total_cells) if total_cells > 0 else 0
        
        # Field-level completeness
        field_completeness = {}
        for col in df.columns:
            non_null = df[col].notna().sum()
            non_empty = (df[col].astype(str).str.strip() != '').sum()
            field_completeness[col] = {
                'non_null_ratio': non_null / len(df),
                'non_empty_ratio': non_empty / len(df),
                'missing_count': len(df) - non_null,
                'empty_count': len(df) - non_empty
            }
        
        return {
            'overall_completeness': completeness_ratio,
            'missing_cells': int(missing_cells),
            'empty_cells': int(empty_string_cells),
            'whitespace_cells': int(whitespace_cells),
            'field_completeness': field_completeness,
            'completeness_grade': self._grade_completeness(completeness_ratio)
        }
    
    def _assess_consistency(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Assess data consistency"""
        consistency_issues = []
        consistency_score = 1.0
        
        for col in df.columns:
            # Check data type consistency
            if df[col].dtype == 'object':
                # Check for mixed types (strings that could be numbers)
                numeric_pattern = r'^\d+\.?\d*$'
                mixed_types = 0
                
                for value in df[col].dropna().astype(str):
                    if value.strip():
                        is_numeric = bool(re.match(numeric_pattern, value.strip()))
                        # Check if this value type is different from others
                        # This is a simplified check
                        if is_numeric and not all(re.match(numeric_pattern, str(v).strip()) 
                                                for v in df[col].dropna().astype(str) if v.strip()):
                            mixed_types += 1
                
                if mixed_types > 0:
                    consistency_issues.append({
                        'field': col,
                        'issue': 'mixed_data_types',
                        'count': mixed_types
                    })
                    consistency_score -= 0.1
            
            # Check format consistency for specific patterns
            if col.lower() in ['email', 'phone', 'url', 'postcode']:
                pattern = self.data_patterns.get(col.lower())
                if pattern:
                    invalid_formats = 0
                    for value in df[col].dropna().astype(str):
                        if value.strip() and not re.match(pattern, value.strip(), re.IGNORECASE):
                            invalid_formats += 1
                    
                    if invalid_formats > 0:
                        consistency_issues.append({
                            'field': col,
                            'issue': 'invalid_format',
                            'count': invalid_formats,
                            'expected_pattern': pattern
                        })
                        consistency_score -= 0.1
        
        return {
            'consistency_score': max(0, consistency_score),
            'consistency_issues': consistency_issues,
            'issues_count': len(consistency_issues)
        }
    
    def _assess_validity(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Assess data validity"""
        validity_issues = []
        validity_score = 1.0
        
        for col in df.columns:
            col_lower = col.lower()
            
            # Email validation
            if 'email' in col_lower:
                invalid_emails = 0
                for value in df[col].dropna().astype(str):
                    if value.strip() and not re.match(self.data_patterns['email'], value.strip()):
                        invalid_emails += 1
                
                if invalid_emails > 0:
                    validity_issues.append({
                        'field': col,
                        'issue': 'invalid_email_format',
                        'count': invalid_emails
                    })
            
            # URL validation
            if 'url' in col_lower or 'website' in col_lower:
                invalid_urls = 0
                for value in df[col].dropna().astype(str):
                    if value.strip() and not re.match(self.data_patterns['url'], value.strip()):
                        invalid_urls += 1
                
                if invalid_urls > 0:
                    validity_issues.append({
                        'field': col,
                        'issue': 'invalid_url_format',
                        'count': invalid_urls
                    })
            
            # Date validation
            if 'date' in col_lower or 'founded' in col_lower or 'updated' in col_lower:
                invalid_dates = 0
                for value in df[col].dropna().astype(str):
                    if value.strip():
                        try:
                            # Try to parse various date formats
                            pd.to_datetime(value.strip())
                        except:
                            invalid_dates += 1
                
                if invalid_dates > 0:
                    validity_issues.append({
                        'field': col,
                        'issue': 'invalid_date_format',
                        'count': invalid_dates
                    })
            
            # Numeric range validation
            if df[col].dtype in ['int64', 'float64']:
                # Check for unrealistic values
                if col_lower in ['year', 'founded_year']:
                    current_year = datetime.now().year
                    invalid_years = ((df[col] < 1800) | (df[col] > current_year)).sum()
                    if invalid_years > 0:
                        validity_issues.append({
                            'field': col,
                            'issue': 'invalid_year_range',
                            'count': int(invalid_years)
                        })
        
        # Calculate validity score
        if validity_issues:
            validity_score = max(0, 1 - (len(validity_issues) * 0.1))
        
        return {
            'validity_score': validity_score,
            'validity_issues': validity_issues,
            'issues_count': len(validity_issues)
        }
    
    def _assess_uniqueness(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Assess data uniqueness"""
        # Check for duplicate records
        duplicate_rows = df.duplicated().sum()
        
        # Check for duplicate values in key fields
        duplicate_fields = {}
        key_fields = ['frn', 'company_name', 'name', 'url', 'email']
        
        for col in df.columns:
            if any(key_field in col.lower() for key_field in key_fields):
                duplicates = df[col].duplicated().sum()
                if duplicates > 0:
                    duplicate_fields[col] = int(duplicates)
        
        uniqueness_score = 1 - (duplicate_rows / len(df)) if len(df) > 0 else 1
        
        return {
            'uniqueness_score': uniqueness_score,
            'duplicate_rows': int(duplicate_rows),
            'duplicate_fields': duplicate_fields,
            'total_duplicates': int(duplicate_rows) + sum(duplicate_fields.values())
        }
    
    def _assess_timeliness(self, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Assess data timeliness"""
        scraped_at = metadata.get('scraped_at', '')
        
        if not scraped_at:
            return {
                'timeliness_score': 0,
                'age_days': None,
                'is_recent': False
            }
        
        try:
            scraped_date = datetime.fromisoformat(scraped_at.replace('Z', '+00:00'))
            age_days = (datetime.now() - scraped_date.replace(tzinfo=None)).days
            
            # Consider data recent if less than 7 days old
            is_recent = age_days <= 7
            
            # Score based on age (1.0 for same day, decreasing over time)
            timeliness_score = max(0, 1 - (age_days / 30))  # 30 days = 0 score
            
            return {
                'timeliness_score': timeliness_score,
                'age_days': age_days,
                'is_recent': is_recent,
                'scraped_at': scraped_at
            }
        
        except Exception as e:
            self.logger.warning(f"Could not parse scraped_at date: {scraped_at}")
            return {
                'timeliness_score': 0.5,  # Unknown age
                'age_days': None,
                'is_recent': False
            }
    
    def _assess_metadata_quality(self, df: pd.DataFrame, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Assess quality of metadata"""
        required_metadata_fields = ['source', 'scraped_at', 'total_records']
        
        present_fields = sum(1 for field in required_metadata_fields if metadata.get(field))
        metadata_completeness = present_fields / len(required_metadata_fields)
        
        # Check if record count matches actual data
        stated_records = metadata.get('total_records', 0)
        actual_records = len(df)
        count_accuracy = 1.0 if stated_records == actual_records else 0.5
        
        # Check for error tracking
        has_error_tracking = bool(metadata.get('errors'))
        
        return {
            'metadata_completeness': metadata_completeness,
            'count_accuracy': count_accuracy,
            'has_error_tracking': has_error_tracking,
            'metadata_score': (metadata_completeness + count_accuracy) / 2
        }
    
    def _analyze_fields(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze individual fields"""
        field_analysis = {}
        
        for col in df.columns:
            analysis = {
                'data_type': str(df[col].dtype),
                'unique_values': int(df[col].nunique()),
                'null_count': int(df[col].isnull().sum()),
                'empty_string_count': int((df[col] == '').sum()),
                'fill_rate': float(df[col].notna().sum() / len(df))
            }
            
            # Additional analysis for object columns
            if df[col].dtype == 'object':
                non_null_values = df[col].dropna().astype(str)
                if len(non_null_values) > 0:
                    analysis.update({
                        'avg_length': float(non_null_values.str.len().mean()),
                        'max_length': int(non_null_values.str.len().max()),
                        'min_length': int(non_null_values.str.len().min()),
                        'most_common_values': non_null_values.value_counts().head(3).to_dict()
                    })
            
            # Additional analysis for numeric columns
            elif df[col].dtype in ['int64', 'float64']:
                non_null_values = df[col].dropna()
                if len(non_null_values) > 0:
                    analysis.update({
                        'min_value': float(non_null_values.min()),
                        'max_value': float(non_null_values.max()),
                        'mean_value': float(non_null_values.mean()),
                        'std_value': float(non_null_values.std())
                    })
            
            field_analysis[col] = analysis
        
        return field_analysis
    
    def _create_data_profile(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Create a comprehensive data profile"""
        return {
            'shape': {
                'rows': len(df),
                'columns': len(df.columns)
            },
            'data_types': df.dtypes.value_counts().to_dict(),
            'memory_usage_mb': float(df.memory_usage(deep=True).sum() / 1024 / 1024),
            'column_names': list(df.columns),
            'sample_records': df.head(3).to_dict('records') if len(df) > 0 else []
        }
    
    def _identify_quality_issues(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Identify specific data quality issues"""
        issues = []
        
        # Check for fields with very low fill rates
        for col in df.columns:
            fill_rate = df[col].notna().sum() / len(df)
            if fill_rate < 0.5:  # Less than 50% filled
                issues.append({
                    'type': 'low_fill_rate',
                    'field': col,
                    'fill_rate': fill_rate,
                    'severity': 'high' if fill_rate < 0.2 else 'medium'
                })
        
        # Check for fields with suspicious patterns
        for col in df.columns:
            if df[col].dtype == 'object':
                non_null_values = df[col].dropna().astype(str)
                if len(non_null_values) > 0:
                    # Check for fields with mostly identical values
                    most_common_count = non_null_values.value_counts().iloc[0]
                    if most_common_count / len(non_null_values) > 0.9:
                        issues.append({
                            'type': 'low_diversity',
                            'field': col,
                            'dominant_value_ratio': most_common_count / len(non_null_values),
                            'severity': 'medium'
                        })
                    
                    # Check for unusually long values
                    max_len = non_null_values.str.len().max()
                    if max_len > 1000:  # Very long text
                        issues.append({
                            'type': 'unusually_long_values',
                            'field': col,
                            'max_length': max_len,
                            'severity': 'low'
                        })
        
        # Check for missing key identifier fields
        identifier_fields = ['id', 'frn', 'name', 'company_name', 'url']
        has_identifier = any(any(id_field in col.lower() for id_field in identifier_fields) 
                           for col in df.columns)
        
        if not has_identifier:
            issues.append({
                'type': 'missing_identifier',
                'field': 'dataset',
                'severity': 'high',
                'description': 'No clear identifier field found'
            })
        
        return issues
    
    def _calculate_overall_score(self, audit_result: Dict[str, Any]) -> float:
        """Calculate overall quality score"""
        # Weight different quality dimensions
        weights = {
            'completeness': 0.25,
            'consistency': 0.20,
            'validity': 0.25,
            'uniqueness': 0.15,
            'timeliness': 0.10,
            'metadata_quality': 0.05
        }
        
        scores = {
            'completeness': audit_result['completeness']['overall_completeness'],
            'consistency': audit_result['consistency']['consistency_score'],
            'validity': audit_result['validity']['validity_score'],
            'uniqueness': audit_result['uniqueness']['uniqueness_score'],
            'timeliness': audit_result['timeliness']['timeliness_score'],
            'metadata_quality': audit_result['metadata_quality']['metadata_score']
        }
        
        # Calculate weighted average
        overall_score = sum(scores[dim] * weights[dim] for dim in weights.keys())
        
        return round(overall_score, 3)
    
    def _grade_completeness(self, completeness_ratio: float) -> str:
        """Grade completeness based on thresholds"""
        if completeness_ratio >= QUALITY_THRESHOLDS['completeness_excellent']:
            return 'Excellent'
        elif completeness_ratio >= QUALITY_THRESHOLDS['completeness_good']:
            return 'Good'
        elif completeness_ratio >= QUALITY_THRESHOLDS['completeness_poor']:
            return 'Fair'
        else:
            return 'Poor'
    
    def _create_empty_audit_result(self, source_name: str) -> Dict[str, Any]:
        """Create empty audit result for failed audits"""
        return {
            'source': source_name,
            'audit_timestamp': datetime.now().isoformat(),
            'total_records': 0,
            'total_fields': 0,
            'error': 'Unable to load or process dataset',
            'overall_quality_score': 0
        }
    
    def _save_audit_report(self, audit_result: Dict[str, Any], source_name: str):
        """Save audit report to file"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"quality_audit_{source_name}_{timestamp}.json"
        filepath = os.path.join(PROCESSED_DATA_DIR, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(audit_result, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"Audit report saved: {filepath}")
    
    def get_audit_summary(self, source_name: str = None) -> Dict[str, Any]:
        """Get summary of audit results"""
        if source_name and source_name in self.audit_results:
            results = {source_name: self.audit_results[source_name]}
        else:
            results = self.audit_results
        
        summary = {
            'total_sources_audited': len(results),
            'audit_timestamp': datetime.now().isoformat(),
            'source_summaries': {}
        }
        
        for source, audit_result in results.items():
            summary['source_summaries'][source] = {
                'overall_score': audit_result.get('overall_quality_score', 0),
                'total_records': audit_result.get('total_records', 0),
                'completeness_grade': audit_result.get('completeness', {}).get('completeness_grade', 'Unknown'),
                'major_issues': len([issue for issue in audit_result.get('quality_issues', []) 
                                   if issue.get('severity') == 'high']),
                'last_updated': audit_result.get('timeliness', {}).get('scraped_at', 'Unknown')
            }
        
        # Calculate average quality score
        scores = [result['overall_score'] for result in summary['source_summaries'].values()]
        summary['average_quality_score'] = sum(scores) / len(scores) if scores else 0
        
        return summary
    
    def compare_sources(self) -> Dict[str, Any]:
        """Compare quality across different data sources"""
        if len(self.audit_results) < 2:
            return {'error': 'Need at least 2 sources to compare'}
        
        comparison = {
            'comparison_timestamp': datetime.now().isoformat(),
            'sources_compared': list(self.audit_results.keys()),
            'quality_comparison': {},
            'best_source': '',
            'worst_source': '',
            'recommendations': []
        }
        
        # Compare quality dimensions
        dimensions = ['completeness', 'consistency', 'validity', 'uniqueness', 'timeliness']
        
        for dimension in dimensions:
            dimension_scores = {}
            for source, audit_result in self.audit_results.items():
                if dimension in audit_result:
                    if dimension == 'completeness':
                        score = audit_result[dimension]['overall_completeness']
                    else:
                        score = audit_result[dimension].get(f'{dimension}_score', 0)
                    dimension_scores[source] = score
            
            comparison['quality_comparison'][dimension] = dimension_scores
        
        # Find best and worst sources
        overall_scores = {source: audit_result.get('overall_quality_score', 0) 
                         for source, audit_result in self.audit_results.items()}
        
        comparison['best_source'] = max(overall_scores.keys(), key=lambda k: overall_scores[k])
        comparison['worst_source'] = min(overall_scores.keys(), key=lambda k: overall_scores[k])
        
        # Generate recommendations
        comparison['recommendations'] = self._generate_recommendations()
        
        return comparison
    
    def _generate_recommendations(self) -> List[str]:
        """Generate quality improvement recommendations"""
        recommendations = []
        
        for source, audit_result in self.audit_results.items():
            overall_score = audit_result.get('overall_quality_score', 0)
            
            if overall_score < 0.6:  # Poor quality
                recommendations.append(f"Source '{source}' needs significant quality improvements")
            
            # Specific recommendations based on quality issues
            completeness = audit_result.get('completeness', {}).get('overall_completeness', 0)
            if completeness < 0.7:
                recommendations.append(f"Improve data completeness for '{source}' (currently {completeness:.1%})")
            
            validity_issues = len(audit_result.get('validity', {}).get('validity_issues', []))
            if validity_issues > 0:
                recommendations.append(f"Address {validity_issues} validity issues in '{source}'")
            
            consistency_issues = len(audit_result.get('consistency', {}).get('consistency_issues', []))
            if consistency_issues > 0:
                recommendations.append(f"Fix {consistency_issues} consistency issues in '{source}'")
        
        return recommendations