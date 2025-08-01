"""
Main pipeline orchestrator for London Fintech Data Trust Index
Coordinates data collection, quality auditing, and trust scoring
"""

import logging
import json
import os
import sys
from datetime import datetime
from typing import Dict, List, Any

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import project modules
from config.config import DATA_SOURCES, RAW_DATA_DIR, PROCESSED_DATA_DIR, LOG_FILE
from src.scrapers.seedrs_scraper import SeedrsScaper
from src.scrapers.fca_scraper import FcaScraper
from src.auditors.data_quality import DataQualityAuditor
from src.scoring.trust_scorer import TrustIndexScorer

class FintechTrustIndexPipeline:
    """Main pipeline orchestrator"""
    
    def __init__(self):
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(LOG_FILE),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger("main_pipeline")
        
        # Initialize components
        self.scrapers = {}
        self.auditor = DataQualityAuditor()
        self.scorer = TrustIndexScorer()
        
        # Pipeline results
        self.pipeline_results = {
            'execution_timestamp': datetime.now().isoformat(),
            'scraped_sources': {},
            'audit_results': {},
            'trust_scores': {},
            'summary': {},
            'errors': []
        }
        
        self.logger.info("Fintech Trust Index Pipeline initialized")
    
    def run_full_pipeline(self) -> Dict[str, Any]:
        """Execute the complete pipeline"""
        self.logger.info("Starting full pipeline execution")
        
        try:
            # Step 1: Data Collection
            self.logger.info("=" * 50)
            self.logger.info("STEP 1: DATA COLLECTION")
            self.logger.info("=" * 50)
            self._collect_all_data()
            
            # Step 2: Data Quality Auditing
            self.logger.info("=" * 50)
            self.logger.info("STEP 2: DATA QUALITY AUDITING")
            self.logger.info("=" * 50)
            self._audit_all_data()
            
            # Step 3: Trust Score Calculation
            self.logger.info("=" * 50)
            self.logger.info("STEP 3: TRUST SCORE CALCULATION")
            self.logger.info("=" * 50)
            self._calculate_all_trust_scores()
            
            # Step 4: Generate Summary
            self.logger.info("=" * 50)
            self.logger.info("STEP 4: GENERATE SUMMARY")
            self.logger.info("=" * 50)
            self._generate_pipeline_summary()
            
            # Save pipeline results
            self._save_pipeline_results()
            
            self.logger.info("Pipeline execution completed successfully")
            return self.pipeline_results
            
        except Exception as e:
            error_msg = f"Pipeline execution failed: {str(e)}"
            self.logger.error(error_msg)
            self.pipeline_results['errors'].append({
                'error': error_msg,
                'timestamp': datetime.now().isoformat()
            })
            return self.pipeline_results
    
    def _collect_all_data(self):
        """Collect data from all enabled sources"""
        self.logger.info("Starting data collection phase")
        
        # Initialize scrapers for enabled sources
        for source_name, config in DATA_SOURCES.items():
            if config.get('enabled', False):
                self.logger.info(f"Initializing scraper for: {source_name}")
                
                try:
                    if source_name == 'seedrs':
                        scraper = SeedrsScaper()
                    elif source_name == 'fca':
                        scraper = FcaScraper()
                    else:
                        self.logger.warning(f"No scraper implemented for: {source_name}")
                        continue
                    
                    self.scrapers[source_name] = scraper
                    
                except Exception as e:
                    error_msg = f"Failed to initialize scraper for {source_name}: {str(e)}"
                    self.logger.error(error_msg)
                    self.pipeline_results['errors'].append({
                        'source': source_name,
                        'phase': 'scraper_initialization',
                        'error': error_msg,
                        'timestamp': datetime.now().isoformat()
                    })
        
        # Execute scraping for each initialized scraper
        for source_name, scraper in self.scrapers.items():
            self.logger.info(f"Scraping data from: {source_name}")
            
            try:
                # Scrape data
                scraped_data = scraper.scrape()
                
                # Save scraped data
                data_file = scraper.save_data()
                
                # Store results
                self.pipeline_results['scraped_sources'][source_name] = {
                    'status': 'success',
                    'records_count': len(scraped_data),
                    'data_file': data_file,
                    'scraper_summary': scraper.get_summary(),
                    'scraped_at': datetime.now().isoformat()
                }
                
                self.logger.info(f"Successfully scraped {len(scraped_data)} records from {source_name}")
                
            except Exception as e:
                error_msg = f"Scraping failed for {source_name}: {str(e)}"
                self.logger.error(error_msg)
                
                self.pipeline_results['scraped_sources'][source_name] = {
                    'status': 'failed',
                    'error': error_msg,
                    'scraped_at': datetime.now().isoformat()
                }
                
                self.pipeline_results['errors'].append({
                    'source': source_name,
                    'phase': 'data_collection',
                    'error': error_msg,
                    'timestamp': datetime.now().isoformat()
                })
    
    def _audit_all_data(self):
        """Audit data quality for all successfully scraped sources"""
        self.logger.info("Starting data quality auditing phase")
        
        for source_name, scrape_result in self.pipeline_results['scraped_sources'].items():
            if scrape_result['status'] != 'success':
                self.logger.info(f"Skipping audit for {source_name} (scraping failed)")
                continue
            
            self.logger.info(f"Auditing data quality for: {source_name}")
            
            try:
                data_file = scrape_result['data_file']
                
                # Perform quality audit
                audit_result = self.auditor.audit_dataset(data_file, source_name)
                
                # Store audit results
                self.pipeline_results['audit_results'][source_name] = {
                    'status': 'success',
                    'audit_result': audit_result,
                    'audited_at': datetime.now().isoformat()
                }
                
                self.logger.info(f"Quality audit completed for {source_name}")
                self.logger.info(f"Overall quality score: {audit_result.get('overall_quality_score', 'N/A')}")
                
            except Exception as e:
                error_msg = f"Quality audit failed for {source_name}: {str(e)}"
                self.logger.error(error_msg)
                
                self.pipeline_results['audit_results'][source_name] = {
                    'status': 'failed',
                    'error': error_msg,
                    'audited_at': datetime.now().isoformat()
                }
                
                self.pipeline_results['errors'].append({
                    'source': source_name,
                    'phase': 'quality_audit',
                    'error': error_msg,
                    'timestamp': datetime.now().isoformat()
                })
    
    def _calculate_all_trust_scores(self):
        """Calculate trust scores for all audited sources"""
        self.logger.info("Starting trust score calculation phase")
        
        for source_name, audit_result_data in self.pipeline_results['audit_results'].items():
            if audit_result_data['status'] != 'success':
                self.logger.info(f"Skipping trust scoring for {source_name} (audit failed)")
                continue
            
            self.logger.info(f"Calculating trust score for: {source_name}")
            
            try:
                # Load raw data
                scrape_result = self.pipeline_results['scraped_sources'][source_name]
                data_file = scrape_result['data_file']
                
                with open(data_file, 'r', encoding='utf-8') as f:
                    raw_data = json.load(f)
                
                audit_result = audit_result_data['audit_result']
                
                # Calculate trust score
                trust_score = self.scorer.calculate_trust_score(source_name, audit_result, raw_data)
                
                # Store trust score results
                self.pipeline_results['trust_scores'][source_name] = {
                    'status': 'success',
                    'trust_score': trust_score,
                    'calculated_at': datetime.now().isoformat()
                }
                
                self.logger.info(f"Trust score calculated for {source_name}")
                self.logger.info(f"Overall trust score: {trust_score.get('overall_trust_score', 'N/A')} ({trust_score.get('trust_grade', 'N/A')})")
                
            except Exception as e:
                error_msg = f"Trust scoring failed for {source_name}: {str(e)}"
                self.logger.error(error_msg)
                
                self.pipeline_results['trust_scores'][source_name] = {
                    'status': 'failed',
                    'error': error_msg,
                    'calculated_at': datetime.now().isoformat()
                }
                
                self.pipeline_results['errors'].append({
                    'source': source_name,
                    'phase': 'trust_scoring',
                    'error': error_msg,
                    'timestamp': datetime.now().isoformat()
                })
    
    def _generate_pipeline_summary(self):
        """Generate comprehensive pipeline summary"""
        self.logger.info("Generating pipeline summary")
        
        # Count successful vs failed operations
        scraping_stats = self._count_operation_stats(self.pipeline_results['scraped_sources'])
        audit_stats = self._count_operation_stats(self.pipeline_results['audit_results'])
        scoring_stats = self._count_operation_stats(self.pipeline_results['trust_scores'])
        
        # Calculate total records processed
        total_records = sum(
            result.get('records_count', 0) 
            for result in self.pipeline_results['scraped_sources'].values()
            if result['status'] == 'success'
        )
        
        # Get trust score statistics
        trust_scores = [
            result['trust_score']['overall_trust_score']
            for result in self.pipeline_results['trust_scores'].values()
            if result['status'] == 'success'
        ]
        
        # Create summary
        summary = {
            'pipeline_execution': {
                'started_at': self.pipeline_results['execution_timestamp'],
                'completed_at': datetime.now().isoformat(),
                'total_errors': len(self.pipeline_results['errors']),
                'status': 'completed_with_errors' if self.pipeline_results['errors'] else 'completed_successfully'
            },
            'data_collection': {
                'sources_attempted': scraping_stats['total'],
                'sources_successful': scraping_stats['success'],
                'sources_failed': scraping_stats['failed'],
                'total_records_collected': total_records
            },
            'quality_auditing': {
                'sources_audited': audit_stats['success'],
                'audits_failed': audit_stats['failed']
            },
            'trust_scoring': {
                'sources_scored': scoring_stats['success'],
                'scoring_failed': scoring_stats['failed'],
                'average_trust_score': sum(trust_scores) / len(trust_scores) if trust_scores else 0,
                'highest_trust_score': max(trust_scores) if trust_scores else 0,
                'lowest_trust_score': min(trust_scores) if trust_scores else 0
            },
            'source_rankings': self._create_source_rankings(),
            'recommendations': self._generate_pipeline_recommendations()
        }
        
        self.pipeline_results['summary'] = summary
        
        # Log summary
        self.logger.info("Pipeline Summary:")
        self.logger.info(f"  Total sources processed: {scraping_stats['total']}")
        self.logger.info(f"  Successful data collection: {scraping_stats['success']}")
        self.logger.info(f"  Total records collected: {total_records}")
        self.logger.info(f"  Quality audits completed: {audit_stats['success']}")
        self.logger.info(f"  Trust scores calculated: {scoring_stats['success']}")
        if trust_scores:
            self.logger.info(f"  Average trust score: {summary['trust_scoring']['average_trust_score']:.3f}")
        self.logger.info(f"  Total errors: {len(self.pipeline_results['errors'])}")
    
    def _count_operation_stats(self, operation_results: Dict[str, Any]) -> Dict[str, int]:
        """Count success/failure statistics for operations"""
        stats = {'total': 0, 'success': 0, 'failed': 0}
        
        for result in operation_results.values():
            stats['total'] += 1
            if result['status'] == 'success':
                stats['success'] += 1
            else:
                stats['failed'] += 1
        
        return stats
    
    def _create_source_rankings(self) -> List[Dict[str, Any]]:
        """Create rankings of sources by trust score"""
        rankings = []
        
        for source_name, score_result in self.pipeline_results['trust_scores'].items():
            if score_result['status'] == 'success':
                trust_data = score_result['trust_score']
                rankings.append({
                    'source': source_name,
                    'trust_score': trust_data['overall_trust_score'],
                    'trust_grade': trust_data['trust_grade'],
                    'confidence_level': trust_data['confidence_level'],
                    'records_count': self.pipeline_results['scraped_sources'][source_name].get('records_count', 0)
                })
        
        # Sort by trust score (descending)
        rankings.sort(key=lambda x: x['trust_score'], reverse=True)
        
        return rankings
    
    def _generate_pipeline_recommendations(self) -> List[str]:
        """Generate recommendations based on pipeline results"""
        recommendations = []
        
        # Check for failed operations
        failed_scraping = [
            source for source, result in self.pipeline_results['scraped_sources'].items()
            if result['status'] != 'success'
        ]
        
        if failed_scraping:
            recommendations.append(f"Investigate scraping failures for: {', '.join(failed_scraping)}")
        
        # Check trust score distribution
        trust_scores = [
            result['trust_score']['overall_trust_score']
            for result in self.pipeline_results['trust_scores'].values()
            if result['status'] == 'success'
        ]
        
        if trust_scores:
            avg_score = sum(trust_scores) / len(trust_scores)
            
            if avg_score < 0.6:
                recommendations.append("Overall data quality is below acceptable threshold - comprehensive review needed")
            elif avg_score < 0.7:
                recommendations.append("Data quality is moderate - focus on improving weakest sources")
            
            # Check for large score variations
            if len(trust_scores) > 1:
                score_range = max(trust_scores) - min(trust_scores)
                if score_range > 0.3:
                    recommendations.append("Large variation in source quality - standardize data collection processes")
        
        # Check error frequency
        error_count = len(self.pipeline_results['errors'])
        if error_count > 5:
            recommendations.append("High error rate detected - review scraping and processing logic")
        
        if not recommendations:
            recommendations.append("Pipeline executed successfully with good overall data quality")
        
        return recommendations
    
    def _save_pipeline_results(self):
        """Save complete pipeline results to file"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"pipeline_results_{timestamp}.json"
        filepath = os.path.join(PROCESSED_DATA_DIR, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(self.pipeline_results, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"Pipeline results saved to: {filepath}")
    
    def get_dashboard_data(self) -> Dict[str, Any]:
        """Prepare data for dashboard visualization"""
        return {
            'last_updated': datetime.now().isoformat(),
            'summary': self.pipeline_results.get('summary', {}),
            'source_rankings': self._create_source_rankings(),
            'trust_scores': {
                source: result['trust_score'] 
                for source, result in self.pipeline_results['trust_scores'].items()
                if result['status'] == 'success'
            },
            'quality_metrics': {
                source: result['audit_result']
                for source, result in self.pipeline_results['audit_results'].items()
                if result['status'] == 'success'
            },
            'collection_stats': {
                source: {
                    'records_count': result.get('records_count', 0),
                    'status': result['status'],
                    'scraped_at': result.get('scraped_at', '')
                }
                for source, result in self.pipeline_results['scraped_sources'].items()
            },
            'errors': self.pipeline_results['errors']
        }
    
    def run_incremental_update(self, sources: List[str] = None) -> Dict[str, Any]:
        """Run incremental update for specific sources"""
        if sources is None:
            sources = list(DATA_SOURCES.keys())
        
        self.logger.info(f"Running incremental update for sources: {sources}")
        
        # Filter enabled sources
        sources_to_update = [
            source for source in sources 
            if DATA_SOURCES.get(source, {}).get('enabled', False)
        ]
        
        incremental_results = {
            'update_timestamp': datetime.now().isoformat(),
            'sources_updated': {},
            'errors': []
        }
        
        for source_name in sources_to_update:
            self.logger.info(f"Updating data for: {source_name}")
            
            try:
                # Re-scrape data
                if source_name == 'seedrs':
                    scraper = SeedrsScaper()
                elif source_name == 'fca':
                    scraper = FcaScraper()
                else:
                    continue
                
                # Execute update pipeline for this source
                scraped_data = scraper.scrape()
                data_file = scraper.save_data()
                
                # Re-audit
                audit_result = self.auditor.audit_dataset(data_file, source_name)
                
                # Re-score
                with open(data_file, 'r', encoding='utf-8') as f:
                    raw_data = json.load(f)
                
                trust_score = self.scorer.calculate_trust_score(source_name, audit_result, raw_data)
                
                # Update pipeline results
                self.pipeline_results['scraped_sources'][source_name] = {
                    'status': 'success',
                    'records_count': len(scraped_data),
                    'data_file': data_file,
                    'scraped_at': datetime.now().isoformat()
                }
                
                self.pipeline_results['audit_results'][source_name] = {
                    'status': 'success',
                    'audit_result': audit_result,
                    'audited_at': datetime.now().isoformat()
                }
                
                self.pipeline_results['trust_scores'][source_name] = {
                    'status': 'success',
                    'trust_score': trust_score,
                    'calculated_at': datetime.now().isoformat()
                }
                
                incremental_results['sources_updated'][source_name] = {
                    'status': 'success',
                    'records_count': len(scraped_data),
                    'trust_score': trust_score['overall_trust_score'],
                    'updated_at': datetime.now().isoformat()
                }
                
                self.logger.info(f"Successfully updated {source_name}")
                
            except Exception as e:
                error_msg = f"Failed to update {source_name}: {str(e)}"
                self.logger.error(error_msg)
                
                incremental_results['sources_updated'][source_name] = {
                    'status': 'failed',
                    'error': error_msg,
                    'updated_at': datetime.now().isoformat()
                }
                
                incremental_results['errors'].append({
                    'source': source_name,
                    'error': error_msg,
                    'timestamp': datetime.now().isoformat()
                })
        
        # Regenerate summary
        self._generate_pipeline_summary()
        
        return incremental_results


def main():
    """Main entry point for pipeline execution"""
    print("London Fintech Data Trust Index Pipeline")
    print("=" * 50)
    
    # Initialize and run pipeline
    pipeline = FintechTrustIndexPipeline()
    
    try:
        # Run full pipeline
        results = pipeline.run_full_pipeline()
        
        # Print summary
        print("\nPipeline Execution Summary:")
        print("-" * 30)
        
        summary = results.get('summary', {})
        
        if summary:
            data_collection = summary.get('data_collection', {})
            trust_scoring = summary.get('trust_scoring', {})
            
            print(f"Sources processed: {data_collection.get('sources_attempted', 0)}")
            print(f"Successful collections: {data_collection.get('sources_successful', 0)}")
            print(f"Total records: {data_collection.get('total_records_collected', 0)}")
            print(f"Trust scores calculated: {trust_scoring.get('sources_scored', 0)}")
            
            if trust_scoring.get('average_trust_score', 0) > 0:
                print(f"Average trust score: {trust_scoring['average_trust_score']:.3f}")
        
        errors = results.get('errors', [])
        if errors:
            print(f"\nErrors encountered: {len(errors)}")
            for error in errors[-3:]:  # Show last 3 errors
                print(f"  - {error.get('source', 'Unknown')}: {error.get('error', 'Unknown error')}")
        
        print(f"\nPipeline completed. Check logs for detailed information.")
        
        return results
        
    except KeyboardInterrupt:
        print("\nPipeline execution interrupted by user")
        return None
    except Exception as e:
        print(f"\nPipeline execution failed: {str(e)}")
        return None


if __name__ == "__main__":
    main()