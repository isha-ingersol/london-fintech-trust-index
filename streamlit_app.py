"""
London Fintech Trust Index - Interactive Dashboard
Streamlit application for visualizing fintech data source trustworthiness
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json
import numpy as np
from datetime import datetime, timedelta
import os
from typing import Dict, List, Any
import sys

# Add src to path for imports
sys.path.append('src')

try:
    from scoring.trust_scorer import TrustScorer
    from auditors.metadata_auditor import MetadataAuditor
    from auditors.data_quality import DataQualityChecker
except ImportError:
    st.error("Unable to import required modules. Please ensure all source files are in place.")

# Page configuration
st.set_page_config(
    page_title="London Fintech Trust Index",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded"
)

class DashboardApp:
    """
    Main dashboard application class
    Handles data loading, processing, and visualization
    """
    
    def __init__(self):
        """Initialize dashboard with data loaders and processors"""
        self.trust_scorer = TrustScorer()
        self.metadata_auditor = MetadataAuditor()
        self.data_quality_checker = DataQualityChecker()
        self.trust_scores = {}
        self.audit_results = {}
        
    def load_data(self) -> bool:
        """
        Load trust scores and audit data from output files
        Returns True if data loaded successfully, False otherwise
        """
        try:
            # Load trust scores
            trust_scores_path = 'outputs/trust_scores.json'
            if os.path.exists(trust_scores_path):
                with open(trust_scores_path, 'r') as f:
                    self.trust_scores = json.load(f)
            
            # Load audit results
            audit_results_path = 'outputs/audit_results.json'
            if os.path.exists(audit_results_path):
                with open(audit_results_path, 'r') as f:
                    self.audit_results = json.load(f)
            
            return bool(self.trust_scores or self.audit_results)
            
        except Exception as e:
            st.error(f"Error loading data: {str(e)}")
            return False
    
    def load_sample_data(self):
        """
        Generate sample data for demonstration purposes
        Creates realistic fintech data source examples
        """
        sample_trust_scores = {
            'Seedrs_API': {
                'overall_score': 85.5,
                'trust_grade': 'A',
                'data_quality_score': 88.0,
                'metadata_score': 82.0,
                'reliability_score': 87.0,
                'last_updated': '2024-01-15',
                'source_type': 'Crowdfunding Platform'
            },
            'FCA_Firm_Data': {
                'overall_score': 92.3,
                'trust_grade': 'A',
                'data_quality_score': 95.0,
                'metadata_score': 90.0,
                'reliability_score': 91.0,
                'last_updated': '2024-01-14',
                'source_type': 'Regulatory Data'
            },
            'Open_Banking_UK': {
                'overall_score': 78.2,
                'trust_grade': 'B',
                'data_quality_score': 75.0,
                'metadata_score': 80.0,
                'reliability_score': 80.0,
                'last_updated': '2024-01-10',
                'source_type': 'Banking API'
            },
            'Fintech_Startup_DB': {
                'overall_score': 65.8,
                'trust_grade': 'C',
                'data_quality_score': 68.0,
                'metadata_score': 62.0,
                'reliability_score': 67.0,
                'last_updated': '2024-01-05',
                'source_type': 'Company Database'
            },
            'Alternative_Lending_Data': {
                'overall_score': 45.2,
                'trust_grade': 'F',
                'data_quality_score': 40.0,
                'metadata_score': 48.0,
                'reliability_score': 47.0,
                'last_updated': '2023-12-20',
                'source_type': 'Lending Platform'
            }
        }
        
        self.trust_scores = sample_trust_scores
        
        # Generate corresponding audit results
        sample_audit_results = {}
        for source_name, scores in sample_trust_scores.items():
            sample_audit_results[source_name] = {
                'overall_score': scores['metadata_score'],
                'grade': scores['trust_grade'],
                'scores': {
                    'completeness': np.random.uniform(70, 95),
                    'freshness': np.random.uniform(60, 90),
                    'structure': np.random.uniform(65, 85),
                    'accessibility': np.random.uniform(55, 80),
                    'documentation': np.random.uniform(50, 75)
                },
                'audit_timestamp': datetime.now().isoformat()
            }
        
        self.audit_results = sample_audit_results
    
    def render_header(self):
        """
        Render the main dashboard header with title and key metrics
        Displays overall trust index status and last update time
        """
        st.title("🏦 London Fintech Trust Index")
        st.markdown("### Comprehensive Data Source Trustworthiness Assessment")
        
        if self.trust_scores:
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                total_sources = len(self.trust_scores)
                st.metric("Total Sources", total_sources)
            
            with col2:
                avg_score = np.mean([data['overall_score'] for data in self.trust_scores.values()])
                st.metric("Average Trust Score", f"{avg_score:.1f}")
            
            with col3:
                high_trust_count = sum(1 for data in self.trust_scores.values() if data['overall_score'] >= 80)
                st.metric("High Trust Sources", high_trust_count)
            
            with col4:
                last_update = max([data.get('last_updated', '2024-01-01') for data in self.trust_scores.values()])
                st.metric("Last Updated", last_update)
        
        st.markdown("---")
    
    def render_trust_overview(self):
        """
        Render the main trust scores overview section
        Shows trust score distribution and grade breakdown
        """
        st.header("📊 Trust Scores Overview")
        
        if not self.trust_scores:
            st.warning("No trust score data available.")
            return
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Trust Score Distribution
            scores_df = pd.DataFrame([
                {
                    'Source': source,
                    'Trust Score': data['overall_score'],
                    'Grade': data['trust_grade'],
                    'Source Type': data.get('source_type', 'Unknown')
                }
                for source, data in self.trust_scores.items()
            ])
            
            fig_scatter = px.scatter(
                scores_df,
                x='Source',
                y='Trust Score',
                color='Grade',
                size='Trust Score',
                hover_data=['Source Type'],
                title="Trust Scores by Source",
                color_discrete_map={'A': 'green', 'B': 'yellow', 'C': 'orange', 'D': 'red', 'F': 'darkred'}
            )
            fig_scatter.update_xaxis(tickangle=45)
            st.plotly_chart(fig_scatter, use_container_width=True)
        
        with col2:
            # Grade Distribution
            grade_counts = self._count_sources_by_grade()
            
            fig_pie = px.pie(
                values=list(grade_counts.values()),
                names=list(grade_counts.keys()),
                title="Trust Grade Distribution",
                color_discrete_map={'A': 'green', 'B': 'yellow', 'C': 'orange', 'D': 'red', 'F': 'darkred'}
            )
            st.plotly_chart(fig_pie, use_container_width=True)
    
    def render_detailed_analysis(self):
        """
        Render detailed analysis section with component scores breakdown
        Shows individual metric scores for each data source
        """
        st.header("🔍 Detailed Analysis")
        
        if not self.trust_scores:
            st.warning("No detailed analysis data available.")
            return
        
        # Component scores heatmap
        component_data = []
        for source, data in self.trust_scores.items():
            component_data.append({
                'Source': source,
                'Data Quality': data.get('data_quality_score', 0),
                'Metadata Quality': data.get('metadata_score', 0),
                'Reliability': data.get('reliability_score', 0),
                'Overall': data.get('overall_score', 0)
            })
        
        component_df = pd.DataFrame(component_data)
        component_df_melted = component_df.melt(
            id_vars=['Source'],
            var_name='Component',
            value_name='Score'
        )
        
        fig_heatmap = px.imshow(
            component_df.set_index('Source').values,
            labels=dict(x="Component", y="Source", color="Score"),
            x=component_df.columns[1:],
            y=component_df['Source'],
            title="Component Scores Heatmap",
            color_continuous_scale="RdYlGn"
        )
        st.plotly_chart(fig_heatmap, use_container_width=True)
        
        # Detailed scores table
        st.subheader("Detailed Scores Table")
        st.dataframe(component_df, use_container_width=True)
    
    def render_metadata_analysis(self):
        """
        Render metadata quality analysis section
        Shows metadata audit results and quality indicators
        """
        st.header("📋 Metadata Quality Analysis")
        
        if not self.audit_results:
            st.warning("No metadata audit data available.")
            return
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Metadata component scores
            metadata_components = []
            for source, audit in self.audit_results.items():
                scores = audit.get('scores', {})
                metadata_components.append({
                    'Source': source,
                    'Completeness': scores.get('completeness', 0),
                    'Freshness': scores.get('freshness', 0),
                    'Structure': scores.get('structure', 0),
                    'Accessibility': scores.get('accessibility', 0),
                    'Documentation': scores.get('documentation', 0)
                })
            
            metadata_df = pd.DataFrame(metadata_components)
            metadata_melted = metadata_df.melt(
                id_vars=['Source'],
                var_name='Component',
                value_name='Score'
            )
            
            fig_radar = go.Figure()
            
            for source in metadata_df['Source'].unique():
                source_data = metadata_df[metadata_df['Source'] == source]
                fig_radar.add_trace(go.Scatterpolar(
                    r=source_data.iloc[0, 1:].values,
                    theta=source_data.columns[1:],
                    fill='toself',
                    name=source
                ))
            
            fig_radar.update_layout(
                polar=dict(
                    radialaxis=dict(
                        visible=True,
                        range=[0, 100]
                    )),
                title="Metadata Quality Components",
                showlegend=True
            )
            st.plotly_chart(fig_radar, use_container_width=True)
        
        with col2:
            # Metadata quality trends
            avg_scores = {
                component: np.mean([
                    audit.get('scores', {}).get(component, 0)
                    for audit in self.audit_results.values()
                ])
                for component in ['completeness', 'freshness', 'structure', 'accessibility', 'documentation']
            }
            
            fig_bar = px.bar(
                x=list(avg_scores.keys()),
                y=list(avg_scores.values()),
                title="Average Metadata Scores by Component",
                labels={'x': 'Component', 'y': 'Average Score'}
            )
            fig_bar.update_layout(xaxis_tickangle=45)
            st.plotly_chart(fig_bar, use_container_width=True)
    
    def render_source_details(self):
        """
        Render individual data source details section
        Allows users to drill down into specific source information
        """
        st.header("🔎 Source Details")
        
        if not self.trust_scores:
            st.warning("No source data available.")
            return
        
        # Source selection
        selected_source = st.selectbox(
            "Select a data source for detailed analysis:",
            list(self.trust_scores.keys())
        )
        
        if selected_source:
            source_data = self.trust_scores[selected_source]
            audit_data = self.audit_results.get(selected_source, {})
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader(f"📈 {selected_source} - Trust Metrics")
                
                # Trust score gauge
                fig_gauge = go.Figure(go.Indicator(
                    mode="gauge+number+delta",
                    value=source_data['overall_score'],
                    domain={'x': [0, 1], 'y': [0, 1]},
                    title={'text': "Overall Trust Score"},
                    gauge={
                        'axis': {'range': [None, 100]},
                        'bar': {'color': "darkblue"},
                        'steps': [
                            {'range': [0, 50], 'color': "lightgray"},
                            {'range': [50, 80], 'color': "gray"}
                        ],
                        'threshold': {
                            'line': {'color': "red", 'width': 4},
                            'thickness': 0.75,
                            'value': 90
                        }
                    }
                ))
                st.plotly_chart(fig_gauge, use_container_width=True)
                
                # Component breakdown
                st.write("**Component Scores:**")
                components = {
                    'Data Quality': source_data.get('data_quality_score', 0),
                    'Metadata Quality': source_data.get('metadata_score', 0),
                    'Reliability': source_data.get('reliability_score', 0)
                }
                
                for component, score in components.items():
                    st.metric(component, f"{score:.1f}")
            
            with col2:
                st.subheader(f"📋 {selected_source} - Metadata Analysis")
                
                if audit_data:
                    st.write(f"**Overall Grade:** {audit_data.get('grade', 'N/A')}")
                    st.write(f"**Audit Date:** {audit_data.get('audit_timestamp', 'N/A')[:10]}")
                    
                    # Metadata component scores
                    metadata_scores = audit_data.get('scores', {})
                    if metadata_scores:
                        metadata_df = pd.DataFrame([metadata_scores])
                        fig_meta_bar = px.bar(
                            x=list(metadata_scores.keys()),
                            y=list(metadata_scores.values()),
                            title="Metadata Component Scores",
                            labels={'x': 'Component', 'y': 'Score'}
                        )
                        fig_meta_bar.update_layout(xaxis_tickangle=45)
                        st.plotly_chart(fig_meta_bar, use_container_width=True)
                else:
                    st.info("No metadata audit data available for this source.")
    
    def render_filters_sidebar(self):
        """
        Render the sidebar with filtering options
        Allows users to filter data by various criteria
        """
        st.sidebar.header("🎛️ Filters & Options")
        
        if self.trust_scores:
            # Trust score range filter
            min_score = st.sidebar.slider(
                "Minimum Trust Score",
                min_value=0.0,
                max_value=100.0,
                value=0.0,
                step=5.0
            )
            
            # Grade filter
            available_grades = list(set(data['trust_grade'] for data in self.trust_scores.values()))
            selected_grades = st.sidebar.multiselect(
                "Trust Grades",
                available_grades,
                default=available_grades
            )
            
            # Source type filter
            available_types = list(set(data.get('source_type', 'Unknown') for data in self.trust_scores.values()))
            selected_types = st.sidebar.multiselect(
                "Source Types",
                available_types,
                default=available_types
            )
            
            # Apply filters to data
            self._apply_filters(min_score, selected_grades, selected_types)
        
        # Dashboard options
        st.sidebar.header("📊 Dashboard Options")
        
        show_sample_data = st.sidebar.checkbox(
            "Use Sample Data",
            value=not bool(self.trust_scores),
            help="Use sample data for demonstration"
        )
        
        if show_sample_data:
            self.load_sample_data()
        
        auto_refresh = st.sidebar.checkbox(
            "Auto Refresh",
            value=False,
            help="Automatically refresh data every 5 minutes"
        )
        
        if auto_refresh:
            st.sidebar.info("Auto refresh enabled")
    
    def render_export_options(self):
        """
        Render data export options section
        Allows users to download reports and data
        """
        st.sidebar.header("📥 Export Options")
        
        if self.trust_scores:
            # Export trust scores as CSV
            trust_df = pd.DataFrame([
                {
                    'Source': source,
                    'Overall Score': data['overall_score'],
                    'Trust Grade': data['trust_grade'],
                    'Data Quality': data.get('data_quality_score', 0),
                    'Metadata Score': data.get('metadata_score', 0),
                    'Reliability': data.get('reliability_score', 0),
                    'Source Type': data.get('source_type', 'Unknown'),
                    'Last Updated': data.get('last_updated', 'N/A')
                }
                for source, data in self.trust_scores.items()
            ])
            
            csv_data = trust_df.to_csv(index=False)
            st.sidebar.download_button(
                label="📊 Download Trust Scores CSV",
                data=csv_data,
                file_name=f"trust_scores_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )
            
            # Export summary report
            summary_report = self._generate_summary_report()
            st.sidebar.download_button(
                label="📋 Download Summary Report",
                data=summary_report,
                file_name=f"trust_index_report_{datetime.now().strftime('%Y%m%d')}.txt",
                mime="text/plain"
            )
    
    def render_about_section(self):
        """
        Render the about section with project information
        Provides context and methodology explanation
        """
        with st.expander("ℹ️ About the London Fintech Trust Index"):
            st.markdown("""
            ### What is the Trust Index?
            
            The London Fintech Trust Index is a comprehensive assessment tool that evaluates 
            the trustworthiness and reliability of fintech data sources across London's 
            financial technology ecosystem.
            
            ### Methodology
            
            Our scoring system evaluates three key dimensions:
            
            **🔍 Data Quality (40%)**
            - Data completeness and accuracy
            - Consistency and validation
            - Error rates and anomalies
            
            **📋 Metadata Quality (35%)**
            - Information completeness
            - Documentation quality
            - Update frequency and freshness
            
            **🔧 Reliability (25%)**
            - API uptime and availability
            - Response times and performance
            - Historical consistency
            
            ### Grading Scale
            
            - **A (90-100)**: Excellent trustworthiness
            - **B (80-89)**: Good trustworthiness
            - **C (70-79)**: Fair trustworthiness
            - **D (60-69)**: Poor trustworthiness
            - **F (0-59)**: Untrustworthy
            
            ### Data Sources
            
            Currently monitoring:
            - Regulatory data (FCA, PRA)
            - Crowdfunding platforms
            - Open Banking APIs
            - Fintech company databases
            - Alternative lending platforms
            """)
    
    def _count_sources_by_grade(self) -> Dict[str, int]:
        """
        Count sources by trust grade
        Returns dictionary mapping grades to counts
        """
        grade_counts = {}
        for data in self.trust_scores.values():
            grade = data['trust_grade']
            grade_counts[grade] = grade_counts.get(grade, 0) + 1
        return grade_counts
    
    def _apply_filters(self, min_score: float, selected_grades: List[str], selected_types: List[str]):
        """
        Apply user-selected filters to the trust scores data
        Modifies the displayed data based on filter criteria
        """
        if not self.trust_scores:
            return
        
        # Store original data if not already stored
        if not hasattr(self, '_original_trust_scores'):
            self._original_trust_scores = self.trust_scores.copy()
        
        # Reset to original data
        self.trust_scores = self._original_trust_scores.copy()
        
        # Apply filters
        filtered_scores = {}
        for source, data in self.trust_scores.items():
            # Check score threshold
            if data['overall_score'] < min_score:
                continue
            
            # Check grade filter
            if data['trust_grade'] not in selected_grades:
                continue
            
            # Check source type filter
            if data.get('source_type', 'Unknown') not in selected_types:
                continue
            
            filtered_scores[source] = data
        
        self.trust_scores = filtered_scores
    
    def _generate_summary_report(self) -> str:
        """
        Generate a text summary report of the trust index findings
        Returns formatted string report for download
        """
        if not self.trust_scores:
            return "No data available for report generation."
        
        # Calculate summary statistics
        scores = [data['overall_score'] for data in self.trust_scores.values()]
        avg_score = np.mean(scores)
        median_score = np.median(scores)
        
        grade_counts = self._count_sources_by_grade()
        
        report = f"""
LONDON FINTECH TRUST INDEX - SUMMARY REPORT
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
============================================

OVERVIEW
--------
Total Data Sources Analyzed: {len(self.trust_scores)}
Average Trust Score: {avg_score:.2f}
Median Trust Score: {median_score:.2f}
Highest Score: {max(scores):.2f}
Lowest Score: {min(scores):.2f}

GRADE DISTRIBUTION
------------------
"""
        
        for grade in ['A', 'B', 'C', 'D', 'F']:
            count = grade_counts.get(grade, 0)
            percentage = (count / len(self.trust_scores)) * 100 if self.trust_scores else 0
            report += f"Grade {grade}: {count} sources ({percentage:.1f}%)\n"
        
        report += f"""
TOP PERFORMING SOURCES
----------------------
"""
        
        # Sort by score and get top 5
        sorted_sources = sorted(
            self.trust_scores.items(),
            key=lambda x: x[1]['overall_score'],
            reverse=True
        )
        
        for i, (source, data) in enumerate(sorted_sources[:5], 1):
            report += f"{i}. {source}: {data['overall_score']:.1f} (Grade {data['trust_grade']})\n"
        
        report += f"""
SOURCES NEEDING ATTENTION
-------------------------
"""
        
        low_performers = [
            (source, data) for source, data in sorted_sources
            if data['overall_score'] < 60
        ]
        
        if low_performers:
            for source, data in low_performers:
                report += f"- {source}: {data['overall_score']:.1f} (Grade {data['trust_grade']})\n"
        else:
            report += "No sources currently require immediate attention.\n"
        
        report += f"""
RECOMMENDATIONS
---------------
"""
        
        if avg_score < 70:
            report += "- Overall trust level is below acceptable threshold\n"
            report += "- Focus on improving data quality and documentation\n"
        
        if grade_counts.get('F', 0) > 0:
            report += f"- {grade_counts['F']} sources are untrustworthy and should be reviewed\n"
        
        if grade_counts.get('A', 0) < len(self.trust_scores) * 0.3:
            report += "- Less than 30% of sources achieve excellent trust rating\n"
            report += "- Consider implementing data quality improvement programs\n"
        
        return report
    
    def run(self):
        """
        Main application runner
        Orchestrates the entire dashboard rendering process
        """
        # Load data
        if not self.load_data():
            st.info("No data files found. Loading sample data for demonstration.")
            self.load_sample_data()
        
        # Render sidebar filters first
        self.render_filters_sidebar()
        self.render_export_options()
        
        # Render main content
        self.render_header()
        self.render_trust_overview()
        self.render_detailed_analysis()
        self.render_metadata_analysis()
        self.render_source_details()
        self.render_about_section()
        
        # Footer
        st.markdown("---")
        st.markdown(
            "**London Fintech Trust Index** | "
            "Built with Streamlit | "
            f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )


def main():
    """
    Application entry point
    Creates and runs the dashboard application
    """
    # Create and run the dashboard
    app = DashboardApp()
    app.run()


if __name__ == "__main__":
    main()