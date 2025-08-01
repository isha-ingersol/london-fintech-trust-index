# 🏦 London Fintech Trust Index

A comprehensive automated system for assessing the trustworthiness of London fintech data sources through data quality auditing, metadata analysis, and interactive visualization.

![Dashboard Preview](assets/dashboard_preview.png)

## 🚀 Features

- **Automated Data Collection**: Scrapes multiple fintech data sources including regulatory data, crowdfunding platforms, and API documentation
- **Comprehensive Quality Assessment**: Evaluates data quality, completeness, metadata, and reliability
- **Interactive Dashboard**: Real-time visualization with filtering, drill-down capabilities, and export options
- **Trust Scoring System**: Interpretable A-F grading system based on multiple quality dimensions
- **Automated Deployment**: Self-updating pipeline with GitHub Actions integration
- **Professional Reporting**: Downloadable CSV exports and summary reports

## 📊 Live Dashboard

Access the live dashboard: [London Fintech Trust Index](https://your-app-url.streamlit.app)

## 🏗️ Architecture

┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Data Sources  │───▶│  Web Scrapers    │───▶│  Data Storage   │
│                 │    │                  │    │                 │
│ • FCA Registry  │    │ • Base Scraper   │    │ • Raw Data      │
│ • Seedrs API    │    │ • Seedrs Scraper │    │ • Processed     │
│ • Open Banking  │    │ • FCA Scraper    │    │ • Audit Results │
│ • Company APIs  │    │ • Custom Sources │    │ • Trust Scores  │
└─────────────────┘    └──────────────────┘    └─────────────────┘
│
▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Dashboard     │◀───│  Scoring Engine  │◀───│  Quality Audit  │
│                 │    │                  │    │                 │
│ • Streamlit App │    │ • Trust Scorer   │    │ • Data Quality  │
│ • Visualizations│    │ • Weighted Calc  │    │ • Metadata Audit│
│ • Export Tools  │    │ • Grade Assignment│   │ • Reliability   │
│ • Filters       │    │ • Trend Analysis │    │ • Completeness  │
└─────────────────┘    └──────────────────┘    └─────────────────┘

## 🛠️ Installation

### Prerequisites

- Python 3.8+
- pip package manager
- Git

### Quick Start

1. **Clone the repository**
   ```bash
   git clone https://github.com/isha-ingersol/london-fintech-trust-index.git
   cd london-fintech-trust-index

2. **Set up virtual environment**
    ```bash
    python -m venv venv

    # Windows
    venv\Scripts\activate

    # macOS/Linux
    source venv/bin/activate

3. **Install dependencies**
    ```bash
    pip install -r requirements.txt

4. **Run setup script**
    ```bash
    python scripts/setup_environment.py

5. **Initialize data collection**
    ```bash
    python src/main_pipeline.py

6. **Launch dashboard**
    ```bash
    streamlit run streamlit_app.py

7. **Visit http://localhost:8501 to access the dashboard.**

## 📋 Usage

### Data Collection
The system automatically collects data from configured sources:

#Run complete data pipeline
   ```bash
   python src/main_pipeline.py

#Run specific scrapers
   ```bash
   python -m src.scrapers.seedrs_scraper
   python -m src.scrapers.fca_scraper

### Trust Score Analysis
   ```bash
   from src.scoring.trust_scorer import TrustScorer
   from src.auditors.data_quality import DataQualityChecker

#Initialize scoring system
   ```bash
   scorer = TrustScorer()
   quality_checker = DataQualityChecker()

#Calculate trust scores
   ```bash
   trust_scores = scorer.calculate_trust_scores(data_sources)

### Dashboard Customization
The Streamlit dashboard supports various customization options:
1. Filters: Score thresholds, grade ranges, source types
2. Visualizations: Charts, heatmaps, detailed breakdowns
3. Export: CSV downloads, summary reports
4. Real-time Updates: Automatic data refresh
