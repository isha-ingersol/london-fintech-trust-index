"""
Configuration file for London Fintech Data Trust Index
Contains all settings, URLs, and parameters for the project
"""

import os
from datetime import datetime

# Project Configuration
PROJECT_NAME = "London Fintech Data Trust Index"
VERSION = "1.0.0"
AUTHOR = "Your Name"

# Data Source URLs and Configuration
DATA_SOURCES = {
    "seedrs": {
        "name": "Seedrs Equity Crowdfunding",
        "base_url": "https://www.seedrs.com",
        "campaigns_url": "https://www.seedrs.com/discover/campaigns",
        "priority": 1,
        "enabled": True,
        "rate_limit": 2  # seconds between requests
    },
    "fca": {
        "name": "FCA Financial Services Register",
        "base_url": "https://register.fca.org.uk",
        "api_url": "https://register.fca.org.uk/services/V0.1/Firm",
        "priority": 2,
        "enabled": True,
        "rate_limit": 1
    },
    "open_banking": {
        "name": "Open Banking UK",
        "base_url": "https://openbanking.org.uk",
        "docs_url": "https://openbankinguk.github.io/read-write-api-site3/",
        "priority": 3,
        "enabled": True,
        "rate_limit": 1
    }
}

# Trust Index Scoring Weights
SCORING_WEIGHTS = {
    "data_completeness": 0.25,      # 25% - How complete is the data
    "metadata_quality": 0.20,       # 20% - Quality of documentation/metadata
    "regulatory_compliance": 0.25,   # 25% - Compliance indicators
    "transparency": 0.15,           # 15% - Update frequency, changelogs
    "accessibility": 0.15           # 15% - Ease of access, API quality
}

# Data Quality Thresholds
QUALITY_THRESHOLDS = {
    "completeness_excellent": 0.95,    # 95%+ completeness = excellent
    "completeness_good": 0.80,         # 80%+ completeness = good
    "completeness_poor": 0.50,         # <50% completeness = poor
    "metadata_fields_min": 5,          # Minimum metadata fields expected
    "update_frequency_days": 30        # Expected update frequency
}

# File Paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
RAW_DATA_DIR = os.path.join(DATA_DIR, "raw")
PROCESSED_DATA_DIR = os.path.join(DATA_DIR, "processed")
OUTPUTS_DIR = os.path.join(BASE_DIR, "outputs")
LOGS_DIR = os.path.join(BASE_DIR, "logs")

# Ensure directories exist
for directory in [DATA_DIR, RAW_DATA_DIR, PROCESSED_DATA_DIR, OUTPUTS_DIR, LOGS_DIR]:
    os.makedirs(directory, exist_ok=True)

# Logging Configuration
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_FILE = os.path.join(LOGS_DIR, f"trust_index_{datetime.now().strftime('%Y%m%d')}.log")

# Request Headers for Web Scraping
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
}

# Dashboard Configuration
DASHBOARD_CONFIG = {
    "title": "London Fintech Data Trust Index",
    "page_icon": "🏦",
    "layout": "wide",
    "initial_sidebar_state": "expanded"
}

# Color scheme for visualizations
COLORS = {
    "excellent": "#2E8B57",    # Sea Green
    "good": "#32CD32",         # Lime Green  
    "fair": "#FFD700",         # Gold
    "poor": "#FF6347",         # Tomato
    "primary": "#1f77b4",      # Blue
    "secondary": "#ff7f0e"     # Orange
}

# API Configuration (if needed for future enhancements)
API_CONFIG = {
    "timeout": 30,
    "max_retries": 3,
    "backoff_factor": 0.3
}