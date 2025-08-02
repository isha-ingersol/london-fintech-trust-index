"""
Environment setup script for London Fintech Trust Index
Initializes directories, checks dependencies, and validates configuration
"""

import os
import sys
import subprocess
import json
from pathlib import Path

def create_directories():
    """Create necessary project directories"""
    directories = [
        'data',
        'outputs', 
        'logs',
        'config',
        'scripts',
        '.streamlit',
        '.github/workflows',
        'src/scrapers',
        'src/auditors', 
        'src/scoring'
    ]
    
    print("Creating project directories...")
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        print(f"✓ Created {directory}")

def check_python_version():
    """Verify Python version compatibility"""
    version = sys.version_info
    if version.major < 3 or (version.major == 3 and version.minor < 8):
        print("❌ Python 3.8+ required")
        sys.exit(1)
    print(f"✓ Python {version.major}.{version.minor}.{version.micro}")

def install_dependencies():
    """Install required Python packages"""
    print("Installing dependencies...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        print("✓ Dependencies installed")
    except subprocess.CalledProcessError:
        print("❌ Failed to install dependencies")
        sys.exit(1)

def create_sample_config():
    """Create sample configuration files"""
    config_data = {
        "data_sources": {
            "seedrs": {
                "enabled": True,
                "base_url": "https://www.seedrs.com",
                "rate_limit": 1.0
            },
            "fca": {
                "enabled": True,
                "base_url": "https://register.fca.org.uk",
                "rate_limit": 0.5
            }
        },
        "scoring": {
            "weights": {
                "data_quality": 0.4,
                "metadata_quality": 0.35,
                "reliability": 0.25
            }
        }
    }
    
    config_path = Path("config/config.json")
    if not config_path.exists():
        with open(config_path, 'w') as f:
            json.dump(config_data, f, indent=2)
        print("✓ Sample configuration created")

def validate_setup():
    """Validate the complete setup"""
    print("\nValidating setup...")
    
    # Check required files
    required_files = [
        "streamlit_app.py",
        "requirements.txt",
        "src/main_pipeline.py"
    ]
    
    for file in required_files:
        if Path(file).exists():
            print(f"✓ {file}")
        else:
            print(f"❌ Missing {file}")
    
    # Test imports
    try:
        import streamlit
        import pandas
        import plotly
        print("✓ Core dependencies importable")
    except ImportError as e:
        print(f"❌ Import error: {e}")

def main():
    """Main setup function"""
    print("🏦 London Fintech Trust Index - Environment Setup")
    print("=" * 50)
    
    check_python_version()
    create_directories()
    create_sample_config()
    install_dependencies()
    validate_setup()
    
    print("\n✅ Setup complete!")
    print("\nNext steps:")
    print("1. Run: python3 src/main_pipeline.py")
    print("2. Launch: streamlit run streamlit_app.py")
    print("3. Deploy to Streamlit Cloud")

if __name__ == "__main__":
    main()