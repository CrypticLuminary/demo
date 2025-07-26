import json
import pandas as pd
import sqlite3
from pathlib import Path
from typing import Dict, List, Any
import matplotlib.pyplot as plt
import seaborn as sns

class ScraperAnalyzer:
    """Analyze and visualize scraped data"""
    
    def __init__(self, data_directory: str = "scraped_data"):
        self.data_dir = Path(data_directory)
    
    def load_latest_data(self) -> Dict[str, Any]:
        """Load the most recent scraped data"""
        json_files = list(self.data_dir.glob("scraped_data_*.json"))
        if not json_files:
            return {}
        
        latest_file = max(json_files, key=lambda f: f.stat().st_mtime)
        with open(latest_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def analyze_data(self, data: Dict[str, List[Dict[str, Any]]]):
        """Generate analysis of scraped data"""
        analysis = {
            'summary': {},
            'details': {}
        }
        
        for website, items in data.items():
            if items:
                df = pd.DataFrame(items)
                analysis['details'][website] = {
                    'item_count': len(items),
                    'columns': list(df.columns),
                    'data_types': df.dtypes.to_dict(),
                    'sample_data': items[:3]  # First 3 items
                }
        
        return analysis
    
    def export_to_excel(self, data: Dict[str, List[Dict[str, Any]]], filename: str = None):
        """Export all data to Excel with multiple sheets"""
        if filename is None:
            filename = f"scraped_data_export_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        with pd.ExcelWriter(self.data_dir / filename, engine='openpyxl') as writer:
            for website, items in data.items():
                if items:
                    df = pd.DataFrame(items)
                    sheet_name = website[:31]  # Excel sheet name limit
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        print(f"📊 Exported to Excel: {filename}")

def validate_config(config_file: str = "config.yaml") -> bool:
    """Validate configuration file"""
    try:
        import yaml
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
        
        required_sections = ['scraper', 'websites', 'storage']
        for section in required_sections:
            if section not in config:
                print(f"❌ Missing required section: {section}")
                return False
        
        print("✅ Configuration is valid")
        return True
        
    except Exception as e:
        print(f"❌ Configuration validation failed: {e}")
        return False

def setup_project():
    """Setup project structure and files"""
    print("🔧 Setting up Multi-Website Scraper project...")
    
    # Create directories
    directories = ["scraped_data", "logs", "config"]
    for directory in directories:
        Path(directory).mkdir(exist_ok=True)
        print(f"📁 Created directory: {directory}")
    
    print("✅ Project setup complete!")

if __name__ == "__main__":
    setup_project()