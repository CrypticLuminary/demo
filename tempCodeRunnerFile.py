import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import json
import csv
import yaml
import logging
import time
import random
import os
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin, urlparse
from typing import Dict, List, Optional, Any
import hashlib
from dotenv import load_dotenv
from datetime import datetime, timezone
class MultiWebsiteScraper:
    """
    🎯 Advanced multi-website scraper with Beautiful Soup
    """
    
    def __init__(self, config_file: str = "config.yaml"):
        """Initialize the scraper with configuration"""
        self.config_file = config_file
        self.config = self.load_config()
        self.setup_logging()
        self.setup_directories()
        self.session = self.create_session()
        self.scraped_data = {}
        
        # Load environment variables
        load_dotenv()
        
        self.logger.info("🚀 Multi-Website Scraper initialized")
        self.logger.info(f"⏰ Started at: {datetime.now(timezone.utc).isoformat()} UTC")
    
    def load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        try:
            with open(self.config_file, 'r', encoding='utf-8') as file:
                config = yaml.safe_load(file)
            return config
        except FileNotFoundError:
            print(f"❌ Config file {self.config_file} not found!")
            return self.get_default_config()
        except Exception as e:
            print(f"❌ Error loading config: {e}")
            return self.get_default_config()
    
    def get_default_config(self) -> Dict[str, Any]:
        """Fallback default configuration"""
        return {
            'scraper': {
                'max_workers': 3,
                'default_delay': 2,
                'timeout': 30,
                'retries': 3
            },
            'websites': {},
            'storage': {
                'formats': ['json'],
                'output_directory': 'scraped_data'
            },
            'logging': {
                'level': 'INFO',
                'console': True
            }
        }
    
    def setup_logging(self):
        """Setup logging configuration"""
        log_config = self.config.get('logging', {})
        log_level = getattr(logging, log_config.get('level', 'INFO'))
        
        # Create logger
        self.logger = logging.getLogger('MultiWebsiteScraper')
        self.logger.setLevel(log_level)
        
        # Clear existing handlers
        self.logger.handlers.clear()
        
        # Console handler
        if log_config.get('console', True):
            console_handler = logging.StreamHandler()
            console_formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            console_handler.setFormatter(console_formatter)
            self.logger.addHandler(console_handler)
        
        # File handler
        if log_config.get('file'):
            file_handler = logging.FileHandler(log_config['file'])
            file_formatter = logging.Formatter(
                log_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            )
            file_handler.setFormatter(file_formatter)
            self.logger.addHandler(file_handler)
    
    def setup_directories(self):
        """Create necessary directories"""
        output_dir = self.config.get('storage', {}).get('output_directory', 'scraped_data')
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        self.output_dir = output_dir
    
    def create_session(self) -> requests.Session:
        """Create configured requests session"""
        session = requests.Session()
        
        # Set default headers
        user_agents = self.config.get('scraper', {}).get('user_agents', [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        ])
        
        session.headers.update({
            'User-Agent': random.choice(user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        })
        
        return session
    
    def get_page_content(self, url: str, retries: int = None) -> Optional[BeautifulSoup]:
        """
        Fetch and parse page content with Beautiful Soup
        """
        if retries is None:
            retries = self.config.get('scraper', {}).get('retries', 3)
        
        timeout = self.config.get('scraper', {}).get('timeout', 30)
        
        for attempt in range(retries + 1):
            try:
                self.logger.info(f"🌐 Fetching: {url} (attempt {attempt + 1})")
                
                response = self.session.get(url, timeout=timeout)
                response.raise_for_status()
                
                # Parse with Beautiful Soup
                soup = BeautifulSoup(response.content, 'html.parser')
                
                self.logger.info(f"✅ Successfully fetched and parsed: {url}")
                return soup
                
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"⚠️ Attempt {attempt + 1} failed for {url}: {e}")
                if attempt < retries:
                    wait_time = (attempt + 1) * 2
                    self.logger.info(f"⏳ Waiting {wait_time} seconds before retry...")
                    time.sleep(wait_time)
                else:
                    self.logger.error(f"❌ All attempts failed for {url}")
                    return None
        
        return None
    
    def extract_data_from_soup(self, soup: BeautifulSoup, selectors: Dict[str, str], website_name: str) -> List[Dict[str, Any]]:
        """
        Extract data from Beautiful Soup object using CSS selectors
        """
        extracted_data = []
        
        try:
            # Find main container elements
            container_selector = selectors.get('quote_container') or selectors.get('book_container') or selectors.get('data_container')
            
            if container_selector:
                containers = soup.select(container_selector)
                self.logger.info(f"📦 Found {len(containers)} items in {website_name}")
                
                for i, container in enumerate(containers):
                    item_data = {
                        'website': website_name,
                        'item_id': i + 1,
                        'scraped_at': datetime.now(timezone.utc).isoformat() + 'Z'
                    }
                    
                    # Extract data using selectors
                    for field, selector in selectors.items():
                        if field.endswith('_container'):
                            continue
                            
                        try:
                            elements = container.select(selector)
                            if elements:
                                if field == 'tags':
                                    # Handle multiple tags
                                    item_data[field] = [tag.get_text(strip=True) for tag in elements]
                                else:
                                    # Single value
                                    item_data[field] = elements[0].get_text(strip=True)
                            else:
                                item_data[field] = None
                                
                        except Exception as e:
                            self.logger.warning(f"⚠️ Error extracting {field}: {e}")
                            item_data[field] = None
                    
                    extracted_data.append(item_data)
            
            else:
                # Extract general page data if no container specified
                item_data = {
                    'website': website_name,
                    'content': soup.get_text(strip=True)[:1000],  # First 1000 chars
                    'scraped_at': datetime.now(timezone.utc).isoformat() + 'Z'
                }
                extracted_data.append(item_data)
                
        except Exception as e:
            self.logger.error(f"❌ Error extracting data from {website_name}: {e}")
        
        return extracted_data
    
    def scrape_website(self, website_name: str, website_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Scrape a single website based on its configuration
        """
        if not website_config.get('enabled', True):
            self.logger.info(f"⏭️ Skipping disabled website: {website_name}")
            return []
        
        self.logger.info(f"🎯 Starting to scrape: {website_config.get('name', website_name)}")
        
        base_url = website_config['base_url']
        pages = website_config.get('pages', ['/'])
        selectors = website_config.get('selectors', {})
        delay = website_config.get('delay', self.config.get('scraper', {}).get('default_delay', 2))
        
        all_data = []
        
        for page in pages:
            # Construct full URL
            full_url = urljoin(base_url, page)
            
            # Get page content
            soup = self.get_page_content(full_url)
            
            if soup:
                # Extract data
                page_data = self.extract_data_from_soup(soup, selectors, website_name)
                all_data.extend(page_data)
                
                self.logger.info(f"📊 Extracted {len(page_data)} items from {full_url}")
            
            # Respectful delay
            if delay > 0:
                self.logger.info(f"⏳ Waiting {delay} seconds...")
                time.sleep(delay)
        
        self.logger.info(f"✅ Completed scraping {website_name}: {len(all_data)} total items")
        return all_data
    
    def scrape_all_websites(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Scrape all configured websites
        """
        websites = self.config.get('websites', {})
        max_workers = self.config.get('scraper', {}).get('max_workers', 3)
        
        self.logger.info(f"🚀 Starting to scrape {len(websites)} websites with {max_workers} workers")
        
        all_scraped_data = {}
        
        # Use ThreadPoolExecutor for concurrent scraping
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit scraping tasks
            future_to_website = {
                executor.submit(self.scrape_website, name, config): name
                for name, config in websites.items()
            }
            
            # Collect results
            for future in as_completed(future_to_website):
                website_name = future_to_website[future]
                try:
                    data = future.result()
                    all_scraped_data[website_name] = data
                    self.logger.info(f"✅ Completed {website_name}")
                except Exception as e:
                    self.logger.error(f"❌ Error scraping {website_name}: {e}")
                    all_scraped_data[website_name] = []
        
        self.scraped_data = all_scraped_data
        return all_scraped_data
    
    def save_data(self, data: Dict[str, List[Dict[str, Any]]]):
        """
        Save scraped data in multiple formats
        """
        formats = self.config.get('storage', {}).get('formats', ['json'])
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
        
        self.logger.info(f"💾 Saving data in formats: {formats}")
        
        for fmt in formats:
            if fmt == 'json':
                self.save_json(data, timestamp)
            elif fmt == 'csv':
                self.save_csv(data, timestamp)
            elif fmt == 'sqlite':
                self.save_sqlite(data, timestamp)
            else:
                self.logger.warning(f"⚠️ Unknown format: {fmt}")
    
    def save_json(self, data: Dict[str, List[Dict[str, Any]]], timestamp: str):
        """Save data as JSON"""
        filename = f"{self.output_dir}/scraped_data_{timestamp}.json"
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            self.logger.info(f"💾 Saved JSON: {filename}")
        except Exception as e:
            self.logger.error(f"❌ Error saving JSON: {e}")
    
    def save_csv(self, data: Dict[str, List[Dict[str, Any]]], timestamp: str):
        """Save data as CSV files (one per website)"""
        for website_name, website_data in data.items():
            if website_data:
                filename = f"{self.output_dir}/scraped_{website_name}_{timestamp}.csv"
                try:
                    df = pd.DataFrame(website_data)
                    df.to_csv(filename, index=False, encoding='utf-8')
                    self.logger.info(f"💾 Saved CSV: {filename}")
                except Exception as e:
                    self.logger.error(f"❌ Error saving CSV for {website_name}: {e}")
    
    def save_sqlite(self, data: Dict[str, List[Dict[str, Any]]], timestamp: str):
        """Save data to SQLite database"""
        db_filename = f"{self.output_dir}/scraped_data_{timestamp}.db"
        try:
            conn = sqlite3.connect(db_filename)
            for website_name, website_data in data.items():
                if website_data:
                    # Convert lists to strings for compatibility
                    for item in website_data:
                        for key, value in item.items():
                            if isinstance(value, list):
                                # Option 1: comma separated
                                item[key] = ', '.join(str(v) for v in value)
                                # Option 2: JSON string (uncomment if preferred)
                                # import json
                                # item[key] = json.dumps(value)

                    df = pd.DataFrame(website_data)
                    df.to_sql(website_name, conn, if_exists='replace', index=False)
            conn.close()
            self.logger.info(f"💾 Saved SQLite: {db_filename}")
        except Exception as e:
            self.logger.error(f"❌ Error saving SQLite: {e}")
    
    def generate_report(self) -> Dict[str, Any]:
        """Generate scraping summary report"""
        report = {
            'timestamp': datetime.now(timezone.utc).isoformat() + 'Z',
            'user': 'CrypticLuminary',
            'total_websites': len(self.scraped_data),
            'websites': {},
            'total_items': 0
        }
        
        for website_name, data in self.scraped_data.items():
            item_count = len(data)
            report['websites'][website_name] = {
                'items_scraped': item_count,
                'status': 'success' if item_count > 0 else 'no_data'
            }
            report['total_items'] += item_count
        
        return report
    
    def run(self):
        """
        Main execution method
        """
        start_time = time.time()
        
        self.logger.info("=" * 60)
        self.logger.info("🕷️ MULTI-WEBSITE SCRAPER STARTING")
        self.logger.info("=" * 60)
        self.logger.info(f"👤 User: CrypticLuminary")
        self.logger.info(f"⏰ Start Time: {datetime.now(timezone.utc).isoformat()} UTC")
        
        try:
            # Scrape all websites
            scraped_data = self.scrape_all_websites()
            
            # Save data
            if scraped_data:
                self.save_data(scraped_data)
            
            # Generate report
            report = self.generate_report()
            
            # Save report
            report_filename = f"{self.output_dir}/scraping_report_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
            with open(report_filename, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2)
            
            end_time = time.time()
            duration = end_time - start_time
            
            self.logger.info("=" * 60)
            self.logger.info("🎉 SCRAPING COMPLETED SUCCESSFULLY!")
            self.logger.info("=" * 60)
            self.logger.info(f"⏱️ Duration: {duration:.2f} seconds")
            self.logger.info(f"🌐 Websites scraped: {report['total_websites']}")
            self.logger.info(f"📊 Total items: {report['total_items']}")
            self.logger.info(f"📋 Report saved: {report_filename}")
            
        except Exception as e:
            self.logger.error(f"❌ Scraping failed: {e}")
            raise


def main():
    """Main function to run the scraper"""
    print("🕷️ Multi-Website Beautiful Soup Scraper")
    print("=" * 50)
    print(f"👤 User: CrypticLuminary")
    print(f"⏰ Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print("=" * 50)
    
    # Initialize and run scraper
    scraper = MultiWebsiteScraper()
    scraper.run()


if __name__ == "__main__":
    main()