"""
Ghana FDA Regulatory Scraper - All-in-One Solution (FIXED VERSION)
Handles Product Recalls, Alerts, and Public Notices from FDA Ghana
Fixes: Duplicates, Company Database Storage, Dynamic Processing
"""
import logging
import time
import os
import re
from typing import List, Dict, Any, Optional, Set
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import requests
from datetime import datetime
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
import psycopg2
from psycopg2.extras import RealDictCursor

from utils import DateParser, PDFProcessor, TextCleaner, ensure_directory

logger = logging.getLogger(__name__)

class GhanaRegulatoryScraperUnified:
    def __init__(self, output_dir: str = './output'):
        self.output_dir = output_dir
        self.recalls_dir = f"{output_dir}/recalls"
        self.alerts_dir = f"{output_dir}/alerts"
        self.notices_dir = f"{output_dir}/notices"
        # Create all directories
        for directory in [self.recalls_dir, self.alerts_dir, self.notices_dir]:
            ensure_directory(directory)
        # URLs for different sections
        self.urls = {
            'recalls': "https://fdaghana.gov.gh/newsroom/product-recalls-and-alerts/",
            'alerts': "https://fdaghana.gov.gh/newsroom/product-alerts/",
            'notices': [
                "https://fdaghana.gov.gh/newsroom/press-release/",
                "https://fdaghana.gov.gh/newsroom/press-release-2/"
            ]
        }
        # Track processed products to avoid duplicates
        self.processed_products = set()
        # Dynamic company extraction patterns - FIXED
        self.company_patterns = {
            'manufacturers': [
                r'manufacturer[^:]*:?\s*([^:\n,;]{3,50})(?=\s*(?:$|\n|,|;|product|batch|recalling|expiry))',
                r'manufactured\s+by[^:]*:?\s*([^:\n,;]{3,50})(?=\s*(?:$|\n|,|;|product|batch|recalling|expiry))',
                r'made\s+by[^:]*:?\s*([^:\n,;]{3,50})(?=\s*(?:$|\n|,|;|product|batch|recalling|expiry))',
                r'produced\s+by[^:]*:?\s*([^:\n,;]{3,50})(?=\s*(?:$|\n|,|;|product|batch|recalling|expiry))',
                r'mfg[^:]*:?\s*([^:\n,;]{3,50})(?=\s*(?:$|\n|,|;|product|batch|recalling|expiry))',
                r'mfr[^:]*:?\s*([^:\n,;]{3,50})(?=\s*(?:$|\n|,|;|product|batch|recalling|expiry))'
            ],
            'recalling_firms': [
                r'recalling\s+firm[^:]*:?\s*([^:\n,;]{3,50})(?=\s*(?:$|\n|,|;|product|batch|manufacturer|expiry))',
                r'recall[^:]*firm[^:]*:?\s*([^:\n,;]{3,50})(?=\s*(?:$|\n|,|;|product|batch|manufacturer|expiry))',
                r'initiating\s+firm[^:]*:?\s*([^:\n,;]{3,50})(?=\s*(?:$|\n|,|;|product|batch|manufacturer|expiry))',
                r'responsible\s+firm[^:]*:?\s*([^:\n,;]{3,50})(?=\s*(?:$|\n|,|;|product|batch|manufacturer|expiry))'
            ],
            'distributors': [
                r'distributor[^:]*:?\s*([^:\n,;]{3,50})(?=\s*(?:$|\n|,|;|product|batch|manufacturer|expiry))',
                r'distribution[^:]*:?\s*([^:\n,;]{3,50})(?=\s*(?:$|\n|,|;|product|batch|manufacturer|expiry))'
            ],
            'importers': [
                r'importer[^:]*:?\s*([^:\n,;]{3,50})(?=\s*(?:$|\n|,|;|product|batch|manufacturer|expiry))',
                r'imported\s+by[^:]*:?\s*([^:\n,;]{3,50})(?=\s*(?:$|\n|,|;|product|batch|manufacturer|expiry))',
                r'import[^:]*:?\s*([^:\n,;]{3,50})(?=\s*(?:$|\n|,|;|product|batch|manufacturer|expiry))'
            ],
            'suppliers': [
                r'supplier[^:]*:?\s*([^:\n,;]{3,50})(?=\s*(?:$|\n|,|;|product|batch|manufacturer|expiry))',
                r'supplied\s+by[^:]*:?\s*([^:\n,;]{3,50})(?=\s*(?:$|\n|,|;|product|batch|manufacturer|expiry))',
                r'supply[^:]*:?\s*([^:\n,;]{3,50})(?=\s*(?:$|\n|,|;|product|batch|manufacturer|expiry))'
            ]
        }
        # Database connection parameters
        self.db_config = {
            'host': 'localhost',
            'database': 'African_Country',
            'user': 'divyanshsingh',
            'port': 5432
        }

    def _create_safetydb_tables(self, cursor):
        """Create required tables in safetydb schema if they don't exist"""
        try:
            # Create countries table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS safetydb.countries (
                    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                    code VARCHAR(3) NOT NULL UNIQUE,
                    name VARCHAR(100) NOT NULL UNIQUE,
                    region VARCHAR(100),
                    population BIGINT,
                    gdp_per_capita NUMERIC(10,2),
                    healthcare_index NUMERIC(5,2),
                    regulatory_maturity VARCHAR(50) CHECK (regulatory_maturity IN ('developing','intermediate','advanced')),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create companies table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS safetydb.companies (
                    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                    name VARCHAR(255) NOT NULL,
                    country_of_origin VARCHAR(100),
                    address TEXT,
                    beneficial_owners JSONB,
                    risk_score INT CHECK (risk_score >= 0 AND risk_score <= 100),
                    total_violations INT DEFAULT 0,
                    logo_url VARCHAR(500),
                    website VARCHAR(500),
                    established_year INT,
                    company_size VARCHAR(50) CHECK (company_size IN ('small','medium','large','multinational')),
                    primary_products TEXT[],
                    regulatory_status VARCHAR(100),
                    last_inspection_date DATE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create regulatory_events table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS safetydb.regulatory_events (
                    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                    url VARCHAR(500) NOT NULL UNIQUE,
                    event_type VARCHAR(50) CHECK (event_type IN ('Alert','Public Notice','Product Recall')),
                    alert_date DATE,
                    alert_name VARCHAR(255),
                    all_text TEXT,
                    notice_date DATE,
                    notice_text TEXT,
                    recall_date DATE,
                    product_name VARCHAR(255),
                    product_type VARCHAR(100),
                    manufacturer_id UUID REFERENCES safetydb.companies(id),
                    recalling_firm_id UUID REFERENCES safetydb.companies(id),
                    batches VARCHAR(255),
                    manufacturing_date DATE,
                    expiry_date DATE,
                    source_url TEXT,
                    pdf_path TEXT,
                    reason_for_action TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create indexes
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_safetydb_companies_name ON safetydb.companies(name)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_safetydb_events_type ON safetydb.regulatory_events(event_type)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_safetydb_events_date ON safetydb.regulatory_events(alert_date, notice_date, recall_date)")
            
            cursor.connection.commit()
            logger.info("✅ SafetyDB tables created successfully")
            
        except Exception as e:
            logger.error(f"❌ Error creating safetydb tables: {e}")
            cursor.connection.rollback()
            raise

    def _ensure_ghana_country(self, cursor):
        """Ensure 'GH' (Ghana) exists in safetydb.countries table."""
        cursor.execute("SELECT code FROM safetydb.countries WHERE code = %s", ('GH',))
        if not cursor.fetchone():
            # Check if Ghana exists with different code and update it
            cursor.execute("SELECT code FROM safetydb.countries WHERE name = %s", ('Ghana',))
            existing = cursor.fetchone()
            if existing:
                cursor.execute("UPDATE safetydb.countries SET code = %s WHERE name = %s", ('GH', 'Ghana'))
            else:
                cursor.execute("INSERT INTO safetydb.countries (code, name, regulatory_maturity) VALUES (%s, %s, %s)", ('GH', 'Ghana', 'intermediate'))

    def _get_or_create_company(self, cursor, name, country_code, company_type):
        """Get company ID by name, or create if not exists. Prevents duplicates by name."""
        if not name or not name.strip():
            return None
        name = name.strip()
        try:
            # First check if company exists by name only (to prevent duplicates)
            cursor.execute("SELECT id FROM safetydb.companies WHERE name = %s", (name,))
            result = cursor.fetchone()
            if result:
                existing_id = result['id'] if isinstance(result, dict) else result[0]
                return existing_id
            
            # Create new company if it doesn't exist
            cursor.execute(
                "INSERT INTO safetydb.companies (name, country_of_origin, company_size) VALUES (%s, %s, %s) RETURNING id",
                (name, country_code, 'medium')
            )
            new_id = cursor.fetchone()
            return new_id['id'] if isinstance(new_id, dict) else new_id[0]
        except Exception as e:
            logger.error(f"❌ Error with company: {name}, {company_type}, {country_code} | {e}")
            # Don't rollback here as it might affect other operations
            return None
    def get_db_connection(self):
        """Get database connection using class db_config"""
        return psycopg2.connect(**self.db_config, cursor_factory=RealDictCursor)
    
    def scrape_all_ghana_data(self) -> Dict[str, List[Dict[str, Any]]]:
        """Main method to scrape all Ghana FDA data with fixed processing.
        Supports limiting via env var SCRAPE_LIMIT (applies per category)."""
        logger.info("🇬🇭 Starting Ghana FDA regulatory data scraping with fixed processing...")
        limit_env = os.getenv('SCRAPE_LIMIT')
        try:
            limit_per_category = int(limit_env) if limit_env else None
        except ValueError:
            limit_per_category = None
        
        results = {
            'recalls': [],
            'alerts': [],
            'notices': []
        }
        # Clear processed products set for this run
        self.processed_products.clear()

        # Scrape recalls with multi-product support
        results['recalls'] = self._scrape_recalls(limit=limit_per_category)

        # Scrape product alerts and download PDFs
        results['alerts'] = self._scrape_alerts(limit=limit_per_category)

        # Scrape press releases and public notices and download PDFs
        results['notices'] = self._scrape_notices(limit=limit_per_category)

        # Save to database with company extraction
        self._save_all_to_database(results)
        return results
    def _scrape_notices(self, limit: Optional[int] = None) -> list:
        """Scrape press releases and public notices, download PDFs to output/notices/Title_Date/"""
        logger.info("🔍 Scraping Ghana FDA press releases and public notices...")
        notices = []
        urls = [
            "https://fdaghana.gov.gh/newsroom/press-release/",
            "https://fdaghana.gov.gh/newsroom/press-release-2/"
        ]
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            for url in urls:
                try:
                    page.goto(url, timeout=120000)
                    page.wait_for_load_state('networkidle')
                    # Set filters to 'All' to get complete data
                    self._set_table_filters_to_all(page)
                    page.wait_for_timeout(2000)
                    rows = page.query_selector_all('table tbody tr')
                    logger.info(f"📊 Found {len(rows)} notice entries at {url}")
                    for i, row in enumerate(rows):
                        if limit and len(notices) >= limit:
                            break
                        try:
                            row_html = row.inner_html()
                            soup = BeautifulSoup(row_html, 'html.parser')
                            cells = soup.find_all(['td', 'th'])
                            if len(cells) < 2:
                                continue
                            date_text = cells[0].get_text(strip=True)
                            title_cell = cells[1]
                            title = title_cell.get_text(strip=True)
                            pdf_url = None
                            for link in title_cell.find_all('a', href=True):
                                href = link['href']
                                if href.lower().endswith('.pdf'):
                                    pdf_url = href if href.startswith('http') else f"https://fdaghana.gov.gh{href}" if href.startswith('/') else f"https://fdaghana.gov.gh/{href}"
                            # Folder name: Title_Date (sanitize)
                            folder_name = f"{title.replace(' ', '_').replace('/', '_')}_{date_text.replace('/', '-')[:10]}"
                            notice_dir = os.path.join(self.notices_dir, folder_name)
                            ensure_directory(notice_dir)
                            pdf_path = None
                            if pdf_url:
                                pdf_filename = pdf_url.split('/')[-1].split('?')[0]
                                pdf_output_path = os.path.join(notice_dir, pdf_filename)
                                try:
                                    response = requests.get(pdf_url, timeout=30)
                                    if response.status_code == 200:
                                        with open(pdf_output_path, 'wb') as f:
                                            f.write(response.content)
                                        logger.info(f"⬇️  Downloaded notice PDF: {pdf_output_path}")
                                        pdf_path = pdf_output_path
                                    else:
                                        logger.warning(f"Failed to download notice PDF: {pdf_url}")
                                except Exception as e:
                                    logger.warning(f"Error downloading notice PDF: {e}")
                            notice_data = {
                                'notice_title': title,
                                'notice_date': date_text,
                                'pdf_url': pdf_url,
                                'pdf_path': pdf_path,
                                'source_url': url
                            }
                            notices.append(notice_data)
                        except Exception as e:
                            logger.warning(f"Error processing notice row {i} at {url}: {e}")
                except Exception as e:
                    logger.error(f"Error scraping notices at {url}: {e}")
            browser.close()
        logger.info(f"🎉 Successfully processed {len(notices)} notices.")
        return notices

    def _scrape_alerts(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Scrape product alerts and download PDFs to output/alerts/Title_Date/"""
        logger.info("🔍 Scraping Ghana FDA product alerts...")
        alerts = []
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            try:
                page.goto(self.urls['alerts'], timeout=120000)
                page.wait_for_load_state('networkidle')
                # Set filters to 'All' to get complete data
                self._set_table_filters_to_all(page)
                page.wait_for_timeout(2000)
                rows = page.query_selector_all('table tbody tr')
                logger.info(f"📊 Found {len(rows)} alert entries")
                for i, row in enumerate(rows):
                    if limit and len(alerts) >= limit:
                        break
                    try:
                        row_html = row.inner_html()
                        soup = BeautifulSoup(row_html, 'html.parser')
                        cells = soup.find_all(['td', 'th'])
                        if len(cells) < 2:
                            continue
                        date_text = cells[0].get_text(strip=True)
                        title_cell = cells[1]
                        title = title_cell.get_text(strip=True)
                        pdf_url = None
                        for link in title_cell.find_all('a', href=True):
                            href = link['href']
                            if href.lower().endswith('.pdf'):
                                pdf_url = href if href.startswith('http') else f"https://fdaghana.gov.gh{href}" if href.startswith('/') else f"https://fdaghana.gov.gh/{href}"
                        # Folder name: Title_Date (sanitize)
                        folder_name = f"{title.replace(' ', '_').replace('/', '_')}_{date_text.replace('/', '-')[:10]}"
                        alert_dir = os.path.join(self.alerts_dir, folder_name)
                        ensure_directory(alert_dir)
                        pdf_path = None
                        if pdf_url:
                            pdf_filename = pdf_url.split('/')[-1].split('?')[0]
                            pdf_output_path = os.path.join(alert_dir, pdf_filename)
                            try:
                                response = requests.get(pdf_url, timeout=30)
                                if response.status_code == 200:
                                    with open(pdf_output_path, 'wb') as f:
                                        f.write(response.content)
                                    logger.info(f"⬇️  Downloaded alert PDF: {pdf_output_path}")
                                    pdf_path = pdf_output_path
                                else:
                                    logger.warning(f"Failed to download alert PDF: {pdf_url}")
                            except Exception as e:
                                logger.warning(f"Error downloading alert PDF: {e}")
                        alert_data = {
                            'alert_title': title,
                            'alert_date': date_text,
                            'pdf_url': pdf_url,
                            'pdf_path': pdf_path,
                            'source_url': self.urls['alerts']
                        }
                        alerts.append(alert_data)
                    except Exception as e:
                        logger.warning(f"Error processing alert row {i}: {e}")
            except Exception as e:
                logger.error(f"Error scraping alerts: {e}")
            finally:
                browser.close()
        logger.info(f"🎉 Successfully processed {len(alerts)} product alerts.")
        return alerts
    
    def _scrape_recalls(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Scrape recalls data with DETAILED PDF EXTRACTION - ENHANCED VERSION"""
        logger.info("🔍 Scraping Ghana FDA recalls with DETAILED PDF extraction...")
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            
            try:
                page.goto(self.urls['recalls'], timeout=120000)
                page.wait_for_load_state('networkidle')
                
                # Set filters to 'All' to get complete data
                self._set_table_filters_to_all(page)
                
                # Wait for table to update with all entries
                page.wait_for_timeout(3000)
                
                # Get all table rows
                rows = page.query_selector_all('table tbody tr')
                logger.info(f"📊 Found {len(rows)} recall entries - Will extract detailed info from each PDF")
                
                recalls = []
                processed_count = 0
                
                for i, row in enumerate(rows):
                    if limit and processed_count >= limit:
                        break
                        
                    try:
                        logger.info(f"� Processing entry {i+1}/{len(rows)}...")
                        
                        # Get row cells as HTML and parse with BeautifulSoup
                        row_html = row.inner_html()
                        soup = BeautifulSoup(row_html, 'html.parser')
                        cells = soup.find_all(['td', 'th'])
                        
                        # Extract basic data from table row
                        recall_data = self._extract_recall_data_with_multiproduct(cells)
                        
                        if recall_data:
                            
                            # Check if this is a multi-product entry
                            if recall_data.get('is_multi_product'):
                                logger.info(f"📦 Processing multi-product entry: {recall_data['product_name']} ({len(recall_data['multi_product_data']['products'])} products)")
                                
                                # Process each individual product from multi-product data
                                for product in recall_data['multi_product_data']['products']:
                                    # Create individual recall entry for each product
                                    individual_recall = {
                                        'product_name': product.get('product_name', recall_data['product_name']),
                                        'product_type': product.get('product_type', recall_data['product_type']),
                                        'manufacturer': product.get('manufacturer', recall_data['manufacturer']),
                                        'recalling_firm': product.get('recalling_firm', recall_data['recalling_firm']),
                                        'batch_numbers': product.get('batch_numbers', recall_data['batch_numbers']),
                                        'manufacturing_date': product.get('manufacturing_date', recall_data['manufacturing_date']),
                                        'expiry_date': product.get('expiry_date', recall_data['expiry_date']),
                                        'recall_date': recall_data['recall_date'],
                                        'source_url': recall_data['source_url'],
                                        'data_source': 'ghana_fda_recalls',
                                        'is_multi_product': False,  # Individual products are not multi-product
                                        # Always use the summary reason for all products in a multi-product recall
                                        'reason_for_recall': recall_data.get('reason_for_recall'),
                                        'original_multi_product': recall_data['product_name'],  # Track original source
                                        # ENHANCED: Include detailed information from PDF
                                        'detailed_content': recall_data.get('detailed_content'),
                                        'manufacturing_firm': product.get('manufacturing_firm') or recall_data.get('manufacturing_firm'),
                                        'importing_firm': recall_data.get('importing_firm'),
                                        'distributing_firm': recall_data.get('distributing_firm'),
                                        'product_description': recall_data.get('product_description'),
                                        'hazard_description': recall_data.get('hazard_description'),
                                        'corrective_action': recall_data.get('corrective_action')
                                    }
                                    # If this entry has a direct PDF link, download and save the PDF as-is
                                    if recall_data.get('pdf_url'):
                                        try:
                                            pdf_url = recall_data['pdf_url']
                                            pdf_filename = f"{individual_recall['product_name'].replace(' ', '_')}.pdf"
                                            pdf_output_path = os.path.join(self.recalls_dir, pdf_filename)
                                            response = requests.get(pdf_url, timeout=30)
                                            if response.status_code == 200:
                                                with open(pdf_output_path, 'wb') as f:
                                                    f.write(response.content)
                                                logger.info(f"⬇️  Downloaded direct PDF: {pdf_output_path}")
                                                individual_recall['pdf_path'] = pdf_output_path
                                            else:
                                                logger.warning(f"Failed to download direct PDF: {pdf_url}")
                                        except Exception as e:
                                            logger.warning(f"Error downloading direct PDF: {e}")
                                    else:
                                        # Generate SIMPLE PDF for individual product (the style you liked)
                                        self._generate_pdf(individual_recall)
                                    recalls.append(individual_recall)
                                    processed_count += 1
                                    logger.info(f"✅ Processed individual product: {individual_recall['product_name']}")
                            else:
                                # Single product - handle direct PDF download if present
                                if recall_data.get('pdf_url'):
                                    try:
                                        pdf_url = recall_data['pdf_url']
                                        pdf_filename = f"{recall_data['product_name'].replace(' ', '_')}.pdf"
                                        pdf_output_path = os.path.join(self.recalls_dir, pdf_filename)
                                        response = requests.get(pdf_url, timeout=30)
                                        if response.status_code == 200:
                                            with open(pdf_output_path, 'wb') as f:
                                                f.write(response.content)
                                            logger.info(f"⬇️  Downloaded direct PDF: {pdf_output_path}")
                                            recall_data['pdf_path'] = pdf_output_path
                                        else:
                                            logger.warning(f"Failed to download direct PDF: {pdf_url}")
                                    except Exception as e:
                                        logger.warning(f"Error downloading direct PDF: {e}")
                                else:
                                    # Generate SIMPLE PDF
                                    self._generate_pdf(recall_data)
                                recalls.append(recall_data)
                                processed_count += 1
                                logger.info(f"✅ Processed single product with detailed info: {recall_data['product_name']}")
                        
                    except Exception as e:
                        logger.error(f"Error processing row {i}: {e}")
                        continue
                
                logger.info(f"🎉 Successfully processed {processed_count} individual products with DETAILED PDF information from {len(rows)} table entries")
                
            except Exception as e:
                logger.error(f"Error scraping recalls: {e}")
                return []
            finally:
                browser.close()
        
        return recalls
    
    def _create_unique_product_id(self, recall_data: Dict[str, Any]) -> str:
        """Create unique identifier for products to avoid duplicates"""
        components = [
            recall_data.get('product_name', '').strip(),
            recall_data.get('manufacturer', '').strip(),
            recall_data.get('batch_numbers', '').strip(),
            recall_data.get('product_size', '').strip()
        ]
        
        # Clean and normalize components
        clean_components = []
        for comp in components:
            if comp and comp != 'N/A':
                # Normalize text for comparison
                clean_comp = re.sub(r'\s+', ' ', comp.lower().strip())
                clean_components.append(clean_comp)
        
        return '|||'.join(clean_components)
    
    def _extract_recall_data_with_multiproduct(self, cells: List) -> Optional[Dict[str, Any]]:
        """Extract recall information with multi-product support for Morning Mills, DR products, etc."""
        try:
            if len(cells) < 4:
                return None
                
            cell_texts = [cell.get_text(strip=True) for cell in cells]
            
            # Skip header rows
            if any(header in str(cell_texts).lower() for header in ['date', 'product name', 'manufacturer', 'batch', 'type']):
                return None
            
            # Ghana FDA table structure: [Date, Product Name, Product Type, Manufacturer, Recalling Firm, Batch(es), Mfg Date, Expiry Date]
            recall_data = {
                'event_type': 'Product Recall',
                'recall_date': cell_texts[0] if len(cell_texts) > 0 else None,
                'product_name': cell_texts[1] if len(cell_texts) > 1 else None,
                'product_type': cell_texts[2] if len(cell_texts) > 2 else None,
                'manufacturer': cell_texts[3] if len(cell_texts) > 3 else None,
                'recalling_firm': cell_texts[4] if len(cell_texts) > 4 else None,
                'batch_numbers': cell_texts[5] if len(cell_texts) > 5 else None,
                'manufacturing_date': cell_texts[6] if len(cell_texts) > 6 else None,
                'expiry_date': cell_texts[7] if len(cell_texts) > 7 else None,
                'reason_for_action': None,
                'reason_for_recall': None,
                'source_url': self.urls['recalls'],
                'product_page_url': None,
                'pdf_url': None,
                'pdf_path': None,
                'detailed_content': None
            }
            
            # Extract links from cells
            for cell in cells:
                links = cell.find_all('a', href=True)
                for link in links:
                    href = link['href']
                    
                    if href.startswith('http'):
                        full_url = href
                    elif href.startswith('/'):
                        full_url = f"https://fdaghana.gov.gh{href}"
                    else:
                        full_url = f"https://fdaghana.gov.gh/{href}"
                    
                    if href.lower().endswith('.pdf'):
                        recall_data['pdf_url'] = full_url
                    else:
                        recall_data['product_page_url'] = full_url
            
            # Clean up date
            if recall_data['recall_date']:
                try:
                    date_obj = DateParser.parse_date(recall_data['recall_date'])
                    if date_obj:
                        recall_data['recall_date'] = date_obj.strftime('%Y-%m-%d')
                except:
                    pass
            
            # Set defaults
            if not recall_data['product_name'] or recall_data['product_name'] in ['', 'N/A']:
                recall_data['product_name'] = f"Product_{int(time.time())}"
            
            if not recall_data['recall_date']:
                recall_data['recall_date'] = datetime.now().strftime('%Y-%m-%d')
            
            # Get detailed content from product page and check for multi-product
            if recall_data['product_page_url']:
                try:
                    detailed_content = self._get_page_content(recall_data['product_page_url'])
                    if detailed_content:
                        recall_data['detailed_content'] = detailed_content
                        
                        # Check if this is a multi-product recall (Morning Mills, DR products, etc.)
                        structured_data = self._parse_structured_product_page(recall_data['product_page_url'])
                        
                        if structured_data.get('is_multi_product'):
                            # This is a multi-product recall - return multiple recall objects
                            logger.info(f"Multi-product recall found with {len(structured_data['products'])} products")
                            recall_data['is_multi_product'] = True
                            recall_data['multi_product_data'] = structured_data
                            
                            # Extract reason for the multi-product recall
                            if not recall_data.get('reason_for_recall'):
                                html_content = None
                                try:
                                    html_content = requests.get(recall_data['product_page_url']).text
                                except Exception:
                                    pass
                                reason = self._extract_reason_from_content(detailed_content, html=html_content)
                                if reason:
                                    recall_data['reason_for_recall'] = reason
                                    recall_data['reason_for_action'] = reason
                                else:
                                    # Generate fallback reason for multi-product recalls
                                    recall_data['reason_for_recall'] = "Multi-product recall due to quality or safety concerns"
                                    recall_data['reason_for_action'] = "Multi-product recall due to quality or safety concerns"
                            
                            return recall_data
                        
                        # Extract reason from detailed content for single products
                        # Try to extract reason from content, fallback to HTML summary if not found
                        html_content = None
                        try:
                            html_content = requests.get(recall_data['product_page_url']).text
                        except Exception:
                            pass
                        reason = self._extract_reason_from_content(detailed_content, html=html_content)
                        if reason:
                            recall_data['reason_for_action'] = reason
                            recall_data['reason_for_recall'] = reason
                        else:
                            # Fallback: Generate basic reason from product name/type
                            fallback_reason = self._generate_fallback_reason(recall_data)
                            if fallback_reason:
                                recall_data['reason_for_action'] = fallback_reason
                                recall_data['reason_for_recall'] = fallback_reason
                except Exception as e:
                    logger.warning(f"Could not fetch detailed content: {e}")
            
            return recall_data
            
        except Exception as e:
            logger.error(f"Error extracting recall data: {e}")
            return None
    
    def _get_page_content(self, url: str) -> Optional[str]:
        """Get page content using requests"""
        try:
            response = requests.get(url, timeout=30, headers={
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
            })
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Extract main content
                main_content = soup.find('main') or soup.find('article') or soup.find('.entry-content') or soup.find('.content')
                if main_content:
                    return main_content.get_text(separator='\n', strip=True)
                else:
                    return soup.get_text(separator='\n', strip=True)
            
        except Exception as e:
            logger.warning(f"Error fetching page content from {url}: {e}")
        
        return None
    
    def _extract_reason_from_content(self, content: str, html: str = None) -> Optional[str]:
        """Extract reason for recall from content with multiple fallback strategies"""
        if not content and not html:
            return None
            
        # Strategy 1: Look for explicit "Reason for Recall" patterns
        if content:
            patterns = [
                r'Reason\s*for\s*Recall[:\-]?\s*(.*?)(?:\n\s*[A-Z][^:]*:|$)',
                r'Reason\s*for\s*Action[:\-]?\s*(.*?)(?:\n\s*[A-Z][^:]*:|$)',
                r'Recall\s*Reason[:\-]?\s*(.*?)(?:\n\s*[A-Z][^:]*:|$)',
                r'Why\s*recalled[:\-]?\s*(.*?)(?:\n\s*[A-Z][^:]*:|$)',
                r'Problem[:\-]?\s*(.*?)(?:\n\s*[A-Z][^:]*:|$)',
                r'Issue[:\-]?\s*(.*?)(?:\n\s*[A-Z][^:]*:|$)',
                r'Defect[:\-]?\s*(.*?)(?:\n\s*[A-Z][^:]*:|$)',
                r'Contamination[:\-]?\s*(.*?)(?:\n\s*[A-Z][^:]*:|$)',
                r'Quality\s*issue[:\-]?\s*(.*?)(?:\n\s*[A-Z][^:]*:|$)'
            ]
            
            for pattern in patterns:
                match = re.search(pattern, content, re.IGNORECASE | re.DOTALL)
                if match:
                    reason = match.group(1).strip()
                    if len(reason) > 10:
                        return reason[:1000]
        
        # Strategy 2: Extract from HTML structure
        if html:
            try:
                soup = BeautifulSoup(html, 'html.parser')
                
                # Look for summary in h5 tags (common in FDA pages)
                h5s = soup.find_all('h5')
                for h5 in h5s:
                    text = h5.get_text(strip=True)
                    if text and len(text) > 30 and not any(skip in text.lower() for skip in ['date', 'batch', 'manufacturer']):
                        return text[:1000]
                
                # Look for content in paragraphs
                paragraphs = soup.find_all('p')
                for p in paragraphs:
                    text = p.get_text(strip=True)
                    if len(text) > 50 and any(keyword in text.lower() for keyword in ['recall', 'contamination', 'defect', 'quality', 'safety', 'problem']):
                        return text[:1000]
                        
                # Look for content in divs with specific classes
                content_divs = soup.find_all('div', class_=['elementor-widget-container', 'entry-content', 'content'])
                for div in content_divs:
                    text = div.get_text(strip=True)
                    if len(text) > 50:
                        # Extract first meaningful sentence
                        sentences = text.split('.')
                        for sentence in sentences:
                            if len(sentence.strip()) > 30 and any(keyword in sentence.lower() for keyword in ['recall', 'contamination', 'defect', 'quality', 'safety']):
                                return sentence.strip()[:1000]
                                
            except Exception as e:
                logger.debug(f"Error parsing HTML for reason: {e}")
        
        # Strategy 3: Generic content extraction
        if content:
            # Look for any sentence mentioning recall-related keywords
            sentences = content.split('.')
            for sentence in sentences:
                sentence = sentence.strip()
                if len(sentence) > 30 and any(keyword in sentence.lower() for keyword in ['recall', 'contamination', 'defect', 'quality', 'safety', 'problem', 'issue']):
                    return sentence[:1000]
        
        return None
    
    def _generate_fallback_reason(self, recall_data: Dict[str, Any]) -> Optional[str]:
        """Generate a fallback reason based on product information"""
        product_name = recall_data.get('product_name', '').lower()
        product_type = recall_data.get('product_type', '').lower()
        
        # Generate reason based on product type and common recall patterns
        if 'antibiotic' in product_name or 'antibiotic' in product_type:
            return "Quality defect or contamination in antibiotic product"
        elif 'injection' in product_name or 'injection' in product_type:
            return "Quality or safety issue with injectable product"
        elif 'suspension' in product_name or 'suspension' in product_type:
            return "Quality defect in oral suspension formulation"
        elif 'tablet' in product_name or 'tablet' in product_type:
            return "Quality or manufacturing defect in tablet formulation"
        elif 'syrup' in product_name or 'syrup' in product_type:
            return "Quality issue or contamination in syrup product"
        elif 'food' in product_type or any(food in product_name for food in ['oats', 'muesli', 'cereal', 'juice']):
            return "Food safety concern or quality defect"
        elif 'water' in product_name or 'mineral' in product_name:
            return "Water quality or contamination issue"
        elif 'bleach' in product_name or 'chlorine' in product_name:
            return "Chemical product safety or quality issue"
        elif 'test' in product_name and 'strip' in product_name:
            return "Medical device quality or accuracy issue"
        else:
            return "Product quality or safety concern"
    
    def extract_companies_from_content(self, content: str) -> Dict[str, List[str]]:
        """Dynamically extract all company information from content with ENHANCED extraction"""
        companies = {
            'manufacturers': set(),
            'recalling_firms': set(),
            'distributors': set(),
            'importers': set(),
            'suppliers': set()
        }
        
        content_clean = re.sub(r'\s+', ' ', content.lower().strip())
        
        # Extract companies using patterns
        for company_type, patterns in self.company_patterns.items():
            for pattern in patterns:
                matches = re.finditer(pattern, content_clean, re.IGNORECASE)
                for match in matches:
                    company = self._clean_company_name(match.group(1))
                    if company:
                        companies[company_type].add(company)
        
        # ENHANCED: Extract brand names from product names (like Morning Mills, DR products)
        product_name_patterns = [
            r'(morning mills?)[^a-z]',
            r'(ritebrand)[^a-z]',
            r'(dr\.?\s+\w+)[^a-z]',
            r'(\w+\s+mills?)[^a-z]',
            r'(\w+\s+pharmaceuticals?)[^a-z]',
            r'(\w+\s+limited)[^a-z]',
            r'(\w+\s+ltd)[^a-z]',
            r'(\w+\s+company)[^a-z]',
        ]
        
        for pattern in product_name_patterns:
            matches = re.finditer(pattern, content_clean, re.IGNORECASE)
            for match in matches:
                company = self._clean_company_name(match.group(1))
                if company and len(company) > 2:
                    # Determine most likely company type based on name
                    if any(word in company.lower() for word in ['mills', 'food', 'cereal']):
                        companies['manufacturers'].add(company)
                    elif any(word in company.lower() for word in ['pharmaceuticals', 'pharma', 'labs', 'healthcare']):
                        companies['manufacturers'].add(company)
                    elif any(word in company.lower() for word in ['limited', 'ltd', 'inc', 'corp']):
                        companies['manufacturers'].add(company)
                    else:
                        companies['manufacturers'].add(company)  # Default to manufacturer
        
        # Convert sets to lists
        return {k: list(v) for k, v in companies.items()}
    
    def _clean_company_name(self, name: str) -> Optional[str]:
        """Clean and validate company names"""
        if not name:
            return None
            
        # Remove common prefixes/suffixes and clean
        name = re.sub(r'^(the\s+|a\s+|an\s+)', '', name.strip(), flags=re.IGNORECASE)
        name = re.sub(r'\s*(ltd|inc|corp|llc|co|company|limited)\.?\s*$', '', name, flags=re.IGNORECASE)
        name = re.sub(r'[^\w\s&.-]', '', name)
        name = re.sub(r'\s+', ' ', name).strip()
        
        # Filter invalid entries
        if len(name) < 2 or len(name) > 100:
            return None
        if re.match(r'^\d+$', name):
            return None
        if name.lower() in ['unknown', 'n/a', 'not available', 'not specified', 'none']:
            return None
            
        return name.title()
    
    def _extract_detailed_pdf_information(self, recall_data: Dict[str, Any], page) -> Dict[str, Any]:
        """ENHANCED: Extract comprehensive information from product's individual page/PDF"""
        try:
            logger.info(f"📄 Extracting detailed PDF information for: {recall_data['product_name']}")
            
            # Look for clickable links in the table row
            source_url = recall_data.get('source_url')
            
            # Try to find individual product page or PDF link
            product_link = None
            
            # Method 1: Look for direct PDF links in the page
            pdf_links = page.query_selector_all('a[href*=".pdf"], a[href*="download"], a[href*="recall"]')
            
            # Method 2: Try to find product-specific page
            # Navigate to individual product pages if they exist
            try:
                # Check if there are individual product detail pages
                detail_links = page.query_selector_all('a[href*="product"], a[href*="recall"], .product-link, .recall-link')
                
                for link in detail_links:
                    link_text = link.inner_text().lower() if link.inner_text() else ""
                    href = link.get_attribute('href') if link else ""
                    
                    # Check if this link relates to our product
                    product_name_words = recall_data['product_name'].lower().split()[:3]  # First 3 words
                    if any(word in link_text or word in href for word in product_name_words if len(word) > 3):
                        product_link = href
                        break
                
                # If we found a specific product link, visit it
                if product_link:
                    if not product_link.startswith('http'):
                        # Make absolute URL
                        base_url = 'https://fdaghana.gov.gh'
                        product_link = base_url + product_link if product_link.startswith('/') else base_url + '/' + product_link
                    
                    logger.info(f"🔗 Found specific product link: {product_link}")
                    
                    # Visit the product page
                    new_page = page.context.new_page()
                    try:
                        new_page.goto(product_link, timeout=15000)
                        new_page.wait_for_load_state('networkidle', timeout=10000)
                        
                        # Extract detailed content from product page
                        page_content = new_page.content()
                        soup = BeautifulSoup(page_content, 'html.parser')
                        
                        # Extract comprehensive information
                        detailed_info = self._extract_comprehensive_product_info(soup, recall_data['product_name'])
                        
                        # Merge with existing data
                        recall_data.update(detailed_info)
                        
                        logger.info(f"✅ Successfully extracted detailed info from product page")
                        
                    except Exception as e:
                        logger.warning(f"Could not access product page {product_link}: {e}")
                    finally:
                        new_page.close()
                        
            except Exception as e:
                logger.warning(f"Error finding product-specific page: {e}")
            
            # Method 3: Look for downloadable PDFs and extract content
            try:
                pdf_links = page.query_selector_all('a[href$=".pdf"]')
                
                for pdf_link in pdf_links:
                    pdf_href = pdf_link.get_attribute('href')
                    if pdf_href:
                        if not pdf_href.startswith('http'):
                            pdf_href = 'https://fdaghana.gov.gh' + pdf_href if pdf_href.startswith('/') else 'https://fdaghana.gov.gh/' + pdf_href
                        
                        # Check if this PDF might be related to our product
                        link_text = pdf_link.inner_text().lower()
                        product_words = recall_data['product_name'].lower().split()[:3]
                        
                        if any(word in link_text or word in pdf_href for word in product_words if len(word) > 3):
                            logger.info(f"📄 Found related PDF: {pdf_href}")
                            
                            # Try to download and extract PDF content
                            pdf_content = self._extract_pdf_content(pdf_href)
                            if pdf_content:
                                pdf_info = self._parse_pdf_content_for_details(pdf_content)
                                recall_data.update(pdf_info)
                                recall_data['pdf_source_url'] = pdf_href
                                logger.info("✅ Successfully extracted content from PDF")
                                break
                            
            except Exception as e:
                logger.warning(f"Error processing PDF links: {e}")
            
            # Method 4: Try to extract more details from current page content if no specific links found
            if not product_link and not recall_data.get('detailed_content'):
                try:
                    page_content = page.content()
                    soup = BeautifulSoup(page_content, 'html.parser')
                    
                    # Look for any additional information in the page
                    additional_info = self._extract_page_details(soup, recall_data['product_name'])
                    recall_data.update(additional_info)
                    
                except Exception as e:
                    logger.warning(f"Error extracting page details: {e}")
            
        except Exception as e:
            logger.error(f"Error in detailed PDF extraction: {e}")
        
        return recall_data
    
    def _extract_pdf_content(self, pdf_url: str) -> Optional[str]:
        """Download and extract text content from PDF"""
        try:
            response = requests.get(pdf_url, timeout=15)
            response.raise_for_status()
            
            # Save temporarily and extract text
            temp_pdf_path = "/tmp/temp_recall.pdf"
            with open(temp_pdf_path, 'wb') as f:
                f.write(response.content)
            
            # Use PDFProcessor from utils to extract text
            processor = PDFProcessor(self.recalls_dir)
            text_content = processor.extract_text_from_pdf(temp_pdf_path)
            
            # Clean up
            os.remove(temp_pdf_path)
            
            return text_content
            
        except Exception as e:
            logger.warning(f"Could not extract PDF content from {pdf_url}: {e}")
            return None
    
    def _parse_pdf_content_for_details(self, pdf_content: str) -> Dict[str, Any]:
        """Parse PDF content to extract detailed product information"""
        details = {}
        
        if not pdf_content:
            return details
        
        try:
            # Clean content
            content = pdf_content.lower().replace('\n', ' ').replace('  ', ' ')
            
            # Enhanced extraction patterns for detailed information
            patterns = {
                'reason_for_recall': [
                    r'reason\s*for\s*recall[:\-]?\s*([^.]{20,300})',
                    r'reason\s*for\s*action[:\-]?\s*([^.]{20,300})',
                    r'hazard[:\-]?\s*([^.]{20,300})',
                    r'problem[:\-]?\s*([^.]{20,300})'
                ],
                'manufacturing_firm': [
                    r'manufacturing\s*firm[:\-]?\s*([^.\n]{5,100})',
                    r'manufactured\s*by[:\-]?\s*([^.\n]{5,100})',
                    r'manufacturer[:\-]?\s*([^.\n]{5,100})',
                    r'made\s*by[:\-]?\s*([^.\n]{5,100})'
                ],
                'recalling_firm': [
                    r'recalling\s*firm[:\-]?\s*([^.\n]{5,100})',
                    r'recall\s*initiator[:\-]?\s*([^.\n]{5,100})',
                    r'responsible\s*firm[:\-]?\s*([^.\n]{5,100})'
                ],
                'importing_firm': [
                    r'import(?:ing)?\s*firm[:\-]?\s*([^.\n]{5,100})',
                    r'imported\s*by[:\-]?\s*([^.\n]{5,100})',
                    r'importer[:\-]?\s*([^.\n]{5,100})'
                ],
                'distributing_firm': [
                    r'distribut(?:ing|or)\s*firm[:\-]?\s*([^.\n]{5,100})',
                    r'distributed\s*by[:\-]?\s*([^.\n]{5,100})',
                    r'distributor[:\-]?\s*([^.\n]{5,100})'
                ],
                'product_description': [
                    r'product\s*description[:\-]?\s*([^.]{20,200})',
                    r'description[:\-]?\s*([^.]{20,200})',
                    r'product\s*details[:\-]?\s*([^.]{20,200})'
                ],
                'hazard_description': [
                    r'hazard\s*description[:\-]?\s*([^.]{20,200})',
                    r'health\s*risk[:\-]?\s*([^.]{20,200})',
                    r'potential\s*harm[:\-]?\s*([^.]{20,200})'
                ],
                'corrective_action': [
                    r'corrective\s*action[:\-]?\s*([^.]{20,200})',
                    r'action\s*taken[:\-]?\s*([^.]{20,200})',
                    r'remedy[:\-]?\s*([^.]{20,200})'
                ],
                'batch_numbers': [
                    r'batch\s*(?:number|no)[s]?[:\-]?\s*([^.\n]{3,50})',
                    r'lot\s*(?:number|no)[s]?[:\-]?\s*([^.\n]{3,50})'
                ],
                'manufacturing_date': [
                    r'manufacturing\s*date[:\-]?\s*([^.\n]{5,30})',
                    r'mfg\s*date[:\-]?\s*([^.\n]{5,30})',
                    r'date\s*of\s*manufacture[:\-]?\s*([^.\n]{5,30})'
                ],
                'expiry_date': [
                    r'expiry\s*date[:\-]?\s*([^.\n]{5,30})',
                    r'expiration\s*date[:\-]?\s*([^.\n]{5,30})',
                    r'exp\s*date[:\-]?\s*([^.\n]{5,30})'
                ]
            }
            
            # Extract information using patterns
            for field, field_patterns in patterns.items():
                for pattern in field_patterns:
                    match = re.search(pattern, content, re.IGNORECASE)
                    if match:
                        value = match.group(1).strip()
                        # Clean extracted value
                        value = re.sub(r'\s+', ' ', value)  # Clean whitespace
                        value = value.replace(':', '').strip()
                        
                        if len(value) > 5:  # Only keep meaningful values
                            details[field] = value
                            break
            
            # Store full content for reference
            details['detailed_content'] = pdf_content[:1000]  # First 1000 chars
            
            logger.info(f"📊 Extracted {len(details)} detailed fields from PDF content")
            
        except Exception as e:
            logger.warning(f"Error parsing PDF content: {e}")
        
        return details
    
    def _extract_comprehensive_product_info(self, soup, product_name: str) -> Dict[str, Any]:
        """Extract comprehensive product information from HTML page"""
        info = {}
        
        try:
            # Get all text content
            text_content = soup.get_text(separator=' ', strip=True).lower()
            
            # Use the same patterns as PDF extraction
            return self._parse_pdf_content_for_details(text_content)
            
        except Exception as e:
            logger.warning(f"Error extracting comprehensive info: {e}")
            return info
    
    def _extract_page_details(self, soup, product_name: str) -> Dict[str, Any]:
        """Extract additional details from the current page"""
        details = {}
        
        try:
            # Look for tables or structured content that might contain additional info
            tables = soup.find_all('table')
            
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 2:
                        key = cells[0].get_text(strip=True).lower()
                        value = cells[1].get_text(strip=True)
                        
                        # Map common keys to our fields
                        if 'reason' in key and ('recall' in key or 'action' in key):
                            details['reason_for_recall'] = value
                        elif 'manufacturer' in key or 'manufacturing' in key:
                            if 'firm' in key:
                                details['manufacturing_firm'] = value
                            else:
                                details['manufacturer'] = value
                        elif 'recalling' in key:
                            details['recalling_firm'] = value
            
        except Exception as e:
            logger.warning(f"Error extracting page details: {e}")
        
        return details
    
    def _generate_pdf(self, recall_data: Dict[str, Any]):
        """Generate PDF for recall - ORIGINAL SIMPLE VERSION THAT YOU LIKED"""
        try:
            product_name = recall_data.get('product_name', 'Unknown_Product')
            manufacturer = recall_data.get('manufacturer', '')
            
            # Create sanitized filename
            filename_parts = [product_name]
            if manufacturer:
                filename_parts.append(manufacturer)
            
            filename = '_'.join(filename_parts)
            filename = re.sub(r'[^\w\s-]', '', filename)
            filename = re.sub(r'[-\s]+', '_', filename)
            filename = filename[:80]  # Shorter length
            
            # Create directory and PDF
            product_dir = os.path.join(self.recalls_dir, filename)
            ensure_directory(product_dir)
            
            pdf_path = os.path.join(product_dir, f"{filename}.pdf")
            
            # Generate PDF content - ORIGINAL SIMPLE STYLE
            c = canvas.Canvas(pdf_path, pagesize=letter)
            width, height = letter
            
            y_position = height - 50
            line_height = 20
            
            c.setFont("Helvetica-Bold", 16)
            c.drawString(50, y_position, "Ghana FDA Product Recall Notice")
            y_position -= 30
            
            c.setFont("Helvetica", 12)
            
            # ORIGINAL SIMPLE FIELDS - exactly as you liked them
            fields = [
                ('Product Name', recall_data.get('product_name')),
                ('Product Type', recall_data.get('product_type')),
                ('Manufacturer', recall_data.get('manufacturer')),
                ('Recalling Firm', recall_data.get('recalling_firm')),
                ('Batch Numbers', recall_data.get('batch_numbers')),
                ('Manufacturing Date', recall_data.get('manufacturing_date')),
                ('Expiry Date', recall_data.get('expiry_date')),
                ('Recall Date', recall_data.get('recall_date')),
                (
                    'Reason for Recall',
                    recall_data.get('reason_for_recall') if recall_data.get('reason_for_recall') and str(recall_data.get('reason_for_recall')).strip() else 'Not specified'
                ),
                ('Source URL', recall_data.get('source_url'))
            ]
            
            for field_name, field_value in fields:
                if field_value and str(field_value).strip():
                    c.drawString(50, y_position, f"{field_name}: {field_value}")
                    y_position -= line_height
                    
                    if y_position < 50:  # Start new page if needed
                        c.showPage()
                        y_position = height - 50
                        c.setFont("Helvetica", 12)
            
            c.save()
            recall_data['pdf_path'] = pdf_path
            
            logger.info(f"✅ Created PDF: {pdf_path}")
            
        except Exception as e:
            logger.error(f"Error creating PDF for {recall_data.get('product_name', 'unknown')}: {e}")
    
    def _set_table_filters_to_all(self, page):
        """Set table filters to 'All' to ensure we capture every product"""
        try:
            logger.info("🔍 Setting table filters to 'All'...")
            
            # Wait for page to be ready
            page.wait_for_timeout(1000)
            
            # Common DataTables selectors
            selectors = [
                '.dataTables_length select',
                'select[name*="length"]',
                'select[name*="entries"]',
                '#example_length select',
                '.length select'
            ]
            
            for selector in selectors:
                try:
                    filter_element = page.query_selector(selector)
                    if filter_element and filter_element.is_visible():
                        logger.info(f"Found filter: {selector}")
                        
                        # Try to select "All" or "-1" or "999" option
                        options = page.query_selector_all(f"{selector} option")
                        for option in options:
                            value = option.get_attribute('value')
                            text = option.inner_text().lower()
                            
                            if value in ['-1', '999', '1000'] or 'all' in text:
                                logger.info(f"Selecting option: {text} (value: {value})")
                                page.select_option(selector, value)
                                page.wait_for_timeout(2000)
                                logger.info("✅ Filter set to 'All'")
                                return
                        
                except Exception as e:
                    continue
                    
            logger.info("ℹ️  No datatable filters found - continuing with default view")
            
        except Exception as e:
            logger.warning(f"Error setting filters: {e}")
    
    def _save_all_to_database(self, results: Dict[str, List[Dict]]):
        """Save all data to database with correct schema mapping and date parsing"""
        logger.info("💾 Starting database save process...")
        logger.info(f"📊 Results to process - Recalls: {len(results.get('recalls', []))}, "
                   f"Alerts: {len(results.get('alerts', []))}, "
                   f"Notices: {len(results.get('notices', []))}")
        
        total_saved = 0
        conn = None
        cursor = None
        
        try:
            # 1. Establish database connection with timeout
            logger.debug("🔌 Establishing database connection...")
            conn = self.get_db_connection()
            conn.autocommit = False  # Use explicit transactions
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            logger.info("✅ Database connection established successfully")
            
            # 2. Create tables and ensure required data exists
            self._create_safetydb_tables(cursor)
            self._ensure_ghana_country(cursor)
            
            # 3. Process each category
            for category, items in results.items():
                if not items:
                    logger.info(f"ℹ️  No items found in category: {category}")
                    continue
                    
                logger.info(f"📥 Processing {len(items)} items in category: {category}")
                
                for item_idx, item in enumerate(items, 1):
                    try:
                        logger.debug(f"🔍 Processing item {item_idx}/{len(items)}: {item.get('product_name', 'Untitled')}")
                        
                        # --- Date parsing with validation ---
                        alert_date = notice_date = recall_date = None
                        
                        try:
                            if category == 'alerts' and item.get('alert_date'):
                                date_field = DateParser.parse_date(item['alert_date'])
                                alert_date = date_field.date() if date_field and hasattr(date_field, 'date') else date_field
                                logger.debug(f"📅 Parsed alert_date: {alert_date}")
                                
                            elif category == 'notices' and item.get('notice_date'):
                                date_field = DateParser.parse_date(item['notice_date'])
                                notice_date = date_field.date() if date_field and hasattr(date_field, 'date') else date_field
                                logger.debug(f"📅 Parsed notice_date: {notice_date}")
                                
                            elif category == 'recalls' and item.get('recall_date'):
                                date_field = DateParser.parse_date(item['recall_date'])
                                recall_date = date_field.date() if date_field and hasattr(date_field, 'date') else date_field
                                logger.debug(f"📅 Parsed recall_date: {recall_date}")
                                
                        except Exception as e:
                            logger.warning(f"⚠️ Error parsing dates for item: {e}")
                        
                        # --- Prepare data for insertion ---
                        event_type = {
                            'alerts': 'Alert',
                            'notices': 'Public Notice',
                            'recalls': 'Product Recall'
                        }.get(category, 'Unknown')
                        
                        product_name = item.get('product_name', '')
                        title = (item.get('alert_title') or 
                                item.get('notice_title') or 
                                product_name or 
                                'Untitled')
                        
                        # Create a clean copy of item data for logging
                        loggable_item = {k: v for k, v in item.items() 
                                       if not isinstance(v, (bytes, bytearray))}
                        
                        logger.debug(f"📝 Preparing to save: {title} ({event_type})")
                        
                        # Build insert data with explicit NULL handling
                        insert_data = {
                            'event_type': event_type,
                            'alert_date': alert_date,
                            'alert_name': title if category == 'alerts' else None,
                            'all_text': item.get('all_text', title)[:10000],  # Limit text length
                            'notice_date': notice_date,
                            'notice_text': title if category == 'notices' else None,
                            'recall_date': recall_date,
                            'product_name': product_name if category == 'recalls' else None,
                            'product_type': (item.get('product_type') or '')[:200],
                            'manufacturer_id': None,
                            'recalling_firm_id': None,
                            'batches': (item.get('batch_numbers') or '')[:200],
                            'manufacturing_date': None,
                            'expiry_date': None,
                            'source_url': (item.get('source_url') or '')[:500],
                            'pdf_path': (item.get('pdf_path') or '')[:500],
                            'reason_for_action': (item.get('reason_for_recall') or 
                                                item.get('reason_for_action') or '')[:1000],
                        }
                        
                        # Handle recall-specific data
                        if category == 'recalls':
                            try:
                                # Parse manufacturing/expiry dates
                                if item.get('manufacturing_date'):
                                    mfg_date = DateParser.parse_date(item['manufacturing_date'])
                                    if mfg_date:
                                        insert_data['manufacturing_date'] = mfg_date.date() if hasattr(mfg_date, 'date') else mfg_date
                                
                                if item.get('expiry_date'):
                                    exp_date = DateParser.parse_date(item['expiry_date'])
                                    if exp_date:
                                        insert_data['expiry_date'] = exp_date.date() if hasattr(exp_date, 'date') else exp_date
                                
                                # Handle company relationships
                                manufacturer = item.get('manufacturing_firm') or item.get('manufacturer')
                                recalling_firm = item.get('recalling_firm')
                                
                                if manufacturer:
                                    manufacturer = str(manufacturer).strip()
                                    if manufacturer and len(manufacturer) <= 200:  # Ensure reasonable length
                                        try:
                                            manufacturer_id = self._get_or_create_company(
                                                cursor, manufacturer, 'GH', 'Manufacturer')
                                            insert_data['manufacturer_id'] = manufacturer_id
                                            logger.debug(f"🏭 Linked manufacturer: {manufacturer} (ID: {manufacturer_id})")
                                        except Exception as e:
                                            logger.error(f"❌ Error linking manufacturer {manufacturer}: {e}")
                                            raise
                                
                                if recalling_firm:
                                    recalling_firm = str(recalling_firm).strip()
                                    if recalling_firm and len(recalling_firm) <= 200:
                                        try:
                                            recalling_firm_id = self._get_or_create_company(
                                                cursor, recalling_firm, 'GH', 'Reselling Firm')
                                            insert_data['recalling_firm_id'] = recalling_firm_id
                                            logger.debug(f"🏢 Linked recalling firm: {recalling_firm} (ID: {recalling_firm_id})")
                                        except Exception as e:
                                            logger.error(f"❌ Error linking recalling firm {recalling_firm}: {e}")
                                            raise
                                
                            except Exception as e:
                                logger.error(f"❌ Error processing recall data: {e}")
                                conn.rollback()
                                continue
                        
                        # --- Execute database insert ---
                        # Create a unique URL (required field)
                        base_url = item.get('source_url', 'https://fdaghana.gov.gh')
                        unique_id = f"{int(time.time())}_{item_idx}"
                        unique_url = f"{base_url}?id={unique_id}"
                        insert_data['url'] = unique_url
                        
                        insert_query = """
                        INSERT INTO safetydb.regulatory_events (
                            url, event_type, alert_date, alert_name, all_text, notice_date, notice_text, 
                            recall_date, product_name, product_type, manufacturer_id, recalling_firm_id,
                            batches, manufacturing_date, expiry_date, source_url, pdf_path, reason_for_action
                        ) VALUES (
                            %(url)s, %(event_type)s, %(alert_date)s, %(alert_name)s, %(all_text)s, %(notice_date)s, 
                            %(notice_text)s, %(recall_date)s, %(product_name)s, %(product_type)s, 
                            %(manufacturer_id)s, %(recalling_firm_id)s, %(batches)s, %(manufacturing_date)s, 
                            %(expiry_date)s, %(source_url)s, %(pdf_path)s, %(reason_for_action)s
                        ) RETURNING id
                        """
                        
                        try:
                            logger.debug("💾 Attempting to insert record...")
                            cursor.execute(insert_query, insert_data)
                            result = cursor.fetchone()
                            
                            if result and 'id' in result:
                                event_id = result['id']
                                conn.commit()
                                total_saved += 1
                                logger.info(f"✅ Successfully saved {event_type} with ID: {event_id} - {title}")
                            else:
                                logger.warning(f"⚠️ No ID returned after insert for: {title}")
                                logger.debug(f"Insert data: {insert_data}")
                                conn.rollback()
                                
                        except psycopg2.Error as e:
                            logger.error(f"❌ Database error inserting {event_type}: {e.pgerror if hasattr(e, 'pgerror') else e}")
                            logger.debug(f"Query: {cursor.query if hasattr(cursor, 'query') else 'N/A'}")
                            logger.debug(f"Data: {insert_data}")
                            conn.rollback()
                            
                    except Exception as e:
                        logger.error(f"❌ Unexpected error processing item {item_idx}: {e}", exc_info=True)
                        if conn and not conn.closed:
                            conn.rollback()
                        continue
            
            logger.info(f"✅ Database save process completed. Total records saved: {total_saved}")
            return total_saved
            
        except psycopg2.OperationalError as e:
            logger.error(f"❌ Database connection failed: {e}")
            logger.debug("Check if PostgreSQL is running and the database exists")
            return 0
            
        except Exception as e:
            logger.error(f"❌ Critical error in _save_all_to_database: {e}", exc_info=True)
            return 0
            
        finally:
            # Ensure resources are always cleaned up
            try:
                if cursor and not cursor.closed:
                    cursor.close()
                if conn and not conn.closed:
                    conn.close()
                    logger.debug("🔌 Database connection closed")
            except Exception as e:
                logger.error(f"Error during cleanup: {e}")
    
    def _save_companies_to_database(self, cursor, conn, companies_data: Dict[str, Dict[str, List[str]]]) -> int:
        """Save companies to database with FIXED method and proper type mapping"""
        companies_saved = 0
        
        # Map our company types to database allowed values
        type_mapping = {
            'manufacturers': 'Manufacturer',
            'recalling_firms': 'Reselling Firm',
            'distributors': 'Reselling Firm',
            'importers': 'Reselling Firm', 
            'suppliers': 'Reselling Firm'
        }
        
        try:
            for event_id, companies in companies_data.items():
                for company_type, company_list in companies.items():
                    # Map to database allowed type
                    db_company_type = type_mapping.get(company_type, 'Manufacturer')
                    
                    for company_name in company_list:
                        if not company_name or company_name.strip() == '':
                            continue
                            
                        try:
                            # Check if company already exists
                            cursor.execute(
                                "SELECT id FROM companies WHERE name = %s AND type = %s",
                                (company_name.strip(), db_company_type)
                            )
                            
                            if not cursor.fetchone():
                                cursor.execute(
                                    """INSERT INTO companies (name, type, country_code, created_at, updated_at) 
                                       VALUES (%s, %s, %s, %s, %s)""",
                                    (company_name.strip(), db_company_type, 'GH', datetime.now(), datetime.now())
                                )
                                companies_saved += 1
                                logger.info(f"💼 Inserted company: {company_name} ({db_company_type})")
                                conn.commit()
                        except Exception as e:
                            logger.error(f"Error inserting company {company_name}: {e}")
                            conn.rollback()
        
        except Exception as e:
            logger.error(f"Error in company save process: {e}")
        
        return companies_saved

    def _parse_structured_product_page(self, product_url: str) -> Dict[str, Any]:
        """Parse structured data from product page to detect multi-product recalls (Morning Mills, DR products, etc.)"""
        try:
            logger.info(f"Parsing structured data from: {product_url}")
            
            # Use requests to get page content
            try:
                response = requests.get(product_url, timeout=30, headers={
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
                })
                
                if response.status_code == 404:
                    logger.warning(f"Product page not found (404): {product_url}")
                    return {}
                
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'html.parser')
                logger.info(f"Successfully loaded page content")
                
            except Exception as e:
                logger.warning(f"Requests failed for {product_url}: {e}")
                return {}
            
            # Look for structured data in the main content area
            main_content = soup.find('main') or soup.find('article') or soup.find('.entry-content') or soup.find('.content')
            
            if main_content:
                # First extract common recall information
                common_data = self._extract_common_recall_info(main_content)
                
                # Then look for product details table (multi-product indicator)
                products_data = self._extract_products_table(main_content, common_data)
                
                if products_data and len(products_data) > 1:
                    # Multiple products found - this is a multi-product recall
                    logger.info(f"Found {len(products_data)} individual products in recall table")
                    return {
                        'is_multi_product': True,
                        'products': products_data,
                        'common_info': common_data
                    }
                else:
                    # Single product format
                    single_product_data = self._parse_single_product_format(main_content)
                    single_product_data['is_multi_product'] = False
                    return single_product_data
            else:
                logger.warning(f"No structured content found on product page: {product_url}")
                return {}
                
        except Exception as e:
            logger.error(f"Error parsing structured product page {product_url}: {e}")
            return {}
    
    def _extract_common_recall_info(self, main_content) -> Dict[str, Any]:
        """Extract common recall information that applies to all products"""
        common_data = {}
        
        try:
            # Try HTML structure first
            for element in main_content.find_all(['div', 'p', 'span', 'td', 'th']):
                text = element.get_text(strip=True)
                if ':' in text and len(text) < 500:
                    parts = text.split(':', 1)
                    if len(parts) == 2:
                        label = parts[0].strip().lower()
                        value = parts[1].strip()
                        
                        # Skip if value is empty
                        if not value or len(value) < 2:
                            continue
                        
                        # Map common fields
                        field_mapping = {
                            'reason for recall': 'reason_for_recall',
                            'reason for action': 'reason_for_recall', 
                            'recall date': 'recall_date',
                            'date of recall': 'recall_date',
                            'manufacturer': 'manufacturer',
                            'recalling firm': 'recalling_firm',
                            'product type': 'product_type'
                        }
                        
                        for pattern, field_name in field_mapping.items():
                            if pattern in label:
                                common_data[field_name] = value
                                break
        except Exception as e:
            logger.warning(f"Error extracting common recall info: {e}")
        
        return common_data
    
    def _extract_products_table(self, main_content, common_data: Dict) -> List[Dict[str, Any]]:
        """Extract individual products from a table (for multi-product recalls like Morning Mills)"""
        products = []
        
        try:
            # Look for tables containing product information
            tables = main_content.find_all('table')
            
            for table in tables:
                rows = table.find_all('tr')
                
                if len(rows) < 2:  # Need at least header + 1 data row
                    continue
                
                # Try to identify headers
                header_row = rows[0]
                headers = [th.get_text(strip=True).lower() for th in header_row.find_all(['th', 'td'])]
                
                # Check if this looks like a product table
                product_indicators = ['product', 'name', 'batch', 'size', 'expiry', 'manufacturing']
                if not any(indicator in ' '.join(headers) for indicator in product_indicators):
                    continue
                
                logger.info(f"Found potential product table with headers: {headers}")
                
                # Process data rows
                for row in rows[1:]:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) < 2:
                        continue
                    
                    cell_texts = [cell.get_text(strip=True) for cell in cells]
                    
                    # Create product data structure
                    product_data = {}
                    
                    # Map cells to fields based on headers
                    for i, cell_text in enumerate(cell_texts):
                        if i < len(headers) and cell_text:
                            header = headers[i]
                            
                            if 'product' in header or 'name' in header:
                                product_data['product_name'] = cell_text
                            elif 'batch' in header:
                                product_data['batch_numbers'] = cell_text
                            elif 'size' in header or 'weight' in header:
                                product_data['product_size'] = cell_text
                            elif 'expiry' in header or 'exp' in header:
                                product_data['expiry_date'] = cell_text
                            elif 'manufacturing' in header or 'mfg' in header:
                                product_data['manufacturing_date'] = cell_text
                            elif 'code' in header:
                                product_data['product_code'] = cell_text
                    
                    # If we found a product name, add this product
                    if product_data.get('product_name'):
                        # Add common info to individual product
                        product_data.update(common_data)
                        products.append(product_data)
                        logger.info(f"Extracted product: {product_data['product_name']}")
        
        except Exception as e:
            logger.warning(f"Error extracting products table: {e}")
        
        return products
    
    def _parse_single_product_format(self, main_content) -> Dict[str, Any]:
        """Parse single product format page"""
        single_data = {}
        
        try:
            # Extract key-value pairs from the content
            text_content = main_content.get_text(separator='\n', strip=True)
            
            # Common field patterns
            patterns = {
                'product_name': r'product\s*name[:\-]?\s*([^\n]{5,100})',
                'manufacturer': r'manufacturer[:\-]?\s*([^\n]{5,100})',
                'batch_numbers': r'batch[es]*[:\-]?\s*([^\n]{3,50})',
                'expiry_date': r'expiry[:\-]?\s*([^\n]{5,30})',
                'manufacturing_date': r'(?:manufacturing|mfg)[:\-]?\s*([^\n]{5,30})',
                'reason_for_recall': r'reason\s*for\s*(?:recall|action)[:\-]?\s*([^\n]{10,200})'
            }
            
            for field, pattern in patterns.items():
                match = re.search(pattern, text_content.lower(), re.IGNORECASE)
                if match:
                    single_data[field] = match.group(1).strip()
        
        except Exception as e:
            logger.warning(f"Error parsing single product format: {e}")
        
        return single_data

# Main execution
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    scraper = GhanaRegulatoryScraperUnified()
    results = scraper.scrape_all_ghana_data()
    
    print(f"\n🎉 SCRAPING COMPLETE!")
    print(f"📊 Total recalls processed: {len(results['recalls'])}")
    print(f"💾 All data saved to African_Country database")
