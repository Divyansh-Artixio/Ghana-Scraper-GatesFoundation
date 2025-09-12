"""
Product Recalls Scraper for FDA Ghana
Enhanced version with detailed data extraction and PDF handling
"""
import logging
import time
import os
from typing import List, Dict, Any, Optional
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import requests
from datetime import datetime
from urllib.parse import urljoin, urlparse

from utils import DateParser, PDFProcessor, TextCleaner, ensure_directory
from database import db_manager
from ai_enrichment import ai_enrichment

logger = logging.getLogger(__name__)

class ProductRecallsScraper:
    """Enhanced scraper for product recalls from FDA Ghana website"""
    
    def __init__(self, output_dir: str):
        self.base_url = "https://fdaghana.gov.gh/newsroom/product-recalls-and-alerts/"
        self.output_dir = f"{output_dir}/recalls"
        ensure_directory(self.output_dir)
        self.pdf_processor = PDFProcessor(self.output_dir)
        
    def scrape_recalls(self) -> List[Dict[str, Any]]:
        """
        Scrape EVERY entry from the recalls page on every run
        Enhanced to handle summary pages and product tables
        
        Returns:
            List of recall dictionaries with detailed information
        """
        logger.info("Starting comprehensive product recalls scraping")
        recalls = []
        
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                
                # Set longer timeout for slow pages
                page.set_default_timeout(60000)
                
                logger.info(f"Loading recalls page: {self.base_url}")
                page.goto(self.base_url)
                
                # Wait for the page to load
                page.wait_for_load_state('networkidle')
                time.sleep(3)
                
                # Get page content
                content = page.content()
                soup = BeautifulSoup(content, 'html.parser')
                
                # Extract all recall entries from the main page
                recall_entries = self._extract_recall_entries(soup)
                logger.info(f"Found {len(recall_entries)} recall entries to process")
                
                # Process each recall entry
                for i, entry in enumerate(recall_entries, 1):
                    logger.info(f"Processing recall {i}/{len(recall_entries)}: {entry.get('title', 'Unknown')}")
                    
                    processed_recalls = self._process_recall_entry(entry, browser)
                    recalls.extend(processed_recalls)
                
                browser.close()
                
        except Exception as e:
            logger.error(f"Error scraping recalls: {e}")
        
        logger.info(f"Scraped {len(recalls)} total recall records")
        return recalls
    
    def _extract_recall_entries(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Extract all recall entries from the main page"""
        entries = []
        
        # Look for table or list structures
        table = soup.find('table')
        if table:
            rows = table.find_all('tr')[1:]  # Skip header row
            
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 2:
                    entry = self._extract_entry_from_row(cells)
                    if entry:
                        entries.append(entry)
        
        # Also look for article/post structures
        articles = soup.find_all(['article', '.post', '.news-item'])
        for article in articles:
            entry = self._extract_entry_from_article(article)
            if entry:
                entries.append(entry)
        
        return entries
    
    def _extract_entry_from_row(self, cells: List) -> Optional[Dict[str, Any]]:
        """Extract basic recall entry information from table row"""
        try:
            entry = {
                'title': None,
                'date': None,
                'link_url': None,
                'is_pdf': False,
                'source': 'table_row'
            }
            
            # Extract text from each cell
            cell_texts = [cell.get_text(strip=True) for cell in cells]
            
            # Look for links in cells
            for cell in cells:
                links = cell.find_all('a', href=True)
                for link in links:
                    href = link['href']
                    if href:
                        # Make URL absolute
                        if not href.startswith('http'):
                            href = urljoin(self.base_url, href)
                        
                        entry['link_url'] = href
                        entry['is_pdf'] = href.lower().endswith('.pdf')
                        
                        # Use link text as title if available
                        link_text = link.get_text(strip=True)
                        if link_text and not entry['title']:
                            entry['title'] = link_text
                        break
            
            # Extract date and title from cell texts
            for text in cell_texts:
                if not text:
                    continue
                
                # Try to parse as date
                date_obj = DateParser.parse_date(text)
                if date_obj and not entry['date']:
                    entry['date'] = date_obj.strftime('%Y-%m-%d')
                
                # Use as title if longer text and no title yet
                elif len(text) > 5 and not entry['title']:
                    entry['title'] = TextCleaner.clean_text(text)
            
            # Set defaults
            if not entry['title']:
                entry['title'] = f"Recall_{int(time.time())}"
            if not entry['date']:
                entry['date'] = datetime.now().strftime('%Y-%m-%d')
            
            return entry
            
        except Exception as e:
            logger.error(f"Error extracting entry from row: {e}")
            return None
    
    def _extract_entry_from_article(self, article) -> Optional[Dict[str, Any]]:
        """Extract recall entry from article element"""
        try:
            entry = {
                'title': None,
                'date': None,
                'link_url': None,
                'is_pdf': False,
                'source': 'article'
            }
            
            # Extract title
            title_selectors = ['h1', 'h2', 'h3', '.title', '.headline', 'a']
            for selector in title_selectors:
                title_elem = article.select_one(selector)
                if title_elem:
                    entry['title'] = TextCleaner.clean_text(title_elem.get_text(strip=True))
                    # Check if title element has a link
                    if title_elem.name == 'a' and title_elem.get('href'):
                        href = title_elem['href']
                        if not href.startswith('http'):
                            href = urljoin(self.base_url, href)
                        entry['link_url'] = href
                        entry['is_pdf'] = href.lower().endswith('.pdf')
                    break
            
            # Extract date
            date_selectors = ['.date', '.published', 'time']
            for selector in date_selectors:
                date_elem = article.select_one(selector)
                if date_elem:
                    date_text = date_elem.get_text(strip=True)
                    date_obj = DateParser.parse_date(date_text)
                    if date_obj:
                        entry['date'] = date_obj.strftime('%Y-%m-%d')
                        break
            
            # Look for other links if not found in title
            if not entry['link_url']:
                links = article.find_all('a', href=True)
                for link in links:
                    href = link['href']
                    if href and ('recall' in href.lower() or 'alert' in href.lower()):
                        if not href.startswith('http'):
                            href = urljoin(self.base_url, href)
                        entry['link_url'] = href
                        entry['is_pdf'] = href.lower().endswith('.pdf')
                        break
            
            # Set defaults
            if not entry['title']:
                entry['title'] = f"Recall_{int(time.time())}"
            if not entry['date']:
                entry['date'] = datetime.now().strftime('%Y-%m-%d')
            
            return entry
            
        except Exception as e:
            logger.error(f"Error extracting entry from article: {e}")
            return None
    
    def _process_recall_entry(self, entry: Dict[str, Any], browser) -> List[Dict[str, Any]]:
        """
        Process a single recall entry based on its type:
        - PDF: download directly
        - Summary page: extract detailed data and handle product tables
        - No link: create fallback PDF
        """
        recalls = []
        
        try:
            if entry['is_pdf'] and entry['link_url']:
                # Direct PDF download
                recall_data = self._download_pdf_recall(entry)
                if recall_data:
                    recalls.append(recall_data)
            
            elif entry['link_url']:
                # Summary page - extract detailed information
                detailed_recalls = self._process_summary_page(entry, browser)
                recalls.extend(detailed_recalls)
            
            else:
                # No link - create fallback PDF
                recall_data = self._create_fallback_recall(entry)
                if recall_data:
                    recalls.append(recall_data)
                    
        except Exception as e:
            logger.error(f"Error processing recall entry {entry['title']}: {e}")
            # Create fallback on error
            recall_data = self._create_fallback_recall(entry)
            if recall_data:
                recalls.append(recall_data)
        
        return recalls
    
    def _download_pdf_recall(self, entry: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Download PDF directly and create recall record"""
        try:
            logger.info(f"Downloading PDF: {entry['link_url']}")
            
            # Create basic recall data
            recall_data = {
                'event_type': 'Product Recall',
                'product_name': entry['title'],
                'recall_date': entry['date'],
                'source_url': entry['link_url'],
                'reason_for_action': 'PDF Download',
                'manufacturer': None,
                'product_type': None,
                'recalling_firm': None,
                'batches': None,
                'manufacturing_date': None,
                'expiry_date': None
            }
            
            # Download and save PDF
            pdf_path = self._save_recall_pdf(recall_data, entry['link_url'], is_direct_pdf=True)
            recall_data['pdf_path'] = pdf_path
            
            return recall_data
            
        except Exception as e:
            logger.error(f"Error downloading PDF recall: {e}")
            return None
    
    def _process_summary_page(self, entry: Dict[str, Any], browser) -> List[Dict[str, Any]]:
        """
        Process summary page to extract detailed recall information
        Handle product tables if present
        """
        recalls = []
        
        try:
            logger.info(f"Processing summary page: {entry['link_url']}")
            
            page = browser.new_page()
            page.goto(entry['link_url'])
            page.wait_for_load_state('networkidle')
            time.sleep(2)
            
            content = page.content()
            soup = BeautifulSoup(content, 'html.parser')
            
            # Extract detailed recall information
            recall_data = self._extract_detailed_recall_info(soup, entry)
            
            # Check for product tables
            product_tables = soup.find_all('table')
            
            if product_tables:
                # Process each product table
                for i, table in enumerate(product_tables):
                    table_recalls = self._process_product_table(table, recall_data, i)
                    recalls.extend(table_recalls)
            else:
                # No product table - create single recall
                pdf_path = self._create_summary_pdf(recall_data)
                recall_data['pdf_path'] = pdf_path
                recalls.append(recall_data)
            
            page.close()
            
        except Exception as e:
            logger.error(f"Error processing summary page {entry['link_url']}: {e}")
            # Create fallback on error
            recall_data = self._create_fallback_recall(entry)
            if recall_data:
                recalls.append(recall_data)
        
        return recalls
    
    def _extract_detailed_recall_info(self, soup: BeautifulSoup, entry: Dict[str, Any]) -> Dict[str, Any]:
        """Extract detailed recall information from summary page"""
        recall_data = {
            'event_type': 'Product Recall',
            'product_name': entry['title'],
            'recall_date': entry['date'],
            'source_url': entry['link_url'],
            'manufacturer': None,
            'product_type': None,
            'recalling_firm': None,
            'batches': None,
            'manufacturing_date': None,
            'expiry_date': None,
            'reason_for_action': None
        }
        
        # Extract text content for analysis
        content_text = soup.get_text()
        
        # Look for specific patterns in the content
        patterns = {
            'manufacturer': [r'manufacturer[:\s]+([^\n]+)', r'manufactured by[:\s]+([^\n]+)'],
            'product_type': [r'product type[:\s]+([^\n]+)', r'type[:\s]+([^\n]+)'],
            'recalling_firm': [r'recalling firm[:\s]+([^\n]+)', r'recalled by[:\s]+([^\n]+)'],
            'batches': [r'batch[es]*[:\s]+([^\n]+)', r'lot[:\s]+([^\n]+)'],
            'manufacturing_date': [r'manufacturing date[:\s]+([^\n]+)', r'mfg date[:\s]+([^\n]+)'],
            'expiry_date': [r'expiry date[:\s]+([^\n]+)', r'exp date[:\s]+([^\n]+)'],
            'reason_for_action': [r'reason[:\s]+([^\n]+)', r'recalled due to[:\s]+([^\n]+)']
        }
        
        import re
        for field, regex_patterns in patterns.items():
            for pattern in regex_patterns:
                match = re.search(pattern, content_text, re.IGNORECASE)
                if match:
                    recall_data[field] = TextCleaner.clean_text(match.group(1))
                    break
        
        # If no reason found, use first paragraph as reason
        if not recall_data['reason_for_action']:
            paragraphs = soup.find_all('p')
            if paragraphs:
                recall_data['reason_for_action'] = TextCleaner.clean_text(paragraphs[0].get_text())[:500]
        
        return recall_data
    
    def _process_product_table(self, table, base_recall_data: Dict[str, Any], table_index: int) -> List[Dict[str, Any]]:
        """Process product table and create separate PDF for each product row"""
        recalls = []
        
        try:
            rows = table.find_all('tr')[1:]  # Skip header
            
            for i, row in enumerate(rows):
                cells = row.find_all(['td', 'th'])
                cell_texts = [cell.get_text(strip=True) for cell in cells]
                
                if any(cell_texts):  # Only process non-empty rows
                    # Create recall data for this product
                    product_recall = base_recall_data.copy()
                    
                    # Try to identify product name from row
                    product_name = None
                    for text in cell_texts:
                        if len(text) > 5 and not any(char.isdigit() for char in text[:5]):
                            product_name = text
                            break
                    
                    if product_name:
                        product_recall['product_name'] = TextCleaner.clean_text(product_name)
                    else:
                        product_recall['product_name'] = f"{base_recall_data['product_name']}_Product_{i+1}"
                    
                    # Create PDF for this product
                    pdf_path = self._create_product_table_pdf(product_recall, cell_texts, table_index, i)
                    product_recall['pdf_path'] = pdf_path
                    
                    recalls.append(product_recall)
            
        except Exception as e:
            logger.error(f"Error processing product table: {e}")
        
        return recalls
    
    def _save_recall_pdf(self, recall_data: Dict[str, Any], pdf_url: str, is_direct_pdf: bool = False) -> str:
        """Save recall PDF with proper naming convention"""
        try:
            product_name = recall_data.get('product_name', 'Unknown_Product')
            recall_date = recall_data.get('recall_date', datetime.now().strftime('%Y-%m-%d'))
            
            # Clean product name for folder/filename
            clean_product_name = TextCleaner.clean_filename(product_name)
            
            # Create product-specific folder
            product_folder = f"{self.output_dir}/{clean_product_name}"
            ensure_directory(product_folder)
            
            # Determine filename
            if is_direct_pdf:
                # Use original filename if possible
                parsed_url = urlparse(pdf_url)
                original_filename = os.path.basename(parsed_url.path)
                if original_filename.endswith('.pdf'):
                    filename = original_filename
                else:
                    filename = f"Recall_Summary_{clean_product_name}_{recall_date.replace('-', '_')}.pdf"
            else:
                filename = f"Recall_Summary_{clean_product_name}_{recall_date.replace('-', '_')}.pdf"
            
            file_path = f"{product_folder}/{filename}"
            
            # Download PDF
            response = requests.get(pdf_url, timeout=30)
            response.raise_for_status()
            
            with open(file_path, 'wb') as f:
                f.write(response.content)
            
            logger.info(f"Saved recall PDF: {file_path}")
            return file_path
            
        except Exception as e:
            logger.error(f"Error saving recall PDF: {e}")
            return self._create_fallback_pdf(recall_data)
    
    def _create_summary_pdf(self, recall_data: Dict[str, Any]) -> str:
        """Create PDF from summary page data"""
        try:
            product_name = recall_data.get('product_name', 'Unknown_Product')
            recall_date = recall_data.get('recall_date', datetime.now().strftime('%Y-%m-%d'))
            
            clean_product_name = TextCleaner.clean_filename(product_name)
            
            # Create product-specific folder
            product_folder = f"{self.output_dir}/{clean_product_name}"
            ensure_directory(product_folder)
            
            filename = f"Recall_Summary_{clean_product_name}_{recall_date.replace('-', '_')}.pdf"
            file_path = f"{product_folder}/{filename}"
            
            # Create PDF with recall information
            from reportlab.pdfgen import canvas
            from reportlab.lib.pagesizes import letter
            
            c = canvas.Canvas(file_path, pagesize=letter)
            y_position = 750
            
            # Add recall information
            c.drawString(50, y_position, f"PRODUCT RECALL SUMMARY")
            y_position -= 30
            
            fields = [
                ('Product Name', recall_data.get('product_name')),
                ('Recall Date', recall_data.get('recall_date')),
                ('Manufacturer', recall_data.get('manufacturer')),
                ('Product Type', recall_data.get('product_type')),
                ('Recalling Firm', recall_data.get('recalling_firm')),
                ('Batch(es)', recall_data.get('batches')),
                ('Manufacturing Date', recall_data.get('manufacturing_date')),
                ('Expiry Date', recall_data.get('expiry_date')),
                ('Reason for Recall', recall_data.get('reason_for_action'))
            ]
            
            for field_name, field_value in fields:
                if field_value:
                    c.drawString(50, y_position, f"{field_name}: {field_value[:80]}")
                    y_position -= 20
                    if y_position < 50:  # Start new page if needed
                        c.showPage()
                        y_position = 750
            
            c.save()
            
            logger.info(f"Created summary PDF: {file_path}")
            return file_path
            
        except Exception as e:
            logger.error(f"Error creating summary PDF: {e}")
            return self._create_fallback_pdf(recall_data)
    
    def _create_product_table_pdf(self, recall_data: Dict[str, Any], cell_texts: List[str], table_index: int, row_index: int) -> str:
        """Create PDF for individual product from table row"""
        try:
            product_name = recall_data.get('product_name', 'Unknown_Product')
            recall_date = recall_data.get('recall_date', datetime.now().strftime('%Y-%m-%d'))
            
            clean_product_name = TextCleaner.clean_filename(product_name)
            
            # Create product-specific folder
            product_folder = f"{self.output_dir}/{clean_product_name}"
            ensure_directory(product_folder)
            
            filename = f"Recall_Summary_{clean_product_name}_{recall_date.replace('-', '_')}_T{table_index}_R{row_index}.pdf"
            file_path = f"{product_folder}/{filename}"
            
            # Create PDF with product information
            from reportlab.pdfgen import canvas
            from reportlab.lib.pagesizes import letter
            
            c = canvas.Canvas(file_path, pagesize=letter)
            y_position = 750
            
            c.drawString(50, y_position, f"PRODUCT RECALL - TABLE ENTRY")
            y_position -= 30
            
            c.drawString(50, y_position, f"Product: {product_name}")
            y_position -= 20
            c.drawString(50, y_position, f"Date: {recall_date}")
            y_position -= 30
            
            c.drawString(50, y_position, "Product Details:")
            y_position -= 20
            
            for i, cell_text in enumerate(cell_texts):
                if cell_text:
                    c.drawString(70, y_position, f"• {cell_text[:70]}")
                    y_position -= 15
                    if y_position < 50:
                        c.showPage()
                        y_position = 750
            
            c.save()
            
            logger.info(f"Created product table PDF: {file_path}")
            return file_path
            
        except Exception as e:
            logger.error(f"Error creating product table PDF: {e}")
            return self._create_fallback_pdf(recall_data)
    
    def _create_fallback_recall(self, entry: Dict[str, Any]) -> Dict[str, Any]:
        """Create fallback recall when no link exists"""
        recall_data = {
            'event_type': 'Product Recall',
            'product_name': entry['title'],
            'recall_date': entry['date'],
            'source_url': self.base_url,
            'reason_for_action': 'Page not found or no link available',
            'manufacturer': None,
            'product_type': None,
            'recalling_firm': None,
            'batches': None,
            'manufacturing_date': None,
            'expiry_date': None
        }
        
        pdf_path = self._create_fallback_pdf(recall_data)
        recall_data['pdf_path'] = pdf_path
        
        return recall_data
    
    def _create_fallback_pdf(self, recall_data: Dict[str, Any]) -> str:
        """Create fallback PDF using main table data"""
        try:
            product_name = recall_data.get('product_name', 'Unknown_Product')
            recall_date = recall_data.get('recall_date', datetime.now().strftime('%Y-%m-%d'))
            
            clean_product_name = TextCleaner.clean_filename(product_name)
            
            # Create product-specific folder
            product_folder = f"{self.output_dir}/{clean_product_name}"
            ensure_directory(product_folder)
            
            filename = f"Page_Not_Found_{clean_product_name}_{recall_date.replace('-', '_')}.pdf"
            file_path = f"{product_folder}/{filename}"
            
            # Create PDF with available information
            from reportlab.pdfgen import canvas
            from reportlab.lib.pagesizes import letter
            
            c = canvas.Canvas(file_path, pagesize=letter)
            c.drawString(100, 750, "Page not found")
            c.drawString(100, 730, f"Product: {product_name}")
            c.drawString(100, 710, f"Date: {recall_date}")
            c.drawString(100, 690, "This recall was listed but the detail page was not accessible.")
            c.save()
            
            logger.info(f"Created fallback PDF: {file_path}")
            return file_path
            
        except Exception as e:
            logger.error(f"Error creating fallback PDF: {e}")
            return None
    
    def save_to_database(self, recalls: List[Dict[str, Any]]):
        """Save recalls to database"""
        logger.info(f"Saving {len(recalls)} recalls to database")
        
        for recall in recalls:
            try:
                # Enrich with AI if available
                if ai_enrichment and recall.get('manufacturer'):
                    enriched_data = ai_enrichment.enrich_company_data(recall['manufacturer'])
                    recall['ai_enriched_content'] = enriched_data
                
                # Convert to regulatory event format
                event_data = {
                    'url': recall.get('source_url', self.base_url),
                    'headline': recall.get('product_name'),
                    'date_published': recall.get('recall_date'),
                    'category': 'Product Recall',
                    'subcategory': recall.get('product_type'),
                    'content': recall.get('reason_for_action'),
                    'product_names': [recall.get('product_name')] if recall.get('product_name') else [],
                    'api_enriched_content': recall.get('ai_enriched_content')
                }
                
                # Save to database
                db_manager.insert_regulatory_event(event_data)
                
            except Exception as e:
                logger.error(f"Error saving recall to database: {e}")

if __name__ == '__main__':
    # Test the enhanced scraper
    scraper = ProductRecallsScraper('./output')
    recalls = scraper.scrape_recalls()
    print(f"Found {len(recalls)} recalls")
    
    for recall in recalls[:5]:  # Show first 5
        print(f"- {recall['product_name']} ({recall['recall_date']})")
        if recall.get('pdf_path'):
            print(f"  PDF: {recall['pdf_path']}")
        if recall.get('manufacturer'):
            print(f"  Manufacturer: {recall['manufacturer']}")
        if recall.get('reason_for_action'):
            print(f"  Reason: {recall['reason_for_action'][:100]}...")
        
    def scrape_recalls(self) -> List[Dict[str, Any]]:
        """
        Scrape all product recalls and save PDFs with proper structure:
        recalls/<Product Name>/Recall_Summary_<Product_Name>_<Date>.pdf
        
        Returns:
            List of recall dictionaries
        """
        logger.info("Starting product recalls scraping")
        recalls = []
        
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                
                # Set longer timeout for slow pages
                page.set_default_timeout(60000)
                
                logger.info(f"Loading recalls page: {self.base_url}")
                page.goto(self.base_url)
                
                # Wait for the page to load
                page.wait_for_load_state('networkidle')
                time.sleep(3)
                
                # Get page content
                content = page.content()
                soup = BeautifulSoup(content, 'html.parser')
                
                # Look for recalls table or list
                recalls_data = self._extract_recalls_from_page(soup)
                
                # Process each recall
                for recall_data in recalls_data:
                    # Process PDF if available
                    if 'pdf_url' in recall_data and recall_data['pdf_url']:
                        pdf_path = self._process_recall_pdf(recall_data, recall_data['pdf_url'])
                        recall_data['pdf_path'] = pdf_path
                    else:
                        # Create "Page Not Found" PDF
                        pdf_path = self._create_not_found_pdf(recall_data)
                        recall_data['pdf_path'] = pdf_path
                    
                    recalls.append(recall_data)
                
                browser.close()
                
        except Exception as e:
            logger.error(f"Error scraping recalls: {e}")
        
        logger.info(f"Scraped {len(recalls)} recalls")
        return recalls
    
    def _extract_recalls_from_page(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Extract recall data from the page"""
        recalls = []
        
        # Look for table or list structures
        table = soup.find('table')
        if not table:
            logger.warning("No recalls table found on page")
            return recalls
        
        # Find all data rows (skip header)
        rows = table.find_all('tr')[1:]  # Skip header row
        
        for row in rows:
            cells = row.find_all(['td', 'th'])
            
            if len(cells) >= 3:  # Minimum columns expected
                try:
                    recall_data = self._extract_recall_from_row(cells)
                    if recall_data:
                        recalls.append(recall_data)
                except Exception as e:
                    logger.error(f"Error processing recall row: {e}")
                    continue
        
        return recalls
    
    def _extract_recall_from_row(self, cells: List) -> Optional[Dict[str, Any]]:
        """Extract recall information from table row cells"""
        try:
            recall_data = {
                'event_type': 'Product Recall',
                'recall_date': None,
                'product_name': None,
                'product_type': None,
                'manufacturer': None,
                'reason_for_action': None,
                'source_url': self.base_url,
                'pdf_url': None,
                'pdf_path': None
            }
            
            # Extract text from each cell
            cell_texts = [cell.get_text(strip=True) for cell in cells]
            
            # Look for PDF links in cells
            for cell in cells:
                links = cell.find_all('a', href=True)
                for link in links:
                    href = link['href']
                    if href.lower().endswith('.pdf'):
                        recall_data['pdf_url'] = href if href.startswith('http') else f"https://fdaghana.gov.gh{href}"
                        break
            
            # Try to identify columns by patterns
            for i, text in enumerate(cell_texts):
                if not text:
                    continue
                
                # Date patterns (likely recall date)
                date_obj = DateParser.parse_date(text)
                if date_obj and not recall_data['recall_date']:
                    recall_data['recall_date'] = date_obj.strftime('%Y-%m-%d')
                
                # Product name (usually longest text or first non-date column)
                elif len(text) > 10 and not recall_data['product_name']:
                    recall_data['product_name'] = TextCleaner.clean_text(text)
                
                # Manufacturer patterns
                elif any(keyword in text.lower() for keyword in ['ltd', 'limited', 'inc', 'corp', 'company', 'pharmaceuticals']):
                    if not recall_data['manufacturer']:
                        recall_data['manufacturer'] = TextCleaner.clean_text(text)
            
            # Set default values if not found
            if not recall_data['product_name']:
                recall_data['product_name'] = f"Product_{int(time.time())}"
            
            if not recall_data['recall_date']:
                recall_data['recall_date'] = datetime.now().strftime('%Y-%m-%d')
            
            return recall_data
            
        except Exception as e:
            logger.error(f"Error extracting recall data: {e}")
            return None
    
    def _process_recall_pdf(self, recall_data: Dict[str, Any], pdf_url: str) -> Optional[str]:
        """
        Download and save recall PDF with proper naming convention:
        recalls/<Product Name>/Recall_Summary_<Product_Name>_<Date>.pdf
        """
        try:
            product_name = recall_data.get('product_name', 'Unknown_Product')
            recall_date = recall_data.get('recall_date', datetime.now().strftime('%Y-%m-%d'))
            
            # Clean product name for folder/filename
            clean_product_name = TextCleaner.clean_filename(product_name)
            
            # Create product-specific folder
            product_folder = f"{self.output_dir}/{clean_product_name}"
            ensure_directory(product_folder)
            
            # Create filename: Recall_Summary_<Product_Name>_<Date>.pdf
            filename = f"Recall_Summary_{clean_product_name}_{recall_date.replace('-', '_')}.pdf"
            file_path = f"{product_folder}/{filename}"
            
            # Download PDF
            response = requests.get(pdf_url, timeout=30)
            response.raise_for_status()
            
            with open(file_path, 'wb') as f:
                f.write(response.content)
            
            logger.info(f"Saved recall PDF: {file_path}")
            return file_path
            
        except Exception as e:
            logger.error(f"Error downloading recall PDF from {pdf_url}: {e}")
            # Create "Page Not Found" PDF
            return self._create_not_found_pdf(recall_data)
    
    def _create_not_found_pdf(self, recall_data: Dict[str, Any]) -> str:
        """
        Create a PDF with 'Page not found' content:
        recalls/<Product Name>/Page_Not_Found_<Product_Name>_<Date>.pdf
        """
        try:
            product_name = recall_data.get('product_name', 'Unknown_Product')
            recall_date = recall_data.get('recall_date', datetime.now().strftime('%Y-%m-%d'))
            
            clean_product_name = TextCleaner.clean_filename(product_name)
            
            # Create product-specific folder
            product_folder = f"{self.output_dir}/{clean_product_name}"
            ensure_directory(product_folder)
            
            # Create filename for not found PDF
            filename = f"Page_Not_Found_{clean_product_name}_{recall_date.replace('-', '_')}.pdf"
            file_path = f"{product_folder}/{filename}"
            
            # Create PDF with "Page not found" content
            from reportlab.pdfgen import canvas
            from reportlab.lib.pagesizes import letter
            
            c = canvas.Canvas(file_path, pagesize=letter)
            c.drawString(100, 750, "Page not found")
            c.drawString(100, 730, f"Product: {product_name}")
            c.drawString(100, 710, f"Date: {recall_date}")
            c.save()
            
            logger.info(f"Created 'Page Not Found' PDF: {file_path}")
            return file_path
            
        except Exception as e:
            logger.error(f"Error creating 'Page Not Found' PDF: {e}")
            return None
    
    def save_to_database(self, recalls: List[Dict[str, Any]]):
        """Save recalls to database"""
        logger.info(f"Saving {len(recalls)} recalls to database")
        
        for recall in recalls:
            try:
                # Enrich with AI if available
                if ai_enrichment and recall.get('manufacturer'):
                    enriched_data = ai_enrichment.enrich_company_data(recall['manufacturer'])
                    recall['ai_enriched_content'] = enriched_data
                
                # Convert to regulatory event format
                event_data = {
                    'url': recall.get('source_url', self.base_url),
                    'headline': recall.get('product_name'),
                    'date_published': recall.get('recall_date'),
                    'category': 'Product Recall',
                    'content': recall.get('reason_for_action'),
                    'pdf_path': recall.get('pdf_path'),
                    'ai_enriched_content': recall.get('ai_enriched_content')
                }
                
                # Save to database
                db_manager.insert_regulatory_event(event_data)
                
            except Exception as e:
                logger.error(f"Error saving recall to database: {e}")

if __name__ == '__main__':
    # Test the scraper
    scraper = ProductRecallsScraper('./output')
    recalls = scraper.scrape_recalls()
    print(f"Found {len(recalls)} recalls")
    
    for recall in recalls[:3]:  # Show first 3
        print(f"- {recall['product_name']} ({recall['recall_date']})")
        if recall.get('pdf_path'):
            print(f"  PDF: {recall['pdf_path']}")
        
    def scrape_recalls(self) -> List[Dict[str, Any]]:
        """
        Scrape all product recalls
        
        Returns:
            List of recall dictionaries
        """
        logger.info("Starting product recalls scraping")
        recalls = []
        
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                
                # Set longer timeout for slow pages
                page.set_default_timeout(60000)
                
                logger.info(f"Loading recalls page: {self.base_url}")
                page.goto(self.base_url)
                
                # Wait for the page to load
                page.wait_for_load_state('networkidle')
                time.sleep(3)
                
                # Check if it's a DataTable that needs JS
                table_selector = 'table'
                if page.query_selector(table_selector):
                    # Try to find "Show All" or pagination controls
                    show_all_selectors = [
                        'select[name*="length"]',
                        '.dataTables_length select',
                        'option[value="-1"]',
                        'option[value="100"]'
                    ]
                    
                    for selector in show_all_selectors:
                        try:
                            element = page.query_selector(selector)
                            if element:
                                if 'option' in selector:
                                    element.click()
                                else:
                                    element.select_option('-1' if page.query_selector('option[value="-1"]') else '100')
                                page.wait_for_timeout(2000)
                                break
                        except Exception as e:
                            logger.debug(f"Could not interact with {selector}: {e}")
                
                # Get page content
                content = page.content()
                browser.close()
                
                # Parse with BeautifulSoup
                soup = BeautifulSoup(content, 'html.parser')
                recalls = self._parse_recalls_table(soup)
                
                logger.info(f"Found {len(recalls)} product recalls")
                
        except Exception as e:
            logger.error(f"Error scraping recalls: {e}")
            return []
        
        return recalls
    
    def _parse_recalls_table(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Parse the recalls table from the HTML"""
        recalls = []
        
        # Find the main table
        table = soup.find('table')
        if not table:
            logger.warning("No recalls table found")
            return recalls
        
        # Find all data rows (skip header)
        rows = table.find_all('tr')[1:]  # Skip header row
        
        for row in rows:
            cells = row.find_all(['td', 'th'])
            
            if len(cells) >= 6:  # Assuming at least 6 columns
                try:
                    recall_data = self._extract_recall_from_row(cells)
                    if recall_data:
                        recalls.append(recall_data)
                except Exception as e:
                    logger.error(f"Error processing recall row: {e}")
                    continue
        
        return recalls
    
    def _extract_recall_from_row(self, cells: List) -> Optional[Dict[str, Any]]:
        """Extract recall information from table row cells"""
        try:
            # Common table structure variations
            # Try to identify columns by content patterns
            recall_data = {
                'event_type': 'Product Recall',
                'recall_date': None,
                'product_name': None,
                'product_type': None,
                'manufacturer': None,
                'recalling_firm': None,
                'batches': None,
                'manufacturing_date': None,
                'expiry_date': None,
                'reason_for_action': None,
                'source_url': self.base_url,
                'pdf_path': None
            }
            
            # Extract text from each cell
            cell_texts = [cell.get_text(strip=True) for cell in cells]
            
            # Try to identify columns by patterns
            for i, text in enumerate(cell_texts):
                if not text:
                    continue
                
                # Date patterns (likely recall date)
                date_obj = DateParser.parse_date(text)
                if date_obj and not recall_data['recall_date']:
                    recall_data['recall_date'] = date_obj.strftime('%Y-%m-%d')
                
                # Product name (usually longer text, not a date)
                elif len(text) > 10 and not DateParser.parse_date(text) and not recall_data['product_name']:
                    recall_data['product_name'] = text
                
                # Look for batch numbers (patterns like Batch, Lot, etc.)
                elif re.search(r'\b(batch|lot|serial)\b', text, re.IGNORECASE):
                    recall_data['batches'] = text
                
                # Look for company names (capitalized words, Ltd, Inc, etc.)
                elif re.search(r'\b(Ltd|Limited|Inc|Corporation|Corp|Pharmaceuticals|Industries)\b', text, re.IGNORECASE):
                    if not recall_data['manufacturer']:
                        recall_data['manufacturer'] = text
                    elif not recall_data['recalling_firm']:
                        recall_data['recalling_firm'] = text
            
            # Look for detail page links
            for cell in cells:
                link = cell.find('a')
                if link and link.get('href'):
                    detail_url = link.get('href')
                    if not detail_url.startswith('http'):
                        detail_url = f"https://fdaghana.gov.gh{detail_url}"
                    
                    # Try to get more details from the detail page
                    detail_info = self._scrape_recall_detail(detail_url)
                    if detail_info:
                        recall_data.update(detail_info)
            
            # Basic validation - must have at least product name
            if recall_data['product_name']:
                return recall_data
            
            return None
            
        except Exception as e:
            logger.error(f"Error extracting recall data: {e}")
            return None
    
    def _scrape_recall_detail(self, detail_url: str) -> Dict[str, Any]:
        """Scrape additional details from recall detail page"""
        detail_info = {}
        
        try:
            logger.info(f"Scraping recall detail: {detail_url}")
            response = requests.get(detail_url, timeout=30, headers={
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
            })
            
            if response.status_code == 404:
                logger.warning(f"Recall detail page not found: {detail_url}")
                # Create fallback PDF for 404
                filename = f"recall_404_{int(time.time())}.pdf"
                pdf_path = self.pdf_processor.create_fallback_pdf(
                    "Page Not Found - 404 Error", filename, detail_url
                )
                detail_info['pdf_path'] = pdf_path
                detail_info['reason_for_action'] = "Page Not Found"
                return detail_info
            
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Look for main content
            content_selectors = [
                '.entry-content',
                '.post-content',
                '.content',
                'article',
                '.main-content'
            ]
            
            content_text = ""
            for selector in content_selectors:
                content_div = soup.select_one(selector)
                if content_div:
                    content_text = content_div.get_text(strip=True)
                    break
            
            if content_text:
                detail_info['reason_for_action'] = content_text[:1000]  # Limit length
                
                # Save as PDF
                filename = f"recall_detail_{int(time.time())}.pdf"
                pdf_path = self.pdf_processor.create_fallback_pdf(
                    content_text, filename, detail_url
                )
                detail_info['pdf_path'] = pdf_path
                
        except Exception as e:
            logger.error(f"Error scraping recall detail from {detail_url}: {e}")
        
        return detail_info
                logger.warning(f"Recall detail page not found: {detail_url}")
                # Create fallback PDF for 404
                filename = f"recall_404_{int(time.time())}.pdf"
                pdf_path = self.pdf_processor.create_fallback_pdf(
                    "Page Not Found - 404 Error", filename, detail_url
                )
                detail_info['pdf_path'] = pdf_path
                detail_info['reason_for_action'] = "Page Not Found"
                return detail_info
            
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Look for main content
            content_selectors = [
                '.entry-content',
                '.post-content',
                '.content',
                'article',
                '.main-content'
            ]
            
            content_text = ""
            for selector in content_selectors:
                content_div = soup.select_one(selector)
                if content_div:
                    content_text = content_div.get_text(strip=True)
                    break
            
            if content_text:
                detail_info['reason_for_action'] = content_text[:1000]  # Limit length
                
                # Save as PDF
                filename = f"recall_detail_{int(time.time())}.pdf"
                pdf_path = self.pdf_processor.create_fallback_pdf(
                    content_text, filename, detail_url
                )
                detail_info['pdf_path'] = pdf_path
            
    def _process_recall_pdf(self, recall_data: Dict[str, Any], pdf_url: str) -> Optional[str]:
        """
        Download and save recall PDF with proper naming convention
        
        Args:
            recall_data: Recall information dictionary
            pdf_url: URL of the PDF to download
            
        Returns:
            Path to saved PDF file
        """
        try:
            product_name = recall_data.get('product_name', 'Unknown_Product')
            recall_date = recall_data.get('recall_date', datetime.now().strftime('%Y-%m-%d'))
            
            # Clean product name for folder/filename
            clean_product_name = TextCleaner.clean_filename(product_name)
            
            # Create product-specific folder
            product_folder = f"{self.output_dir}/{clean_product_name}"
            ensure_directory(product_folder)
            
            # Create filename: Recall_Summary_<Product_Name>_<Date>.pdf
            filename = f"Recall_Summary_{clean_product_name}_{recall_date.replace('-', '_')}.pdf"
            file_path = f"{product_folder}/{filename}"
            
            # Download PDF
            response = requests.get(pdf_url, timeout=30)
            response.raise_for_status()
            
            with open(file_path, 'wb') as f:
                f.write(response.content)
            
            logger.info(f"Saved recall PDF: {file_path}")
            return file_path
            
        except Exception as e:
            logger.error(f"Error downloading recall PDF from {pdf_url}: {e}")
            # Create "Page Not Found" PDF
            return self._create_not_found_pdf(recall_data)
    
    def _create_not_found_pdf(self, recall_data: Dict[str, Any]) -> str:
        """Create a PDF with 'Page not found' content"""
        try:
            product_name = recall_data.get('product_name', 'Unknown_Product')
            recall_date = recall_data.get('recall_date', datetime.now().strftime('%Y-%m-%d'))
            
            clean_product_name = TextCleaner.clean_filename(product_name)
            
            # Create product-specific folder
            product_folder = f"{self.output_dir}/{clean_product_name}"
            ensure_directory(product_folder)
            
            # Create filename for not found PDF
            filename = f"Page_Not_Found_{clean_product_name}_{recall_date.replace('-', '_')}.pdf"
            file_path = f"{product_folder}/{filename}"
            
            # Create PDF with "Page not found" content
            pdf_path = self.pdf_processor.create_fallback_pdf(
                "Page not found", filename, f"Product: {product_name}"
            )
            
            logger.info(f"Created 'Page Not Found' PDF: {file_path}")
            return pdf_path
            
        except Exception as e:
            logger.error(f"Error creating 'Page Not Found' PDF: {e}")
            return None
    
    def save_to_database(self, recalls: List[Dict[str, Any]]):
        """Save recalls to database"""
        logger.info(f"Saving {len(recalls)} recalls to database")
        
        for recall in recalls:
            try:
                # Check if already exists
                if db_manager.check_event_exists('Product Recall', recall['source_url']):
                    logger.info(f"Recall already exists, skipping: {recall.get('product_name', 'Unknown')}")
                    continue
                
                # Create/get companies
                manufacturer_id = None
                recalling_firm_id = None
                
                if recall['manufacturer']:
                    manufacturer_id = db_manager.get_or_create_company(
                        recall['manufacturer'], 'Manufacturer'
                    )
                    # Enrich manufacturer details
                    self._enrich_company(manufacturer_id, recall['manufacturer'], 'Manufacturer')
                
                if recall['recalling_firm'] and recall['recalling_firm'] != recall['manufacturer']:
                    recalling_firm_id = db_manager.get_or_create_company(
                        recall['recalling_firm'], 'Reselling Firm'
                    )
                    # Enrich recalling firm details
                    self._enrich_company(recalling_firm_id, recall['recalling_firm'], 'Reselling Firm')
                
                # Prepare event data
                event_data = {
                    'event_type': 'Product Recall',
                    'recall_date': recall['recall_date'],
                    'product_name': recall['product_name'],
                    'product_type': recall['product_type'],
                    'manufacturer_id': manufacturer_id,
                    'recalling_firm_id': recalling_firm_id,
                    'batches': recall['batches'],
                    'manufacturing_date': recall['manufacturing_date'],
                    'expiry_date': recall['expiry_date'],
                    'source_url': recall['source_url'],
                    'pdf_path': recall['pdf_path'],
                    'reason_for_action': recall['reason_for_action'],
                    'alert_date': None,
                    'alert_name': None,
                    'all_text': None,
                    'notice_date': None,
                    'notice_text': None
                }
                
                # Insert into database
                event_id = db_manager.insert_regulatory_event(event_data)
                logger.info(f"Saved recall {event_id}: {recall.get('product_name', 'Unknown')}")
                
            except Exception as e:
                logger.error(f"Error saving recall to database: {e}")
                continue
    
    def _enrich_company(self, company_id: int, company_name: str, company_type: str):
        """Enrich company with AI details"""
        try:
            enrichment = ai_enrichment.enrich_company(company_name, company_type)
            if any(enrichment.values()):  # If we got any non-null values
                db_manager.update_company_details(company_id, enrichment)
                logger.info(f"Enriched company {company_id}: {company_name}")
        except Exception as e:
            logger.error(f"Error enriching company {company_name}: {e}")

# Import regex for pattern matching
import re
