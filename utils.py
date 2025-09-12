"""
Utilities for date parsing, text extraction, and file handling
"""
import os
import re
import logging
from datetime import datetime
from typing import Optional, List
import requests
from urllib.parse import urljoin, urlparse
import PyPDF2
import io
from pdf2image import convert_from_bytes
import pytesseract
from PIL import Image
import hashlib

logger = logging.getLogger(__name__)

class DateParser:
    """Handles various date format parsing"""
    
    DATE_FORMATS = [
        '%d/%m/%Y',     # 15/03/2023
        '%d-%m-%Y',     # 15-03-2023
        '%Y-%m-%d',     # 2023-03-15
        '%Y/%m/%d',     # 2023/03/15
        '%d %B %Y',     # 15 March 2023
        '%d %b %Y',     # 15 Mar 2023
        '%B %d, %Y',    # March 15, 2023
        '%b %d, %Y',    # Mar 15, 2023
        '%Y',           # 2023
        '%Y-%m',        # 2023-03
    ]
    
    @classmethod
    def parse_date(cls, date_str: str) -> Optional[datetime]:
        """
        Parse date string using multiple formats
        
        Args:
            date_str: Date string to parse
            
        Returns:
            datetime object or None if parsing fails
        """
        if not date_str or not isinstance(date_str, str):
            return None
        
        # Clean the date string
        date_str = date_str.strip()
        
        # Remove common prefixes
        date_str = re.sub(r'^(date[:\s]*|on[:\s]*)', '', date_str, flags=re.IGNORECASE)
        
        for fmt in cls.DATE_FORMATS:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        # Try to extract year if nothing else works
        year_match = re.search(r'\b(19|20)\d{2}\b', date_str)
        if year_match:
            try:
                return datetime(int(year_match.group()), 1, 1)
            except ValueError:
                pass
        
        logger.warning(f"Could not parse date: {date_str}")
        return None

class PDFProcessor:
    """Handles PDF downloading and text extraction"""
    
    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        
    def download_pdf(self, url: str, filename: str) -> Optional[str]:
        """
        Download PDF from URL
        
        Args:
            url: URL to download from
            filename: Local filename to save as
            
        Returns:
            Local file path or None if failed
        """
        try:
            response = requests.get(url, timeout=30, headers={
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
            })
            response.raise_for_status()
            
            # Check if it's actually a PDF
            content_type = response.headers.get('content-type', '').lower()
            if 'application/pdf' in content_type:
                filepath = os.path.join(self.output_dir, filename)
                os.makedirs(os.path.dirname(filepath), exist_ok=True)
                
                with open(filepath, 'wb') as f:
                    f.write(response.content)
                
                logger.info(f"Downloaded PDF: {filepath}")
                return filepath
            else:
                # It's not a PDF, might be HTML (404 page)
                logger.warning(f"URL returned non-PDF content: {url}")
                return self.create_fallback_pdf(response.text, filename, url)
                
        except Exception as e:
            logger.error(f"Failed to download PDF from {url}: {e}")
            return self.create_fallback_pdf(f"Failed to download: {e}", filename, url)
    
    def create_fallback_pdf(self, content: str, filename: str, original_url: str) -> str:
        """
        Create a fallback PDF for failed downloads or HTML content
        
        Args:
            content: Text content to include
            filename: Filename for the PDF
            original_url: Original URL that failed
            
        Returns:
            Path to created PDF
        """
        try:
            from reportlab.pdfgen import canvas
            from reportlab.lib.pagesizes import letter
            from reportlab.lib import colors
            
            filepath = os.path.join(self.output_dir, filename)
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            
            c = canvas.Canvas(filepath, pagesize=letter)
            width, height = letter
            
            # Title
            c.setFont("Helvetica-Bold", 14)
            c.drawString(50, height - 50, "FDA Ghana - Document Not Available")
            
            # URL
            c.setFont("Helvetica", 10)
            c.drawString(50, height - 80, f"Original URL: {original_url}")
            
            # Content (truncated if too long)
            c.setFont("Helvetica", 10)
            y_position = height - 120
            
            # Clean HTML if present
            if '<html' in content.lower() or '<div' in content.lower():
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(content, 'html.parser')
                content = soup.get_text()
            
            content = content[:1000] + "..." if len(content) > 1000 else content
            
            for line in content.split('\n')[:20]:  # Max 20 lines
                if y_position < 50:
                    break
                c.drawString(50, y_position, line[:80])  # Max 80 chars per line
                y_position -= 15
            
            c.save()
            logger.info(f"Created fallback PDF: {filepath}")
            return filepath
            
        except ImportError:
            # Fallback to simple text file if reportlab not available
            filepath = os.path.join(self.output_dir, filename.replace('.pdf', '.txt'))
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(f"FDA Ghana - Document Not Available\n")
                f.write(f"Original URL: {original_url}\n\n")
                f.write(content[:1000])
            
            logger.info(f"Created fallback text file: {filepath}")
            return filepath
        except Exception as e:
            logger.error(f"Failed to create fallback PDF: {e}")
            return None
    
    def extract_text_from_pdf(self, pdf_path: str) -> str:
        """
        Extract text from PDF using multiple methods
        
        Args:
            pdf_path: Path to PDF file
            
        Returns:
            Extracted text
        """
        text = ""
        
        # Method 1: Try PyPDF2
        try:
            with open(pdf_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                for page in pdf_reader.pages:
                    text += page.extract_text() + "\n"
            
            # If we got good text, return it
            if len(text.strip()) > 50:
                logger.info(f"Extracted {len(text)} characters using PyPDF2")
                return text.strip()
                
        except Exception as e:
            logger.warning(f"PyPDF2 extraction failed for {pdf_path}: {e}")
        
        # Method 2: OCR with pdf2image + pytesseract
        try:
            logger.info(f"Attempting OCR extraction for {pdf_path}")
            
            with open(pdf_path, 'rb') as file:
                pdf_bytes = file.read()
            
            # Convert first 10 pages to images
            images = convert_from_bytes(pdf_bytes, first_page=1, last_page=10)
            
            ocr_text = ""
            for i, image in enumerate(images):
                logger.info(f"Processing page {i+1} with OCR")
                page_text = pytesseract.image_to_string(image)
                ocr_text += page_text + "\n"
            
            if len(ocr_text.strip()) > 20:
                logger.info(f"Extracted {len(ocr_text)} characters using OCR")
                return ocr_text.strip()
                
        except Exception as e:
            logger.error(f"OCR extraction failed for {pdf_path}: {e}")
        
        # If all methods fail, return minimal text
        logger.warning(f"Could not extract text from {pdf_path}")
        return f"Text extraction failed for file: {os.path.basename(pdf_path)}"

class TextCleaner:
    """Utilities for cleaning and processing text"""
    
    @staticmethod
    def clean_filename(filename: str) -> str:
        """Clean filename for safe saving"""
        # Remove invalid characters
        filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
        # Remove multiple spaces and underscores
        filename = re.sub(r'[_\s]+', '_', filename)
        # Trim and ensure max length
        filename = filename.strip('_')[:100]
        return filename
    
    @staticmethod
    def extract_company_names(text: str) -> List[str]:
        """Extract potential company names from text"""
        # Common patterns for company names
        patterns = [
            r'\b([A-Z][a-zA-Z\s&]+(?:Ltd|Limited|Inc|Corporation|Corp|Co|Company)\.?)\b',
            r'\b([A-Z][a-zA-Z\s&]+(?:Pharmaceuticals|Pharma|Industries|Manufacturing))\b',
            r'\b([A-Z][a-zA-Z\s&]+(?:Group|Holdings|Enterprises))\b'
        ]
        
        companies = set()
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            companies.update(matches)
        
        return list(companies)
    
    @staticmethod
    def generate_filename_from_title(title: str, date_str: str = None, extension: str = 'pdf') -> str:
        """Generate filename from title and date"""
        clean_title = TextCleaner.clean_filename(title)
        if date_str:
            clean_date = DateParser.parse_date(date_str)
            if clean_date:
                date_part = clean_date.strftime('%Y-%m-%d')
                return f"{clean_title}_{date_part}.{extension}"
        
        return f"{clean_title}.{extension}"
    
    @staticmethod
    def create_content_hash(content: str) -> str:
        """Create hash for content deduplication"""
        return hashlib.md5(content.encode('utf-8')).hexdigest()[:12]

def setup_logging(log_level: str = 'INFO'):
    """Setup logging configuration"""
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('scraper.log'),
            logging.StreamHandler()
        ]
    )

def ensure_directory(directory: str):
    """Ensure directory exists"""
    os.makedirs(directory, exist_ok=True)
