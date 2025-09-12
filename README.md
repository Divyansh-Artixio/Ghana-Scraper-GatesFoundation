# Ghana FDA Regulatory Events Scraper

A comprehensive Python scraper and ETL pipeline that extracts **Product Recalls, Alerts, and Public Notices** from FDA Ghana's newsroom and stores them in a normalized PostgreSQL database structure.

## 🎯 Features

- **Multi-content scraping**: Product Recalls, Alerts, and Public Notices
- **Robust PDF handling**: Direct PDFs, HTML fallbacks, OCR extraction
- **Error resilience**: Handles 404s, broken PDFs, missing content
- **Normalized database**: Separate tables for events, companies, countries
- **AI enrichment**: Automatic company metadata enhancement (optional)
- **Comprehensive logging**: Full audit trail of scraping activities
- **Duplicate prevention**: Smart company deduplication and reason extraction
- **Multi-product support**: Handles complex multi-product recalls

## 📊 Latest Results

- ✅ **77 Product Recalls** with detailed reasons
- ✅ **29 Product Alerts** 
- ✅ **67 Public Notices**
- ✅ **173 Total Records** processed in ~20 minutes
- ✅ **46 Unique Companies** (no duplicates)
- ✅ **All PDF files** downloaded and organized

## 📂 Database Schema

The scraper uses a normalized PostgreSQL database with three main tables:

### `regulatory_events`
Stores all regulatory events (recalls, alerts, notices) with event-specific fields including:
- Event type, dates, product information
- Company relationships (manufacturer_id, recalling_firm_id)
- Detailed recall reasons and actions
- PDF paths and source URLs

### `companies` 
Stores company information with optional AI-enriched metadata:
- Company name, country, type (Manufacturer/Reselling Firm)
- Founding dates, founders, business descriptions (AI-enhanced)

### `countries`
Reference table with country codes and WHO maturity levels.

## 🚀 Quick Start

### 1. Prerequisites

- Python 3.8+
- PostgreSQL 12+
- Chrome/Chromium browser (for Playwright)

### 2. Installation

```bash
# Clone the repository
git clone https://github.com/divyanshwrite/Ghana.git
cd Ghana

# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Install Playwright browsers
playwright install
```

### 3. Database Setup

```bash
# Create PostgreSQL database
createdb ghana_regulatory

# Run the schema script
psql -d ghana_regulatory -f schema.sql
```

### 4. Configuration

```bash
# Copy environment template
cp .env.example .env
```

Edit `.env` with your settings:
```env
# Database Configuration
DB_HOST=localhost
DB_NAME=ghana_regulatory
DB_USER=your_username
DB_PASSWORD=your_password
DB_PORT=5432

# AI Provider (Optional - for company enrichment)
OPENAI_API_KEY=your_openai_key
# OR
ANTHROPIC_API_KEY=your_anthropic_key
AI_PROVIDER=openai  # or 'anthropic'

# Scraping Configuration
SCRAPE_LIMIT=  # Leave empty for no limit, or set number per category
HEADLESS_BROWSER=true
LOG_LEVEL=INFO
OUTPUT_DIR=./output
```

### 5. Test the Setup

```bash
# Test database connection
python test_db_connection.py

# Test with limited scraping
SCRAPE_LIMIT=2 python run_scraper.py
```

### 6. Run Full Scraper

```bash
# Run complete scraper
python run_scraper.py
```

## 📋 Command Line Options

```bash
# Basic usage
python run_scraper.py

# With environment variables
SCRAPE_LIMIT=10 LOG_LEVEL=DEBUG python run_scraper.py

# Test database connection
python test_db_connection.py

# Check results after scraping
python check_db_results.py

# Clear database (use with caution)
python clear_db.py

# Fix duplicate companies (if needed)
python fix_duplicates.py

# Monitor scraping progress (run in separate terminal)
python monitor_scrape.py
```

## 🎯 Target URLs

The scraper extracts data from these FDA Ghana pages:

- **Product Recalls**: `https://fdaghana.gov.gh/newsroom/product-recalls-and-alerts/`
- **Product Alerts**: `https://fdaghana.gov.gh/newsroom/product-alerts/`
- **Public Notices**: 
  - `https://fdaghana.gov.gh/newsroom/press-release/`
  - `https://fdaghana.gov.gh/newsroom/press-release-2/`

## 🛠 How It Works

### 1. Product Recalls Scraper
- Uses Playwright to handle JavaScript-rendered tables
- Extracts recall details: dates, product names, manufacturers, batch numbers
- Follows detail page links for additional information
- Handles multi-product recalls by splitting them into individual records
- Extracts detailed recall reasons from content
- Creates organized PDF files for each product

### 2. Product Alerts Scraper  
- Handles DataTable pagination with "Show All" functionality
- Downloads PDFs or extracts HTML content
- Uses OCR (pdf2image + pytesseract) for text extraction when PyPDF2 fails
- Stores all extracted text in the database

### 3. Public Notices Scraper
- Scrapes multiple press release pages
- Handles pagination automatically
- Extracts content from both PDF links and HTML pages
- Creates structured fallback content for missing pages

### 4. Company Management
- Smart deduplication prevents duplicate company entries
- Links companies to regulatory events via foreign keys
- Supports both manufacturers and reselling firms
- Optional AI enrichment for company metadata

### 5. Error Handling
- **404 Pages**: Creates fallback PDFs with "Page Not Found" content
- **PDF Extraction Failures**: Falls back to OCR processing
- **Network Issues**: Retries and logs all failures
- **Database Errors**: Continues processing other records
- **Duplicate Prevention**: Smart company and event deduplication

## 📁 File Organization

```
output/
├── recalls/           # Product recall PDFs (organized by product/company)
├── alerts/            # Product alert PDFs (organized by title/date)
└── notices/           # Public notice PDFs (organized by title/date)

logs/
└── ghana_scraper.log  # Comprehensive scraping logs
```

## 🔧 Advanced Usage

### Environment Variables

```bash
# Limit scraping for testing
export SCRAPE_LIMIT=5

# Custom output directory
export OUTPUT_DIR="/path/to/custom/output"

# Logging levels
export LOG_LEVEL=DEBUG  # INFO, WARNING, ERROR

# Browser visibility (for debugging)
export HEADLESS_BROWSER=false
```

### Database Queries

```sql
-- View recent events
SELECT event_type, 
       COALESCE(alert_name, notice_text, product_name) as title,
       COALESCE(alert_date, notice_date, recall_date) as event_date,
       reason_for_action
FROM regulatory_events 
ORDER BY created_at DESC 
LIMIT 10;

-- Company statistics  
SELECT type, COUNT(*) as count, 
       COUNT(founding_date) as enriched_count
FROM companies 
GROUP BY type;

-- Events by type with reasons
SELECT event_type, COUNT(*) as count,
       COUNT(reason_for_action) as with_reasons
FROM regulatory_events  
GROUP BY event_type;

-- Products by manufacturer
SELECT c.name as manufacturer, COUNT(*) as products_recalled
FROM regulatory_events re
JOIN companies c ON re.manufacturer_id = c.id
WHERE re.event_type = 'Product Recall'
GROUP BY c.name
ORDER BY products_recalled DESC;
```

## 🚨 Troubleshooting

### Common Issues

1. **Database Connection Errors**
   ```bash
   # Check PostgreSQL is running
   pg_ctl status
   
   # Test connection
   python test_db_connection.py
   ```

2. **PDF Extraction Failures**
   ```bash
   # Install Tesseract OCR
   # macOS
   brew install tesseract
   
   # Ubuntu/Debian
   sudo apt-get install tesseract-ocr
   
   # Windows
   # Download from: https://github.com/UB-Mannheim/tesseract/wiki
   ```

3. **Browser/Playwright Issues**
   ```bash
   # Reinstall browsers
   playwright install
   
   # For debugging, run with visible browser
   HEADLESS_BROWSER=false python run_scraper.py
   ```

4. **Memory Issues with Large Scrapes**
   ```bash
   # Use limits for testing
   SCRAPE_LIMIT=10 python run_scraper.py
   
   # Monitor progress
   python monitor_scrape.py
   ```

### Logs Location
All scraping activity is logged to `ghana_scraper.log` in the project directory.

## 📈 Performance

- **Processing Speed**: ~173 records in 20 minutes
- **Success Rate**: 99%+ (handles failures gracefully)
- **Memory Usage**: Optimized for large datasets
- **Database**: Normalized structure for efficient queries
- **PDF Storage**: Organized folder structure for easy access

## 🤝 Contributing

### Development Setup

```bash
# Install development dependencies
pip install -r requirements.txt

# Run tests
python test_db_connection.py

# Check code style
flake8 *.py

# Run with debug logging
LOG_LEVEL=DEBUG python run_scraper.py
```

### Key Areas for Enhancement

1. **Additional Content Types**: Extend to scrape other regulatory content
2. **Enhanced AI Prompts**: Improve company enrichment accuracy
3. **Data Validation**: Add more robust data cleaning and validation
4. **Performance**: Optimize for large-scale scraping
5. **Monitoring**: Add health checks and alerting
6. **API Integration**: Add REST API for data access

## 📄 File Structure

```
Ghana/
├── README.md                 # This file
├── requirements.txt          # Python dependencies
├── schema.sql               # Database schema
├── .env.example             # Environment template
├── run_scraper.py           # Main scraper runner
├── ghana_scraper_unified.py # Core scraper logic
├── utils.py                 # Utility functions
├── database.py              # Database operations
├── ai_enrichment.py         # AI enhancement features
├── test_db_connection.py    # Database testing
├── check_db_results.py      # Results verification
├── clear_db.py              # Database cleanup
├── fix_duplicates.py        # Duplicate resolution
├── monitor_scrape.py        # Progress monitoring
└── output/                  # Downloaded files
    ├── recalls/
    ├── alerts/
    └── notices/
```

## 📝 License

This project is designed for regulatory compliance and public health monitoring purposes. Please ensure compliance with website terms of service and local regulations when scraping public data.

## 🔗 Links

- **FDA Ghana Website**: https://fdaghana.gov.gh/
- **GitHub Repository**: https://github.com/divyanshwrite/Ghana
- **Issues**: https://github.com/divyanshwrite/Ghana/issues

---

**Note**: This scraper respects robots.txt and implements reasonable delays between requests. Always ensure compliance with website terms of service and local regulations when scraping public data.