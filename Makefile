# Ghana FDA Regulatory Scraper Makefile

.PHONY: help setup install test clean run check-db clear-db lint format

# Default target
help:
	@echo "Ghana FDA Regulatory Scraper"
	@echo "============================"
	@echo ""
	@echo "Available commands:"
	@echo "  setup      - Set up development environment"
	@echo "  install    - Install dependencies"
	@echo "  test       - Run tests"
	@echo "  run        - Run the scraper"
	@echo "  check-db   - Check database connection and results"
	@echo "  clear-db   - Clear database (use with caution)"
	@echo "  lint       - Run code linting"
	@echo "  format     - Format code"
	@echo "  clean      - Clean up temporary files"

# Setup development environment
setup:
	@echo "Setting up development environment..."
	python3 -m venv venv
	./venv/bin/pip install -r requirements.txt
	./venv/bin/playwright install
	@echo "Setup complete! Don't forget to:"
	@echo "1. Copy .env.example to .env and configure"
	@echo "2. Create database: createdb ghana_regulatory"
	@echo "3. Run schema: psql -d ghana_regulatory -f schema.sql"

# Install dependencies
install:
	pip install -r requirements.txt
	playwright install

# Install development dependencies
install-dev:
	pip install -r requirements-dev.txt

# Run tests
test:
	@echo "Testing database connection..."
	python test_db_connection.py
	@echo "Running limited scraper test..."
	SCRAPE_LIMIT=1 python run_scraper.py

# Run the scraper
run:
	python run_scraper.py

# Run with limit (for testing)
run-test:
	SCRAPE_LIMIT=5 python run_scraper.py

# Check database
check-db:
	python test_db_connection.py
	python check_db_results.py

# Clear database (use with caution)
clear-db:
	@echo "⚠️  This will delete all data! Press Ctrl+C to cancel, Enter to continue..."
	@read
	python clear_db.py

# Fix duplicates
fix-duplicates:
	python fix_duplicates.py

# Monitor scraping (run in separate terminal)
monitor:
	python monitor_scrape.py

# Code linting
lint:
	flake8 *.py --max-line-length=120 --ignore=E501,W503

# Code formatting
format:
	black *.py --line-length=120
	isort *.py

# Clean up
clean:
	rm -rf __pycache__/
	rm -rf .pytest_cache/
	rm -rf *.pyc
	rm -rf .coverage
	rm -rf htmlcov/
	rm -f ghana_scraper.log

# Database operations
db-schema:
	psql -d ghana_regulatory -f schema.sql

db-backup:
	pg_dump ghana_regulatory > backup_$(shell date +%Y%m%d_%H%M%S).sql

# Development helpers
dev-setup: setup install-dev
	@echo "Development environment ready!"

# Production deployment helpers
prod-check:
	@echo "Checking production readiness..."
	python test_db_connection.py
	@echo "✅ Production checks passed"

# Show logs
logs:
	tail -f ghana_scraper.log

# Show recent database activity
db-activity:
	python -c "
import psycopg2
from psycopg2.extras import RealDictCursor
conn = psycopg2.connect(host='localhost', database='ghana_regulatory', user='divyanshsingh', port=5432, cursor_factory=RealDictCursor)
cursor = conn.cursor()
cursor.execute('SELECT event_type, COUNT(*) as count FROM regulatory_events GROUP BY event_type ORDER BY count DESC')
for row in cursor.fetchall():
    print(f'{row[\"event_type\"]}: {row[\"count\"]} records')
conn.close()
"