#!/usr/bin/env python3
"""
Ghana FDA Regulatory Scraper - Main Runner
Simple script to run the unified scraper
"""
import sys
import logging
from datetime import datetime
from ghana_scraper_unified import GhanaRegulatoryScraperUnified

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ghana_scraper.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

def main():
    """Run the Ghana regulatory scraper"""
    print("🇬🇭 Ghana FDA Regulatory Scraper")
    print("=" * 50)
    print("📋 Scraping: Product Recalls")
    print("⚠️ Scraping: Product Alerts") 
    print("📢 Scraping: Public Notices")
    print("💾 Saving to: safetyiq Database")
    print("=" * 50)
    
    start_time = datetime.now()
    
    try:
        # Create and run the unified scraper
        scraper = GhanaRegulatoryScraperUnified('./output')
        results = scraper.scrape_all_ghana_data()

        # Show results
        end_time = datetime.now()
        duration = end_time - start_time

        print("\n🎯 SCRAPING COMPLETE!")
        print("=" * 50)
        for category, items in results.items():
            print(f"📊 {category.title()}: {len(items)} items")

        total_items = sum(len(items) for items in results.values())
        print(f"📁 Total items: {total_items}")
        print(f"⏱️ Duration: {duration}")
        print(f"🗂️ Files saved to: ./output/")
        print(f"🗄️ Database: safetyiq (Ghana focus)")
        print("=" * 50)

        return 0

    except Exception as e:
        logging.error(f"Scraper failed: {e}")
        print(f"❌ Error: {e}")
        return 1

if __name__ == '__main__':
    sys.exit(main())
