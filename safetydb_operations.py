#!/usr/bin/env python3
"""
Demonstrate storing and fetching data from safetydb schema
"""
import psycopg2
from psycopg2.extras import RealDictCursor
import uuid
from datetime import datetime, date
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SafetyDBOperations:
    """Class to handle operations on safetydb schema"""
    
    def __init__(self):
        self.db_config = {
            'host': 'localhost',
            'database': 'African_Country',
            'user': 'divyanshsingh',
            'port': 5432
        }
    
    def get_connection(self):
        """Get database connection"""
        return psycopg2.connect(**self.db_config, cursor_factory=RealDictCursor)
    
    def store_country(self, code, name, region=None, regulatory_maturity='developing'):
        """Store a country in safetydb.countries"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO safetydb.countries (code, name, region, regulatory_maturity)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (code) DO UPDATE SET
                            name = EXCLUDED.name,
                            region = EXCLUDED.region,
                            regulatory_maturity = EXCLUDED.regulatory_maturity
                        RETURNING id, code, name
                    """, (code, name, region, regulatory_maturity))
                    
                    result = cursor.fetchone()
                    conn.commit()
                    print(f"✅ Stored country: {result['name']} ({result['code']})")
                    return result['id']
        except Exception as e:
            logger.error(f"Error storing country: {e}")
            return None
    
    def store_company(self, name, country_code=None, established_year=None, 
                     company_size='medium', risk_score=0):
        """Store a company in safetydb.companies"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO safetydb.companies (
                            name, country_of_origin, established_year, 
                            company_size, risk_score
                        )
                        VALUES (%s, %s, %s, %s, %s)
                        RETURNING id, name
                    """, (name, country_code, established_year, company_size, risk_score))
                    
                    result = cursor.fetchone()
                    conn.commit()
                    print(f"✅ Stored company: {result['name']}")
                    return result['id']
        except Exception as e:
            logger.error(f"Error storing company: {e}")
            return None
    
    def store_regulatory_event(self, url, headline, date_published=None, 
                              category=None, event_type='Alert'):
        """Store a regulatory event in safetydb.regulatory_events"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO safetydb.regulatory_events (
                            url, headline, date_published, category, event_type
                        )
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (url) DO UPDATE SET
                            headline = EXCLUDED.headline,
                            date_published = EXCLUDED.date_published,
                            category = EXCLUDED.category,
                            event_type = EXCLUDED.event_type
                        RETURNING id, headline
                    """, (url, headline, date_published, category, event_type))
                    
                    result = cursor.fetchone()
                    conn.commit()
                    print(f"✅ Stored regulatory event: {result['headline']}")
                    return result['id']
        except Exception as e:
            logger.error(f"Error storing regulatory event: {e}")
            return None
    
    def fetch_all_countries(self):
        """Fetch all countries from safetydb.countries"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT id, code, name, region, regulatory_maturity, created_at
                        FROM safetydb.countries
                        ORDER BY name
                    """)
                    return cursor.fetchall()
        except Exception as e:
            logger.error(f"Error fetching countries: {e}")
            return []
    
    def fetch_all_companies(self):
        """Fetch all companies from safetydb.companies"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT id, name, country_of_origin, established_year, 
                               company_size, risk_score, created_at
                        FROM safetydb.companies
                        ORDER BY name
                    """)
                    return cursor.fetchall()
        except Exception as e:
            logger.error(f"Error fetching companies: {e}")
            return []
    
    def fetch_all_regulatory_events(self):
        """Fetch all regulatory events from safetydb.regulatory_events"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT id, url, headline, date_published, category, 
                               event_type, created_at
                        FROM safetydb.regulatory_events
                        ORDER BY date_published DESC, created_at DESC
                    """)
                    return cursor.fetchall()
        except Exception as e:
            logger.error(f"Error fetching regulatory events: {e}")
            return []
    
    def get_summary(self):
        """Get summary of all data in safetydb schema"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT 
                            (SELECT COUNT(*) FROM safetydb.countries) as countries,
                            (SELECT COUNT(*) FROM safetydb.companies) as companies,
                            (SELECT COUNT(*) FROM safetydb.regulatory_events) as regulatory_events
                    """)
                    return cursor.fetchone()
        except Exception as e:
            logger.error(f"Error getting summary: {e}")
            return None

def demo_operations():
    """Demonstrate storing and fetching data"""
    db = SafetyDBOperations()
    
    print("🎯 SafetyDB Operations Demo")
    print("=" * 30)
    
    # Store sample data
    print("\n📝 Storing Sample Data:")
    
    # Store countries
    ghana_id = db.store_country('GH', 'Ghana', 'West Africa', 'intermediate')
    usa_id = db.store_country('US', 'United States', 'North America', 'advanced')
    
    # Store companies
    company1_id = db.store_company('Ghana Pharma Ltd', 'GH', 2010, 'medium', 25)
    company2_id = db.store_company('SafeMed Corp', 'US', 2005, 'large', 10)
    
    # Store regulatory events
    event1_id = db.store_regulatory_event(
        'https://fda.gov.gh/alert/001',
        'Product Recall: Contaminated Medicine Batch',
        date.today(),
        'Product Recall',
        'Product Recall'
    )
    
    event2_id = db.store_regulatory_event(
        'https://fda.gov.gh/notice/002',
        'New Safety Guidelines for Pharmaceutical Companies',
        date.today(),
        'Public Notice',
        'Public Notice'
    )
    
    # Fetch and display data
    print("\n📊 Fetching Data from SafetyDB:")
    
    # Countries
    countries = db.fetch_all_countries()
    print(f"\n🌍 Countries ({len(countries)}):")
    for country in countries:
        print(f"  - {country['name']} ({country['code']}) - {country['regulatory_maturity']}")
    
    # Companies
    companies = db.fetch_all_companies()
    print(f"\n🏢 Companies ({len(companies)}):")
    for company in companies:
        print(f"  - {company['name']} ({company['country_of_origin']}) - Risk: {company['risk_score']}")
    
    # Regulatory Events
    events = db.fetch_all_regulatory_events()
    print(f"\n📋 Regulatory Events ({len(events)}):")
    for event in events:
        print(f"  - {event['headline']} ({event['event_type']})")
    
    # Summary
    summary = db.get_summary()
    print(f"\n📈 Summary:")
    print(f"  - Countries: {summary['countries']}")
    print(f"  - Companies: {summary['companies']}")
    print(f"  - Regulatory Events: {summary['regulatory_events']}")

if __name__ == '__main__':
    demo_operations()