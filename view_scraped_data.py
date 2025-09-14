#!/usr/bin/env python3
"""
View scraped data from safetyiq database
"""
import psycopg2
from psycopg2.extras import RealDictCursor
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def view_scraped_data():
    """View the scraped regulatory data"""
    
    db_config = {
        'host': 'localhost',
        'database': 'safetyiq',
        'user': 'sanatanupmanyu',
        'password': 'ksDq2jazKmxxzv.VxXbkwR6Uxz',
        'port': 5432
    }
    
    try:
        conn = psycopg2.connect(**db_config, cursor_factory=RealDictCursor)
        cursor = conn.cursor()
        
        print("🇬🇭 Ghana FDA Scraped Data in SafetyIQ Database")
        print("=" * 60)
        
        # Get summary counts
        cursor.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE event_type = 'Product Recall') as recalls,
                COUNT(*) FILTER (WHERE event_type = 'Alert') as alerts,
                COUNT(*) FILTER (WHERE event_type = 'Public Notice') as notices,
                COUNT(*) as total
            FROM safetydb.regulatory_events
        """)
        summary = cursor.fetchone()
        
        print(f"📊 Summary:")
        print(f"  - Product Recalls: {summary['recalls']}")
        print(f"  - Alerts: {summary['alerts']}")
        print(f"  - Public Notices: {summary['notices']}")
        print(f"  - Total Events: {summary['total']}")
        
        # Show recent recalls
        print(f"\n📦 Recent Product Recalls:")
        cursor.execute("""
            SELECT product_name, recall_date, batches, 
                   c1.name as manufacturer, c2.name as recalling_firm
            FROM safetydb.regulatory_events re
            LEFT JOIN safetydb.companies c1 ON re.manufacturer_id = c1.id
            LEFT JOIN safetydb.companies c2 ON re.recalling_firm_id = c2.id
            WHERE event_type = 'Product Recall' AND product_name IS NOT NULL
            ORDER BY re.created_at DESC
            LIMIT 10
        """)
        recalls = cursor.fetchall()
        
        for i, recall in enumerate(recalls, 1):
            print(f"  {i}. {recall['product_name']}")
            if recall['manufacturer']:
                print(f"     Manufacturer: {recall['manufacturer']}")
            if recall['batches']:
                print(f"     Batches: {recall['batches']}")
            if recall['recall_date']:
                print(f"     Recall Date: {recall['recall_date']}")
            print()
        
        # Show alerts
        print(f"⚠️  Recent Alerts:")
        cursor.execute("""
            SELECT alert_name, alert_date, product_name
            FROM safetydb.regulatory_events
            WHERE event_type = 'Alert' AND alert_name IS NOT NULL
            ORDER BY created_at DESC
            LIMIT 5
        """)
        alerts = cursor.fetchall()
        
        for i, alert in enumerate(alerts, 1):
            print(f"  {i}. {alert['alert_name']}")
            if alert['product_name']:
                print(f"     Product: {alert['product_name']}")
            if alert['alert_date']:
                print(f"     Date: {alert['alert_date']}")
            print()
        
        # Show companies
        print(f"🏢 Companies in Database:")
        cursor.execute("""
            SELECT name, country_of_origin, 
                   COUNT(re1.id) as recalls_as_manufacturer,
                   COUNT(re2.id) as recalls_as_recalling_firm
            FROM safetydb.companies c
            LEFT JOIN safetydb.regulatory_events re1 ON c.id = re1.manufacturer_id
            LEFT JOIN safetydb.regulatory_events re2 ON c.id = re2.recalling_firm_id
            GROUP BY c.id, c.name, c.country_of_origin
            ORDER BY (COUNT(re1.id) + COUNT(re2.id)) DESC
        """)
        companies = cursor.fetchall()
        
        for company in companies:
            total_recalls = company['recalls_as_manufacturer'] + company['recalls_as_recalling_firm']
            if total_recalls > 0:
                print(f"  • {company['name']} ({company['country_of_origin']})")
                print(f"    - Recalls as Manufacturer: {company['recalls_as_manufacturer']}")
                print(f"    - Recalls as Recalling Firm: {company['recalls_as_recalling_firm']}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"❌ Error viewing data: {e}")

if __name__ == '__main__':
    view_scraped_data()