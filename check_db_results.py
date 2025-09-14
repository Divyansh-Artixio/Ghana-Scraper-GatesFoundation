#!/usr/bin/env python3
"""
Check database results after scraping
"""
import psycopg2
from psycopg2.extras import RealDictCursor
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_database_results():
    """Check what was saved to the database"""
    
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
        
        print("🔍 Database Results Summary")
        print("=" * 50)
        
        # Count by event type
        cursor.execute("""
            SELECT event_type, COUNT(*) as count
            FROM regulatory_events 
            GROUP BY event_type
            ORDER BY count DESC
        """)
        results = cursor.fetchall()
        
        total_events = 0
        for row in results:
            print(f"📊 {row['event_type']}: {row['count']} records")
            total_events += row['count']
        
        print(f"📁 Total Events: {total_events}")
        
        # Show recent events
        print("\n🕒 Recent Events:")
        print("-" * 30)
        cursor.execute("""
            SELECT event_type, 
                   COALESCE(alert_name, notice_text, product_name) as title,
                   COALESCE(alert_date, notice_date, recall_date) as event_date,
                   created_at
            FROM regulatory_events 
            ORDER BY created_at DESC 
            LIMIT 10
        """)
        recent = cursor.fetchall()
        
        for event in recent:
            title = (event['title'] or 'Untitled')[:50]
            print(f"• {event['event_type']}: {title} ({event['event_date']})")
        
        # Company stats
        print(f"\n🏢 Company Records:")
        print("-" * 20)
        cursor.execute("SELECT COUNT(*) as count FROM companies")
        company_count = cursor.fetchone()['count']
        print(f"📈 Total Companies: {company_count}")
        
        cursor.execute("""
            SELECT type, COUNT(*) as count 
            FROM companies 
            GROUP BY type
        """)
        company_types = cursor.fetchall()
        for ctype in company_types:
            print(f"  - {ctype['type']}: {ctype['count']}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error checking database: {e}")

if __name__ == '__main__':
    check_database_results()