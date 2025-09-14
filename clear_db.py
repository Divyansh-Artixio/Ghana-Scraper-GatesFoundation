#!/usr/bin/env python3
"""
Clear database data before full scrape
"""
import psycopg2
from psycopg2.extras import RealDictCursor
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def clear_database():
    """Clear all data from regulatory tables"""
    
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
        
        print("🗑️  Clearing Database Data")
        print("=" * 30)
        
        # Get current counts
        cursor.execute("SELECT COUNT(*) as count FROM regulatory_events")
        events_count = cursor.fetchone()['count']
        
        cursor.execute("SELECT COUNT(*) as count FROM companies")
        companies_count = cursor.fetchone()['count']
        
        print(f"📊 Current data:")
        print(f"  - Regulatory Events: {events_count}")
        print(f"  - Companies: {companies_count}")
        
        if events_count == 0 and companies_count == 0:
            print("✅ Database is already empty!")
            return
        
        # Clear regulatory_events first (due to foreign key constraints)
        print("\n🧹 Clearing regulatory_events...")
        cursor.execute("DELETE FROM regulatory_events")
        deleted_events = cursor.rowcount
        
        # Clear companies
        print("🧹 Clearing companies...")
        cursor.execute("DELETE FROM companies WHERE id > 0")  # Keep any default entries
        deleted_companies = cursor.rowcount
        
        # Reset sequences
        print("🔄 Resetting sequences...")
        cursor.execute("SELECT setval('regulatory_events_id_seq', 1, false)")
        cursor.execute("SELECT setval('companies_id_seq', 1, false)")
        
        # Commit changes
        conn.commit()
        
        print(f"\n✅ Database cleared successfully!")
        print(f"  - Deleted {deleted_events} regulatory events")
        print(f"  - Deleted {deleted_companies} companies")
        print("  - Reset ID sequences")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error clearing database: {e}")
        if conn:
            conn.rollback()

if __name__ == '__main__':
    clear_database()