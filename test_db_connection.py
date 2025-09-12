#!/usr/bin/env python3
"""
Test script to verify database connection and insertion
"""
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_db_connection():
    """Test database connection and basic operations"""
    
    # Database configuration (same as in your scraper)
    db_config = {
        'host': 'localhost',
        'database': 'African_Country',
        'user': 'divyanshsingh',
        'port': 5432
    }
    
    try:
        # Test connection
        logger.info("🔌 Testing database connection...")
        conn = psycopg2.connect(**db_config, cursor_factory=RealDictCursor)
        cursor = conn.cursor()
        logger.info("✅ Database connection successful!")
        
        # Test basic query
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        logger.info(f"📊 PostgreSQL version: {version['version'] if isinstance(version, dict) else version[0]}")
        
        # Check if tables exist
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name IN ('regulatory_events', 'companies', 'countries')
        """)
        tables = cursor.fetchall()
        logger.info(f"📋 Found tables: {[t['table_name'] for t in tables]}")
        
        # Check regulatory_events table structure
        cursor.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_name = 'regulatory_events'
            ORDER BY ordinal_position
        """)
        columns = cursor.fetchall()
        logger.info("📝 regulatory_events table structure:")
        for col in columns:
            logger.info(f"  - {col['column_name']}: {col['data_type']} ({'NULL' if col['is_nullable'] == 'YES' else 'NOT NULL'})")
        
        # Test a simple insert
        logger.info("🧪 Testing simple insert...")
        test_data = {
            'url': f'https://test.com?id={int(datetime.now().timestamp())}',
            'event_type': 'Alert',
            'alert_date': datetime.now().date(),
            'alert_name': 'Test Alert',
            'all_text': 'This is a test alert',
            'notice_date': None,
            'notice_text': None,
            'recall_date': None,
            'product_name': None,
            'product_type': None,
            'manufacturer_id': None,
            'recalling_firm_id': None,
            'batches': None,
            'manufacturing_date': None,
            'expiry_date': None,
            'source_url': 'https://test.com',
            'pdf_path': None,
            'reason_for_action': 'Test reason'
        }
        
        insert_query = """
        INSERT INTO regulatory_events (
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
        
        cursor.execute(insert_query, test_data)
        result = cursor.fetchone()
        
        if result:
            test_id = result['id']
            logger.info(f"✅ Test insert successful! ID: {test_id}")
            
            # Clean up test record
            cursor.execute("DELETE FROM regulatory_events WHERE id = %s", (test_id,))
            conn.commit()
            logger.info("🧹 Test record cleaned up")
        else:
            logger.error("❌ Test insert failed - no ID returned")
            
        cursor.close()
        conn.close()
        logger.info("✅ Database test completed successfully!")
        return True
        
    except psycopg2.OperationalError as e:
        logger.error(f"❌ Database connection failed: {e}")
        logger.error("💡 Check if PostgreSQL is running and credentials are correct")
        return False
        
    except psycopg2.Error as e:
        logger.error(f"❌ Database error: {e}")
        return False
        
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        return False

if __name__ == '__main__':
    print("🧪 Ghana FDA Scraper - Database Connection Test")
    print("=" * 50)
    success = test_db_connection()
    if success:
        print("🎉 All tests passed! Your database is ready.")
    else:
        print("❌ Tests failed. Please check the errors above.")