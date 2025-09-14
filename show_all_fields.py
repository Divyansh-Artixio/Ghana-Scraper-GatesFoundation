#!/usr/bin/env python3
"""
Show all fields being captured in the safetyiq database
"""
import psycopg2
from psycopg2.extras import RealDictCursor
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def show_all_fields():
    """Show all fields and their data from the safetyiq database"""
    
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
        
        print("🔍 SafetyIQ Database - All Captured Fields")
        print("=" * 60)
        
        # Get table structure
        cursor.execute("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns 
            WHERE table_schema = 'safetydb' AND table_name = 'regulatory_events'
            ORDER BY ordinal_position
        """)
        columns = cursor.fetchall()
        
        print("📋 Table Structure (safetydb.regulatory_events):")
        print("-" * 50)
        for col in columns:
            nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
            default = f" DEFAULT {col['column_default']}" if col['column_default'] else ""
            print(f"  • {col['column_name']}: {col['data_type']} ({nullable}){default}")
        
        # Get sample data with all fields
        cursor.execute("""
            SELECT * FROM safetydb.regulatory_events 
            WHERE created_at >= CURRENT_DATE - INTERVAL '1 day'
            ORDER BY created_at DESC 
            LIMIT 3
        """)
        recent_events = cursor.fetchall()
        
        print(f"\n📊 Recent Events ({len(recent_events)} records):")
        print("-" * 50)
        
        for i, event in enumerate(recent_events, 1):
            print(f"\n🔸 Record {i}: {event['event_type']}")
            print(f"   ID: {event['id']}")
            print(f"   Product: {event['product_name'] or 'N/A'}")
            print(f"   Alert Name: {event['alert_name'] or 'N/A'}")
            print(f"   Notice Text: {event['notice_text'] or 'N/A'}")
            print(f"   Dates: Alert={event['alert_date']}, Notice={event['notice_date']}, Recall={event['recall_date']}")
            print(f"   Batches: {event['batches'] or 'N/A'}")
            print(f"   Manufacturing Date: {event['manufacturing_date'] or 'N/A'}")
            print(f"   Expiry Date: {event['expiry_date'] or 'N/A'}")
            print(f"   Product Type: {event['product_type'] or 'N/A'}")
            print(f"   Source URL: {event['source_url'] or 'N/A'}")
            print(f"   PDF Path: {event['pdf_path'] or 'N/A'}")
            
            # New detailed fields
            print(f"   📄 Detailed Content: {(event['detailed_content'] or 'N/A')[:100]}{'...' if event['detailed_content'] and len(event['detailed_content']) > 100 else ''}")
            print(f"   🏭 Manufacturing Firm: {event['manufacturing_firm'] or 'N/A'}")
            print(f"   📦 Importing Firm: {event['importing_firm'] or 'N/A'}")
            print(f"   🚚 Distributing Firm: {event['distributing_firm'] or 'N/A'}")
            print(f"   📝 Product Description: {(event['product_description'] or 'N/A')[:100]}{'...' if event['product_description'] and len(event['product_description']) > 100 else ''}")
            print(f"   ⚠️  Hazard Description: {(event['hazard_description'] or 'N/A')[:100]}{'...' if event['hazard_description'] and len(event['hazard_description']) > 100 else ''}")
            print(f"   🔧 Corrective Action: {(event['corrective_action'] or 'N/A')[:100]}{'...' if event['corrective_action'] and len(event['corrective_action']) > 100 else ''}")
            print(f"   📅 Created: {event['created_at']}")
        
        # Field usage statistics
        print(f"\n📈 Field Usage Statistics:")
        print("-" * 30)
        
        field_stats = [
            ('product_name', 'Product Name'),
            ('alert_name', 'Alert Name'),
            ('notice_text', 'Notice Text'),
            ('batches', 'Batches'),
            ('manufacturing_date', 'Manufacturing Date'),
            ('expiry_date', 'Expiry Date'),
            ('detailed_content', 'Detailed Content'),
            ('manufacturing_firm', 'Manufacturing Firm'),
            ('importing_firm', 'Importing Firm'),
            ('distributing_firm', 'Distributing Firm'),
            ('product_description', 'Product Description'),
            ('hazard_description', 'Hazard Description'),
            ('corrective_action', 'Corrective Action'),
            ('pdf_path', 'PDF Path'),
            ('reason_for_action', 'Reason for Action')
        ]
        
        for field, label in field_stats:
            cursor.execute(f"""
                SELECT 
                    COUNT(*) as total,
                    COUNT({field}) as populated,
                    ROUND(COUNT({field}) * 100.0 / COUNT(*), 1) as percentage
                FROM safetydb.regulatory_events
            """)
            stats = cursor.fetchone()
            print(f"  • {label}: {stats['populated']}/{stats['total']} ({stats['percentage']}%)")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"❌ Error showing fields: {e}")

if __name__ == '__main__':
    show_all_fields()