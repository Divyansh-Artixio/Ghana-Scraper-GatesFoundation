#!/usr/bin/env python3
"""
Query regulatory events with company names instead of just IDs
"""

import psycopg2
from psycopg2.extras import RealDictCursor

def query_regulatory_events_with_company_names():
    """Query regulatory events with actual company names"""
    
    # Database connection
    db_config = {
        'host': 'localhost',
        'database': 'African_Country',
        'user': 'divyanshsingh',
        'password': 'password'
    }
    
    try:
        conn = psycopg2.connect(**db_config, cursor_factory=RealDictCursor)
        cursor = conn.cursor()
        
        # Query with JOINs to get company names
        query = """
        SELECT 
            re.id,
            re.product_name,
            re.product_type,
            re.recall_date,
            re.batches,
            re.reason_for_action,
            re.event_type,
            -- Get manufacturer name
            m.name as manufacturer_name,
            m.type as manufacturer_type,
            -- Get recalling firm name  
            rf.name as recalling_firm_name,
            rf.type as recalling_firm_type,
            re.source_url,
            re.pdf_path
        FROM regulatory_events re
        LEFT JOIN companies m ON re.manufacturer_id = m.id
        LEFT JOIN companies rf ON re.recalling_firm_id = rf.id
        WHERE re.event_type = 'Product Recall'
        AND (re.manufacturer_id IS NOT NULL OR re.recalling_firm_id IS NOT NULL)
        ORDER BY re.recall_date DESC, re.id DESC
        LIMIT 20;
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        print("🇬🇭 Ghana FDA Regulatory Events - WITH COMPANY NAMES")
        print("=" * 80)
        print(f"📊 Found {len(results)} product recalls with company information")
        print()
        
        for i, record in enumerate(results, 1):
            print(f"🔸 Record {i}: {record['product_name']}")
            print(f"   📅 Recall Date: {record['recall_date']}")
            
            if record['manufacturer_name']:
                print(f"   🏭 Manufacturer: {record['manufacturer_name']} ({record['manufacturer_type']})")
            else:
                print(f"   🏭 Manufacturer: Not specified (ID: {record.get('manufacturer_id', 'None')})")
                
            if record['recalling_firm_name']:
                print(f"   🏢 Recalling Firm: {record['recalling_firm_name']} ({record['recalling_firm_type']})")
            else:
                print(f"   🏢 Recalling Firm: Not specified (ID: {record.get('recalling_firm_id', 'None')})")
                
            print(f"   📦 Product Type: {record['product_type'] or 'Not specified'}")
            print(f"   🔢 Batches: {record['batches'] or 'Not specified'}")
            
            if record['reason_for_action']:
                print(f"   ⚠️  Reason: {record['reason_for_action']}")
            print()
        
        # Show summary statistics
        print("\n📈 Company Statistics:")
        print("-" * 40)
        
        # Count by manufacturer
        manufacturer_query = """
        SELECT 
            c.name as company_name,
            c.type as company_type,
            COUNT(*) as recall_count
        FROM regulatory_events re
        JOIN companies c ON re.manufacturer_id = c.id
        WHERE re.event_type = 'Product Recall'
        GROUP BY c.name, c.type
        ORDER BY recall_count DESC;
        """
        
        cursor.execute(manufacturer_query)
        manufacturers = cursor.fetchall()
        
        print("🏭 Top Manufacturers by Recall Count:")
        for mfg in manufacturers:
            print(f"   • {mfg['company_name']} ({mfg['company_type']}): {mfg['recall_count']} recalls")
        
        # Count by recalling firm
        recalling_firm_query = """
        SELECT 
            c.name as company_name,
            c.type as company_type,
            COUNT(*) as recall_count
        FROM regulatory_events re
        JOIN companies c ON re.recalling_firm_id = c.id
        WHERE re.event_type = 'Product Recall'
        GROUP BY c.name, c.type
        ORDER BY recall_count DESC;
        """
        
        cursor.execute(recalling_firm_query)
        recalling_firms = cursor.fetchall()
        
        print("\n🏢 Top Recalling Firms by Recall Count:")
        for firm in recalling_firms:
            print(f"   • {firm['company_name']} ({firm['company_type']}): {firm['recall_count']} recalls")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    query_regulatory_events_with_company_names()