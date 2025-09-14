#!/usr/bin/env python3
"""
Query script to show company-product recall relationships
"""
import psycopg2
from psycopg2.extras import RealDictCursor
# import pandas as pd  # Optional for CSV export

def query_company_product_links():
    """Query and display company-product recall relationships"""
    
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
        
        print("🔗 Company-Product Recall Relationships")
        print("=" * 80)
        
        # Query to show product recalls with linked companies
        query = """
        SELECT 
            re.id,
            re.product_name,
            re.product_type,
            re.recall_date,
            re.reason_for_action,
            m.name as manufacturer_name,
            m.type as manufacturer_type,
            rf.name as recalling_firm_name,
            rf.type as recalling_firm_type,
            re.batches,
            re.manufacturing_date,
            re.expiry_date
        FROM regulatory_events re
        LEFT JOIN companies m ON re.manufacturer_id = m.id
        LEFT JOIN companies rf ON re.recalling_firm_id = rf.id
        WHERE re.event_type = 'Product Recall'
        ORDER BY re.recall_date DESC, re.id
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        print(f"📊 Found {len(results)} product recalls with company links\n")
        
        # Display results in a formatted way
        for i, record in enumerate(results[:10], 1):  # Show first 10
            print(f"🔸 Record {i}: {record['product_name']}")
            print(f"   📅 Recall Date: {record['recall_date']}")
            print(f"   🏭 Manufacturer: {record['manufacturer_name'] or 'Not specified'} ({record['manufacturer_type'] or 'N/A'})")
            print(f"   🏢 Recalling Firm: {record['recalling_firm_name'] or 'Not specified'} ({record['recalling_firm_type'] or 'N/A'})")
            print(f"   📦 Product Type: {record['product_type'] or 'Not specified'}")
            print(f"   🔢 Batches: {record['batches'] or 'Not specified'}")
            if record['reason_for_action']:
                reason = record['reason_for_action'][:100] + "..." if len(record['reason_for_action']) > 100 else record['reason_for_action']
                print(f"   ⚠️  Reason: {reason}")
            print()
        
        if len(results) > 10:
            print(f"... and {len(results) - 10} more records")
        
        # Summary statistics
        print("\n📈 Company Statistics:")
        print("-" * 40)
        
        # Manufacturer statistics
        cursor.execute("""
            SELECT m.name, m.type, COUNT(*) as recall_count
            FROM regulatory_events re
            JOIN companies m ON re.manufacturer_id = m.id
            WHERE re.event_type = 'Product Recall'
            GROUP BY m.name, m.type
            ORDER BY recall_count DESC
            LIMIT 10
        """)
        
        manufacturers = cursor.fetchall()
        print("🏭 Top Manufacturers by Recall Count:")
        for mfg in manufacturers:
            print(f"   • {mfg['name']} ({mfg['type']}): {mfg['recall_count']} recalls")
        
        # Recalling firm statistics
        cursor.execute("""
            SELECT rf.name, rf.type, COUNT(*) as recall_count
            FROM regulatory_events re
            JOIN companies rf ON re.recalling_firm_id = rf.id
            WHERE re.event_type = 'Product Recall'
            GROUP BY rf.name, rf.type
            ORDER BY recall_count DESC
            LIMIT 10
        """)
        
        recalling_firms = cursor.fetchall()
        print("\n🏢 Top Recalling Firms by Recall Count:")
        for firm in recalling_firms:
            print(f"   • {firm['name']} ({firm['type']}): {firm['recall_count']} recalls")
        
        # Product type analysis
        cursor.execute("""
            SELECT 
                re.product_type,
                COUNT(*) as recall_count,
                COUNT(DISTINCT re.manufacturer_id) as unique_manufacturers,
                COUNT(DISTINCT re.recalling_firm_id) as unique_recalling_firms
            FROM regulatory_events re
            WHERE re.event_type = 'Product Recall' AND re.product_type IS NOT NULL
            GROUP BY re.product_type
            ORDER BY recall_count DESC
        """)
        
        product_types = cursor.fetchall()
        print("\n📦 Product Types Analysis:")
        for pt in product_types:
            print(f"   • {pt['product_type']}: {pt['recall_count']} recalls, "
                  f"{pt['unique_manufacturers']} manufacturers, "
                  f"{pt['unique_recalling_firms']} recalling firms")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error querying database: {e}")

def export_company_links_csv():
    """Export company-product links to CSV file"""
    
    db_config = {
        'host': 'localhost',
        'database': 'safetyiq',
        'user': 'sanatanupmanyu',
        'password': 'ksDq2jazKmxxzv.VxXbkwR6Uxz',
        'port': 5432
    }
    
    try:
        import pandas as pd
        conn = psycopg2.connect(**db_config)
        
        query = """
        SELECT 
            re.id as recall_id,
            re.product_name,
            re.product_type,
            re.recall_date,
            re.reason_for_action,
            re.batches,
            re.manufacturing_date,
            re.expiry_date,
            re.manufacturer_id,
            m.name as manufacturer_name,
            m.type as manufacturer_type,
            m.country_code as manufacturer_country,
            re.recalling_firm_id,
            rf.name as recalling_firm_name,
            rf.type as recalling_firm_type,
            rf.country_code as recalling_firm_country,
            re.source_url,
            re.pdf_path
        FROM regulatory_events re
        LEFT JOIN companies m ON re.manufacturer_id = m.id
        LEFT JOIN companies rf ON re.recalling_firm_id = rf.id
        WHERE re.event_type = 'Product Recall'
        ORDER BY re.recall_date DESC, re.id
        """
        
        df = pd.read_sql_query(query, conn)
        
        # Export to CSV
        filename = 'company_product_recall_links.csv'
        df.to_csv(filename, index=False)
        
        print(f"📊 Exported {len(df)} records to {filename}")
        print(f"📁 File contains complete company-product recall relationships")
        
        conn.close()
        
    except ImportError:
        print("❌ pandas not installed. Install with: pip install pandas")
    except Exception as e:
        print(f"❌ Error exporting data: {e}")

if __name__ == '__main__':
    print("🇬🇭 Ghana FDA Regulatory Scraper - Company Links Analysis")
    print("=" * 60)
    
    # Query and display relationships
    query_company_product_links()
    
    # Ask if user wants to export to CSV
    export_choice = input("\n📤 Export to CSV file? (y/n): ").lower().strip()
    if export_choice in ['y', 'yes']:
        try:
            export_company_links_csv()
        except ImportError:
            print("❌ pandas not installed. Install with: pip install pandas")
        except Exception as e:
            print(f"❌ Export failed: {e}")