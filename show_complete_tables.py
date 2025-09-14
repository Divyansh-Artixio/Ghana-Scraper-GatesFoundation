#!/usr/bin/env python3
"""
Show complete table structure and content for all safetydb tables
"""
import psycopg2
from psycopg2.extras import RealDictCursor
import json
from datetime import datetime

def show_complete_tables():
    """Show complete structure and content for all safetydb tables"""
    
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
        
        print("🗄️  COMPLETE SAFETYDB TABLES CONTENT")
        print("=" * 80)
        
        # Get all tables in safetydb schema
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'safetydb'
            ORDER BY table_name
        """)
        tables = cursor.fetchall()
        
        for table_info in tables:
            table_name = table_info['table_name']
            print(f"\n📋 TABLE: safetydb.{table_name}")
            print("=" * 60)
            
            # Get table structure
            cursor.execute("""
                SELECT 
                    column_name, 
                    data_type, 
                    character_maximum_length,
                    is_nullable, 
                    column_default,
                    ordinal_position
                FROM information_schema.columns 
                WHERE table_schema = 'safetydb' AND table_name = %s
                ORDER BY ordinal_position
            """, (table_name,))
            columns = cursor.fetchall()
            
            print("🏗️  STRUCTURE:")
            print("-" * 40)
            for col in columns:
                nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
                max_len = f"({col['character_maximum_length']})" if col['character_maximum_length'] else ""
                default = f" DEFAULT {col['column_default']}" if col['column_default'] else ""
                print(f"  {col['ordinal_position']:2d}. {col['column_name']:<25} {col['data_type']}{max_len:<10} {nullable:<8}{default}")
            
            # Get row count
            cursor.execute(f"SELECT COUNT(*) as count FROM safetydb.{table_name}")
            row_count = cursor.fetchone()['count']
            
            print(f"\n📊 DATA ({row_count} records):")
            print("-" * 40)
            
            if row_count == 0:
                print("  (No data)")
                continue
            
            # Get all data
            cursor.execute(f"SELECT * FROM safetydb.{table_name} ORDER BY created_at DESC")
            rows = cursor.fetchall()
            
            # Display data based on table type
            if table_name == 'countries':
                show_countries_data(rows)
            elif table_name == 'companies':
                show_companies_data(rows)
            elif table_name == 'regulatory_events':
                show_regulatory_events_data(rows)
            else:
                # Generic display for unknown tables
                for i, row in enumerate(rows[:5], 1):  # Show first 5 rows
                    print(f"\n  Record {i}:")
                    for key, value in row.items():
                        if value is not None:
                            display_value = str(value)[:100] + "..." if len(str(value)) > 100 else str(value)
                            print(f"    {key}: {display_value}")
                
                if row_count > 5:
                    print(f"\n  ... and {row_count - 5} more records")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")

def show_countries_data(rows):
    """Display countries data in a formatted way"""
    for i, row in enumerate(rows, 1):
        print(f"\n  🌍 Country {i}:")
        print(f"    ID: {row['id']}")
        print(f"    Code: {row['code']}")
        print(f"    Name: {row['name']}")
        print(f"    Region: {row['region'] or 'N/A'}")
        print(f"    Population: {row['population'] or 'N/A'}")
        print(f"    GDP per Capita: {row['gdp_per_capita'] or 'N/A'}")
        print(f"    Healthcare Index: {row['healthcare_index'] or 'N/A'}")
        print(f"    Regulatory Maturity: {row['regulatory_maturity'] or 'N/A'}")
        print(f"    Created: {row['created_at']}")

def show_companies_data(rows):
    """Display companies data in a formatted way"""
    for i, row in enumerate(rows, 1):
        print(f"\n  🏢 Company {i}:")
        print(f"    ID: {row['id']}")
        print(f"    Name: {row['name']}")
        print(f"    Country: {row['country_of_origin'] or 'N/A'}")
        print(f"    Address: {row['address'] or 'N/A'}")
        print(f"    Risk Score: {row['risk_score'] or 'N/A'}")
        print(f"    Total Violations: {row['total_violations'] or 0}")
        print(f"    Company Size: {row['company_size'] or 'N/A'}")
        print(f"    Established Year: {row['established_year'] or 'N/A'}")
        print(f"    Website: {row['website'] or 'N/A'}")
        print(f"    Regulatory Status: {row['regulatory_status'] or 'N/A'}")
        print(f"    Last Inspection: {row['last_inspection_date'] or 'N/A'}")
        print(f"    Primary Products: {row['primary_products'] or 'N/A'}")
        print(f"    Beneficial Owners: {row['beneficial_owners'] or 'N/A'}")
        print(f"    Created: {row['created_at']}")
        print(f"    Updated: {row['updated_at']}")

def show_regulatory_events_data(rows):
    """Display regulatory events data in a formatted way"""
    for i, row in enumerate(rows, 1):
        print(f"\n  📋 Event {i}: {row['event_type']}")
        print(f"    ID: {row['id']}")
        print(f"    URL: {row['url']}")
        
        # Event-specific fields
        if row['event_type'] == 'Product Recall':
            print(f"    Product Name: {row['product_name'] or 'N/A'}")
            print(f"    Product Type: {row['product_type'] or 'N/A'}")
            print(f"    Recall Date: {row['recall_date'] or 'N/A'}")
            print(f"    Batches: {row['batches'] or 'N/A'}")
            print(f"    Manufacturing Date: {row['manufacturing_date'] or 'N/A'}")
            print(f"    Expiry Date: {row['expiry_date'] or 'N/A'}")
            print(f"    Manufacturer ID: {row['manufacturer_id'] or 'N/A'}")
            print(f"    Recalling Firm ID: {row['recalling_firm_id'] or 'N/A'}")
        elif row['event_type'] == 'Alert':
            print(f"    Alert Name: {row['alert_name'] or 'N/A'}")
            print(f"    Alert Date: {row['alert_date'] or 'N/A'}")
        elif row['event_type'] == 'Public Notice':
            print(f"    Notice Text: {(row['notice_text'] or 'N/A')[:100]}{'...' if row['notice_text'] and len(row['notice_text']) > 100 else ''}")
            print(f"    Notice Date: {row['notice_date'] or 'N/A'}")
        
        # Common fields
        print(f"    Source URL: {row['source_url'] or 'N/A'}")
        print(f"    PDF Path: {(row['pdf_path'] or 'N/A')[:80]}{'...' if row['pdf_path'] and len(row['pdf_path']) > 80 else ''}")
        print(f"    Reason for Action: {(row['reason_for_action'] or 'N/A')[:100]}{'...' if row['reason_for_action'] and len(row['reason_for_action']) > 100 else ''}")
        
        # Enhanced fields
        if row['detailed_content']:
            print(f"    Detailed Content: {row['detailed_content'][:100]}...")
        if row['manufacturing_firm']:
            print(f"    Manufacturing Firm: {row['manufacturing_firm']}")
        if row['importing_firm']:
            print(f"    Importing Firm: {row['importing_firm']}")
        if row['distributing_firm']:
            print(f"    Distributing Firm: {row['distributing_firm']}")
        if row['product_description']:
            print(f"    Product Description: {row['product_description'][:100]}...")
        if row['hazard_description']:
            print(f"    Hazard Description: {row['hazard_description'][:100]}...")
        if row['corrective_action']:
            print(f"    Corrective Action: {row['corrective_action'][:100]}...")
        
        print(f"    Created: {row['created_at']}")
        print(f"    Updated: {row['updated_at']}")

if __name__ == '__main__':
    show_complete_tables()