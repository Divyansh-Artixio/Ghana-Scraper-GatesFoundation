#!/usr/bin/env python3
"""
Populate foreign key columns in regulatory_events with company relationships
Maps manufacturer, recalling firm, and distributor companies to their UUIDs
"""
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
import re

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def populate_company_relationships():
    """Populate foreign key columns with company relationships"""
    
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
        
        print("🔗 Populating Company Relationships in regulatory_events")
        print("=" * 55)
        
        # Check current data
        cursor.execute("SELECT COUNT(*) as count FROM public.regulatory_events")
        total_events = cursor.fetchone()['count']
        
        cursor.execute("SELECT COUNT(*) as count FROM safetydb.companies")
        total_companies = cursor.fetchone()['count']
        
        print(f"📊 Current Data:")
        print(f"  - Regulatory events: {total_events}")
        print(f"  - Companies in safetydb: {total_companies}")
        
        if total_events == 0:
            print("⚠️  No regulatory events found. Run the scraper first.")
            return
        
        if total_companies == 0:
            print("⚠️  No companies found in safetydb. Run migration first.")
            return
        
        # Get all companies for matching
        cursor.execute("SELECT id, name FROM safetydb.companies")
        companies = {row['name'].lower().strip(): row['id'] for row in cursor.fetchall()}
        
        print(f"\n🏢 Available companies for matching: {len(companies)}")
        
        # Get regulatory events that need company mapping
        cursor.execute("""
            SELECT id, manufacturer_id, recalling_firm_id, all_text, content
            FROM public.regulatory_events 
            WHERE manufacturer_company_id IS NULL 
            OR recalling_firm_company_id IS NULL
        """)
        
        events_to_update = cursor.fetchall()
        print(f"📋 Events needing company mapping: {len(events_to_update)}")
        
        if len(events_to_update) == 0:
            print("✅ All events already have company relationships mapped!")
            return
        
        # Company name patterns for extraction
        company_patterns = [
            r'manufacturer[^:]*:?\s*([^:\n,;]{3,100})(?=\s*(?:$|\n|,|;|product|batch|recalling|expiry))',
            r'manufactured\s+by[^:]*:?\s*([^:\n,;]{3,100})(?=\s*(?:$|\n|,|;|product|batch|recalling|expiry))',
            r'recalling\s+firm[^:]*:?\s*([^:\n,;]{3,100})(?=\s*(?:$|\n|,|;|product|batch|manufacturer|expiry))',
            r'recalled\s+by[^:]*:?\s*([^:\n,;]{3,100})(?=\s*(?:$|\n|,|;|product|batch|manufacturer|expiry))',
            r'distributor[^:]*:?\s*([^:\n,;]{3,100})(?=\s*(?:$|\n|,|;|product|batch|manufacturer|expiry))',
            r'distributed\s+by[^:]*:?\s*([^:\n,;]{3,100})(?=\s*(?:$|\n|,|;|product|batch|manufacturer|expiry))'
        ]
        
        updated_count = 0
        
        for event in events_to_update:
            event_id = event['id']
            text_content = (event['all_text'] or '') + ' ' + (event['content'] or '')
            
            manufacturer_id = None
            recalling_firm_id = None
            distributor_id = None
            
            # Extract company names from text
            for pattern in company_patterns:
                matches = re.findall(pattern, text_content, re.IGNORECASE)
                for match in matches:
                    company_name = match.strip().lower()
                    
                    # Find matching company UUID
                    matched_uuid = None
                    for comp_name, comp_uuid in companies.items():
                        if company_name in comp_name or comp_name in company_name:
                            matched_uuid = comp_uuid
                            break
                    
                    if matched_uuid:
                        if 'manufacturer' in pattern or 'manufactured' in pattern:
                            manufacturer_id = matched_uuid
                        elif 'recalling' in pattern or 'recalled' in pattern:
                            recalling_firm_id = matched_uuid
                        elif 'distributor' in pattern or 'distributed' in pattern:
                            distributor_id = matched_uuid
            
            # Update the event with company relationships
            if manufacturer_id or recalling_firm_id or distributor_id:
                update_parts = []
                params = []
                
                if manufacturer_id:
                    update_parts.append("manufacturer_company_id = %s")
                    params.append(manufacturer_id)
                
                if recalling_firm_id:
                    update_parts.append("recalling_firm_company_id = %s")
                    params.append(recalling_firm_id)
                
                if distributor_id:
                    update_parts.append("distributor_company_id = %s")
                    params.append(distributor_id)
                
                if update_parts:
                    params.append(event_id)
                    update_query = f"""
                        UPDATE public.regulatory_events 
                        SET {', '.join(update_parts)}
                        WHERE id = %s
                    """
                    
                    cursor.execute(update_query, params)
                    updated_count += 1
        
        conn.commit()
        
        # Verification
        print(f"\n📊 Update Results:")
        print(f"  - Events processed: {len(events_to_update)}")
        print(f"  - Events updated: {updated_count}")
        
        # Show statistics
        cursor.execute("""
            SELECT 
                COUNT(*) as total_events,
                COUNT(manufacturer_company_id) as with_manufacturer,
                COUNT(recalling_firm_company_id) as with_recalling_firm,
                COUNT(distributor_company_id) as with_distributor
            FROM public.regulatory_events
        """)
        
        stats = cursor.fetchone()
        print(f"\n📈 Final Statistics:")
        print(f"  - Total events: {stats['total_events']}")
        print(f"  - With manufacturer: {stats['with_manufacturer']}")
        print(f"  - With recalling firm: {stats['with_recalling_firm']}")
        print(f"  - With distributor: {stats['with_distributor']}")
        
        # Show sample relationships
        cursor.execute("""
            SELECT 
                re.id,
                re.product_name,
                mc.name as manufacturer_name,
                rc.name as recalling_firm_name,
                dc.name as distributor_name
            FROM public.regulatory_events re
            LEFT JOIN safetydb.companies mc ON re.manufacturer_company_id = mc.id
            LEFT JOIN safetydb.companies rc ON re.recalling_firm_company_id = rc.id
            LEFT JOIN safetydb.companies dc ON re.distributor_company_id = dc.id
            WHERE re.manufacturer_company_id IS NOT NULL 
            OR re.recalling_firm_company_id IS NOT NULL 
            OR re.distributor_company_id IS NOT NULL
            LIMIT 5
        """)
        
        samples = cursor.fetchall()
        if samples:
            print(f"\n📋 Sample Company Relationships:")
            for sample in samples:
                print(f"  - Product: {sample['product_name']}")
                if sample['manufacturer_name']:
                    print(f"    Manufacturer: {sample['manufacturer_name']}")
                if sample['recalling_firm_name']:
                    print(f"    Recalling Firm: {sample['recalling_firm_name']}")
                if sample['distributor_name']:
                    print(f"    Distributor: {sample['distributor_name']}")
                print()
        
        print(f"✅ Company relationships populated successfully!")
        print("🎯 Foreign key relationships are now established")
        
        cursor.close()
        conn.close()
        
    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
        if 'conn' in locals():
            conn.rollback()
    except Exception as e:
        logger.error(f"Error populating company relationships: {e}")
        if 'conn' in locals():
            conn.rollback()

if __name__ == '__main__':
    populate_company_relationships()