#!/usr/bin/env python3
"""
Fix the companies_mentioned field to use UUIDs instead of integers
This script handles the data type mismatch between:
- safetydb.companies.id (UUID)
- public.regulatory_events.companies_mentioned (Int[])
"""
import psycopg2
from psycopg2.extras import RealDictCursor
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fix_companies_mentioned_uuid():
    """Update companies_mentioned field to use UUIDs and create proper mappings"""
    
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
        
        print("🔧 Fixing companies_mentioned UUID mapping")
        print("=" * 45)
        
        # Step 1: Check current state
        print("🔍 Analyzing current data structure...")
        
        # Check if we have data in both schemas
        cursor.execute("SELECT COUNT(*) FROM public.companies")
        public_companies_count = cursor.fetchone()['count']
        
        cursor.execute("SELECT COUNT(*) FROM safetydb.companies")
        safety_companies_count = cursor.fetchone()['count']
        
        cursor.execute("SELECT COUNT(*) FROM public.regulatory_events WHERE companies_mentioned IS NOT NULL")
        events_with_companies = cursor.fetchone()['count']
        
        print(f"  - Public companies: {public_companies_count}")
        print(f"  - SafetyDB companies: {safety_companies_count}")
        print(f"  - Events with company mentions: {events_with_companies}")
        
        if public_companies_count == 0:
            print("⚠️  No companies found in public schema. Nothing to migrate.")
            return
        
        # Step 2: Create a mapping table for company ID conversion
        print("\n📋 Creating company ID mapping...")
        
        cursor.execute("""
            DROP TABLE IF EXISTS temp_company_id_mapping
        """)
        
        cursor.execute("""
            CREATE TEMP TABLE temp_company_id_mapping (
                old_id INT,
                new_uuid UUID,
                company_name VARCHAR(255)
            )
        """)
        
        # Step 3: Populate mapping based on company names
        print("🔗 Building company name-based mapping...")
        
        cursor.execute("""
            INSERT INTO temp_company_id_mapping (old_id, new_uuid, company_name)
            SELECT 
                pc.id as old_id,
                sc.id as new_uuid,
                pc.name as company_name
            FROM public.companies pc
            JOIN safetydb.companies sc ON LOWER(TRIM(pc.name)) = LOWER(TRIM(sc.name))
        """)
        
        mapped_companies = cursor.rowcount
        print(f"  ✓ Mapped {mapped_companies} companies by name")
        
        # Step 4: Handle unmapped companies
        if mapped_companies < public_companies_count:
            unmapped_count = public_companies_count - mapped_companies
            print(f"⚠️  {unmapped_count} companies couldn't be mapped by name")
            
            # Insert missing companies into safetydb
            print("📝 Adding missing companies to safetydb...")
            cursor.execute("""
                INSERT INTO safetydb.companies (name, country_of_origin, established_year, created_at, updated_at)
                SELECT 
                    pc.name,
                    COALESCE(pc.country_code, 'Unknown'),
                    EXTRACT(YEAR FROM pc.founding_date),
                    pc.created_at,
                    pc.updated_at
                FROM public.companies pc
                LEFT JOIN temp_company_id_mapping tm ON pc.id = tm.old_id
                WHERE tm.old_id IS NULL
                ON CONFLICT (name) DO NOTHING
                RETURNING id, name
            """)
            
            new_companies = cursor.fetchall()
            print(f"  ✓ Added {len(new_companies)} new companies to safetydb")
            
            # Update mapping with newly added companies
            cursor.execute("""
                INSERT INTO temp_company_id_mapping (old_id, new_uuid, company_name)
                SELECT 
                    pc.id as old_id,
                    sc.id as new_uuid,
                    pc.name as company_name
                FROM public.companies pc
                JOIN safetydb.companies sc ON LOWER(TRIM(pc.name)) = LOWER(TRIM(sc.name))
                LEFT JOIN temp_company_id_mapping tm ON pc.id = tm.old_id
                WHERE tm.old_id IS NULL
            """)
            
            additional_mapped = cursor.rowcount
            print(f"  ✓ Mapped {additional_mapped} additional companies")
        
        # Step 5: Update regulatory_events with UUID mappings
        print("\n🔄 Updating regulatory_events with UUID mappings...")
        
        # First, let's see what we're working with
        cursor.execute("""
            SELECT COUNT(*) 
            FROM public.regulatory_events 
            WHERE companies_mentioned IS NOT NULL AND array_length(companies_mentioned, 1) > 0
        """)
        events_to_update = cursor.fetchone()['count']
        print(f"  - Events with company mentions to update: {events_to_update}")
        
        if events_to_update > 0:
            # Create a function to convert integer arrays to UUID arrays
            cursor.execute("""
                CREATE OR REPLACE FUNCTION convert_company_ids_to_uuids(int_array INT[])
                RETURNS UUID[] AS $$
                DECLARE
                    result UUID[] := '{}';
                    company_id INT;
                    mapped_uuid UUID;
                BEGIN
                    IF int_array IS NULL OR array_length(int_array, 1) IS NULL THEN
                        RETURN result;
                    END IF;
                    
                    FOREACH company_id IN ARRAY int_array
                    LOOP
                        SELECT new_uuid INTO mapped_uuid 
                        FROM temp_company_id_mapping 
                        WHERE old_id = company_id;
                        
                        IF mapped_uuid IS NOT NULL THEN
                            result := array_append(result, mapped_uuid);
                        END IF;
                    END LOOP;
                    
                    RETURN result;
                END;
                $$ LANGUAGE plpgsql;
            """)
            
            # Update safetydb.regulatory_events with converted UUIDs
            cursor.execute("""
                UPDATE safetydb.regulatory_events 
                SET companies_mentioned = convert_company_ids_to_uuids(
                    (SELECT companies_mentioned FROM public.regulatory_events pre 
                     WHERE pre.url = safetydb.regulatory_events.url)
                )
                WHERE EXISTS (
                    SELECT 1 FROM public.regulatory_events pre 
                    WHERE pre.url = safetydb.regulatory_events.url 
                    AND pre.companies_mentioned IS NOT NULL
                )
            """)
            
            updated_events = cursor.rowcount
            print(f"  ✓ Updated {updated_events} events with UUID company references")
            
            # Clean up the function
            cursor.execute("DROP FUNCTION IF EXISTS convert_company_ids_to_uuids(INT[])")
        
        # Step 6: Verify the results
        print("\n📊 Verification Results:")
        
        cursor.execute("""
            SELECT COUNT(*) 
            FROM safetydb.regulatory_events 
            WHERE companies_mentioned IS NOT NULL AND array_length(companies_mentioned, 1) > 0
        """)
        final_events_with_companies = cursor.fetchone()['count']
        
        cursor.execute("SELECT COUNT(*) FROM temp_company_id_mapping")
        total_mappings = cursor.fetchone()['count']
        
        print(f"  - Total company mappings created: {total_mappings}")
        print(f"  - Events with UUID company references: {final_events_with_companies}")
        
        # Show sample of converted data
        cursor.execute("""
            SELECT 
                url,
                array_length(companies_mentioned, 1) as company_count,
                companies_mentioned[1:3] as sample_uuids
            FROM safetydb.regulatory_events 
            WHERE companies_mentioned IS NOT NULL 
            AND array_length(companies_mentioned, 1) > 0
            LIMIT 3
        """)
        
        sample_events = cursor.fetchall()
        if sample_events:
            print("\n📋 Sample converted events:")
            for event in sample_events:
                print(f"  - URL: {event['url'][:50]}...")
                print(f"    Companies: {event['company_count']}, Sample UUIDs: {event['sample_uuids']}")
        
        conn.commit()
        print(f"\n✅ Successfully fixed companies_mentioned UUID mapping!")
        print("🎯 All company references now use UUIDs consistently")
        
        cursor.close()
        conn.close()
        
    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
        if 'conn' in locals():
            conn.rollback()
    except Exception as e:
        logger.error(f"Error during UUID conversion: {e}")
        if 'conn' in locals():
            conn.rollback()

if __name__ == '__main__':
    fix_companies_mentioned_uuid()