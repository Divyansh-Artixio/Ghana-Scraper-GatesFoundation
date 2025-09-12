#!/usr/bin/env python3
"""
Fix duplicate companies in the database
"""
import psycopg2
from psycopg2.extras import RealDictCursor
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fix_duplicate_companies():
    """Remove duplicate companies and update references"""
    
    db_config = {
        'host': 'localhost',
        'database': 'African_Country',
        'user': 'divyanshsingh',
        'port': 5432
    }
    
    try:
        conn = psycopg2.connect(**db_config, cursor_factory=RealDictCursor)
        cursor = conn.cursor()
        
        print("🔧 Fixing Duplicate Companies")
        print("=" * 40)
        
        # Find duplicate companies by name
        cursor.execute("""
            SELECT name, COUNT(*) as count, array_agg(id) as ids, array_agg(type) as types
            FROM companies 
            GROUP BY name 
            HAVING COUNT(*) > 1
            ORDER BY count DESC
        """)
        duplicates = cursor.fetchall()
        
        print(f"📊 Found {len(duplicates)} companies with duplicates")
        
        for dup in duplicates:
            name = dup['name']
            ids = dup['ids']
            types = dup['types']
            print(f"\n🏢 {name}: {len(ids)} duplicates")
            print(f"   IDs: {ids}")
            print(f"   Types: {types}")
            
            # Keep the first ID, merge others
            keep_id = ids[0]
            remove_ids = ids[1:]
            
            # Determine the best type (prefer Manufacturer over Reselling Firm)
            best_type = 'Manufacturer' if 'Manufacturer' in types else types[0]
            
            # Update the kept record with the best type
            cursor.execute("UPDATE companies SET type = %s WHERE id = %s", (best_type, keep_id))
            
            # Update all regulatory_events references to point to the kept ID
            for remove_id in remove_ids:
                cursor.execute("""
                    UPDATE regulatory_events 
                    SET manufacturer_id = %s 
                    WHERE manufacturer_id = %s
                """, (keep_id, remove_id))
                
                cursor.execute("""
                    UPDATE regulatory_events 
                    SET recalling_firm_id = %s 
                    WHERE recalling_firm_id = %s
                """, (keep_id, remove_id))
                
                # Delete the duplicate company
                cursor.execute("DELETE FROM companies WHERE id = %s", (remove_id,))
            
            print(f"   ✅ Merged into ID {keep_id} ({best_type})")
        
        # Commit all changes
        conn.commit()
        
        # Show final stats
        cursor.execute("SELECT COUNT(*) as count FROM companies")
        final_count = cursor.fetchone()['count']
        
        cursor.execute("""
            SELECT type, COUNT(*) as count 
            FROM companies 
            GROUP BY type
        """)
        type_counts = cursor.fetchall()
        
        print(f"\n✅ Cleanup completed!")
        print(f"📈 Final company count: {final_count}")
        for tc in type_counts:
            print(f"  - {tc['type']}: {tc['count']}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error fixing duplicates: {e}")
        if conn:
            conn.rollback()

if __name__ == '__main__':
    fix_duplicate_companies()