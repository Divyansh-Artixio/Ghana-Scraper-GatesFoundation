#!/usr/bin/env python3
"""
Drop all tables from safetydb schema to prepare for fresh scraper run
"""
import psycopg2
from psycopg2.extras import RealDictCursor
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def drop_all_safetydb_tables():
    """Drop all tables from safetydb schema"""
    
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
        
        print("🗑️  Dropping All Tables from SafetyDB Schema")
        print("=" * 45)
        
        # Get all table names from safetydb schema
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'safetydb' 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """)
        
        tables = cursor.fetchall()
        
        if not tables:
            print("✅ No tables found in safetydb schema - already clean!")
            return
        
        print(f"📊 Found {len(tables)} tables to drop:")
        for table in tables:
            print(f"  - {table['table_name']}")
        
        # Disable foreign key checks to avoid dependency issues
        print("\n🔓 Disabling foreign key constraints...")
        cursor.execute("SET session_replication_role = replica;")
        
        # Drop all tables
        print("🗑️  Dropping tables...")
        dropped_count = 0
        for table in tables:
            table_name = table['table_name']
            try:
                cursor.execute(f"DROP TABLE IF EXISTS safetydb.{table_name} CASCADE")
                print(f"  ✓ Dropped safetydb.{table_name}")
                dropped_count += 1
            except Exception as e:
                print(f"  ❌ Failed to drop safetydb.{table_name}: {e}")
        
        # Re-enable foreign key checks
        print("\n🔒 Re-enabling foreign key constraints...")
        cursor.execute("SET session_replication_role = DEFAULT;")
        
        # Drop any remaining sequences
        print("🔄 Cleaning up sequences...")
        cursor.execute("""
            SELECT sequence_name 
            FROM information_schema.sequences 
            WHERE sequence_schema = 'safetydb'
        """)
        
        sequences = cursor.fetchall()
        for seq in sequences:
            seq_name = seq['sequence_name']
            try:
                cursor.execute(f"DROP SEQUENCE IF EXISTS safetydb.{seq_name} CASCADE")
                print(f"  ✓ Dropped sequence safetydb.{seq_name}")
            except Exception as e:
                print(f"  ❌ Failed to drop sequence safetydb.{seq_name}: {e}")
        
        # Commit all changes
        conn.commit()
        
        # Verify schema is clean
        cursor.execute("""
            SELECT COUNT(*) as count
            FROM information_schema.tables 
            WHERE table_schema = 'safetydb' 
            AND table_type = 'BASE TABLE'
        """)
        remaining_tables = cursor.fetchone()['count']
        
        print(f"\n✅ SafetyDB Schema Cleanup Complete!")
        print(f"  - Dropped {dropped_count} tables")
        print(f"  - Dropped {len(sequences)} sequences")
        print(f"  - Remaining tables: {remaining_tables}")
        
        if remaining_tables == 0:
            print("🎯 SafetyDB schema is now clean and ready for your scraper!")
        else:
            print("⚠️  Some tables may still remain - check manually if needed")
        
        cursor.close()
        conn.close()
        
    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
        if 'conn' in locals():
            conn.rollback()
    except Exception as e:
        logger.error(f"Error dropping tables: {e}")
        if 'conn' in locals():
            conn.rollback()

if __name__ == '__main__':
    drop_all_safetydb_tables()