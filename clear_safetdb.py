#!/usr/bin/env python3
"""
Clear all table data from safetdb database
"""
import psycopg2
from psycopg2.extras import RealDictCursor
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def clear_safetdb():
    """Clear all data from safetdb database tables"""
    
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
        
        print("🗑️  Clearing SafetyDB Database Data")
        print("=" * 35)
        
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
            print("❌ No tables found in safetdb database!")
            return
        
        print(f"📊 Found {len(tables)} tables:")
        
        # Show current data counts
        total_records = 0
        for table in tables:
            table_name = table['table_name']
            cursor.execute(f"SELECT COUNT(*) as count FROM safetydb.{table_name}")
            count = cursor.fetchone()['count']
            total_records += count
            print(f"  - {table_name}: {count} records")
        
        if total_records == 0:
            print("✅ Database is already empty!")
            return
        
        print(f"\n🧹 Clearing {total_records} total records...")
        
        # Disable foreign key checks temporarily
        cursor.execute("SET session_replication_role = replica;")
        
        # Clear all tables
        deleted_total = 0
        for table in tables:
            table_name = table['table_name']
            print(f"  Clearing {table_name}...")
            cursor.execute(f"DELETE FROM safetydb.{table_name}")
            deleted_count = cursor.rowcount
            deleted_total += deleted_count
            print(f"    ✓ Deleted {deleted_count} records")
        
        # Re-enable foreign key checks
        cursor.execute("SET session_replication_role = DEFAULT;")
        
        # Reset sequences for tables with serial columns
        print("\n🔄 Resetting sequences...")
        cursor.execute("""
            SELECT sequence_name 
            FROM information_schema.sequences 
            WHERE sequence_schema = 'safetydb'
        """)
        
        sequences = cursor.fetchall()
        for seq in sequences:
            seq_name = seq['sequence_name']
            cursor.execute(f"SELECT setval('{seq_name}', 1, false)")
            print(f"    ✓ Reset {seq_name}")
        
        # Commit all changes
        conn.commit()
        
        print(f"\n✅ SafetyDB cleared successfully!")
        print(f"  - Deleted {deleted_total} total records")
        print(f"  - Cleared {len(tables)} tables")
        print(f"  - Reset {len(sequences)} sequences")
        
        cursor.close()
        conn.close()
        
    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
        if 'conn' in locals():
            conn.rollback()
    except Exception as e:
        logger.error(f"Error clearing safetdb: {e}")
        if 'conn' in locals():
            conn.rollback()

if __name__ == '__main__':
    clear_safetdb()