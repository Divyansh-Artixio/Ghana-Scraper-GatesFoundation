#!/usr/bin/env python3
"""
Drop all tables from both public and safetydb schemas
Complete database cleanup
"""
import psycopg2
from psycopg2.extras import RealDictCursor
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def drop_all_tables():
    """Drop all tables from both public and safetydb schemas"""
    
    db_config = {
        'host': 'localhost',
        'database': 'African_Country',
        'user': 'divyanshsingh',
        'port': 5432
    }
    
    try:
        conn = psycopg2.connect(**db_config, cursor_factory=RealDictCursor)
        cursor = conn.cursor()
        
        print("🗑️  Dropping ALL Tables from Database")
        print("=" * 40)
        
        # Get all tables from both schemas
        cursor.execute("""
            SELECT table_schema, table_name 
            FROM information_schema.tables 
            WHERE table_schema IN ('public', 'safetydb')
            AND table_type = 'BASE TABLE'
            ORDER BY table_schema, table_name
        """)
        
        all_tables = cursor.fetchall()
        
        if not all_tables:
            print("✅ No tables found - database is already clean!")
            return
        
        print(f"📊 Found {len(all_tables)} tables to drop:")
        for table in all_tables:
            print(f"  - {table['table_schema']}.{table['table_name']}")
        
        # Disable foreign key checks to avoid dependency issues
        print("\n🔓 Disabling foreign key constraints...")
        cursor.execute("SET session_replication_role = replica;")
        
        # Drop all tables
        print("🗑️  Dropping tables...")
        dropped_count = 0
        
        for table in all_tables:
            schema_name = table['table_schema']
            table_name = table['table_name']
            
            try:
                cursor.execute(f"DROP TABLE IF EXISTS {schema_name}.{table_name} CASCADE")
                print(f"  ✓ Dropped {schema_name}.{table_name}")
                dropped_count += 1
            except Exception as e:
                print(f"  ❌ Failed to drop {schema_name}.{table_name}: {e}")
        
        # Re-enable foreign key checks
        print("\n🔒 Re-enabling foreign key constraints...")
        cursor.execute("SET session_replication_role = DEFAULT;")
        
        # Drop sequences from both schemas
        print("🔄 Cleaning up sequences...")
        cursor.execute("""
            SELECT sequence_schema, sequence_name 
            FROM information_schema.sequences 
            WHERE sequence_schema IN ('public', 'safetydb')
        """)
        
        sequences = cursor.fetchall()
        for seq in sequences:
            schema_name = seq['sequence_schema']
            seq_name = seq['sequence_name']
            try:
                cursor.execute(f"DROP SEQUENCE IF EXISTS {schema_name}.{seq_name} CASCADE")
                print(f"  ✓ Dropped sequence {schema_name}.{seq_name}")
            except Exception as e:
                print(f"  ❌ Failed to drop sequence {schema_name}.{seq_name}: {e}")
        
        # Drop any remaining views
        print("👁️  Cleaning up views...")
        cursor.execute("""
            SELECT table_schema, table_name 
            FROM information_schema.views 
            WHERE table_schema IN ('public', 'safetydb')
        """)
        
        views = cursor.fetchall()
        for view in views:
            schema_name = view['table_schema']
            view_name = view['table_name']
            try:
                cursor.execute(f"DROP VIEW IF EXISTS {schema_name}.{view_name} CASCADE")
                print(f"  ✓ Dropped view {schema_name}.{view_name}")
            except Exception as e:
                print(f"  ❌ Failed to drop view {schema_name}.{view_name}: {e}")
        
        # Commit all changes
        conn.commit()
        
        # Verify cleanup
        cursor.execute("""
            SELECT COUNT(*) as count
            FROM information_schema.tables 
            WHERE table_schema IN ('public', 'safetydb')
            AND table_type = 'BASE TABLE'
        """)
        remaining_tables = cursor.fetchone()['count']
        
        cursor.execute("""
            SELECT COUNT(*) as count
            FROM information_schema.sequences 
            WHERE sequence_schema IN ('public', 'safetydb')
        """)
        remaining_sequences = cursor.fetchone()['count']
        
        print(f"\n✅ Database Cleanup Complete!")
        print(f"  - Tables dropped: {dropped_count}")
        print(f"  - Sequences dropped: {len(sequences)}")
        print(f"  - Views dropped: {len(views)}")
        print(f"  - Remaining tables: {remaining_tables}")
        print(f"  - Remaining sequences: {remaining_sequences}")
        
        if remaining_tables == 0 and remaining_sequences == 0:
            print("🎯 Database is now completely clean!")
        else:
            print("⚠️  Some objects may still remain - check manually if needed")
        
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
    drop_all_tables()