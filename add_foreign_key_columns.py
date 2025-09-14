#!/usr/bin/env python3
"""
Add foreign key columns to public.regulatory_events table
Creates proper relationships between regulatory events and companies
"""
import psycopg2
from psycopg2.extras import RealDictCursor
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def add_foreign_key_columns():
    """Add foreign key columns to public.regulatory_events table"""
    
    db_config = {
        'host': 'localhost',
        'database': 'African_Country',
        'user': 'divyanshsingh',
        'port': 5432
    }
    
    try:
        conn = psycopg2.connect(**db_config, cursor_factory=RealDictCursor)
        cursor = conn.cursor()
        
        print("🔗 Adding Foreign Key Columns to regulatory_events")
        print("=" * 50)
        
        # Check if columns already exist
        print("🔍 Checking existing columns...")
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'public' 
            AND table_name = 'regulatory_events'
            AND column_name LIKE '%company_id'
        """)
        
        existing_columns = [row['column_name'] for row in cursor.fetchall()]
        print(f"  Found existing company_id columns: {existing_columns}")
        
        # Define the columns to add
        columns_to_add = [
            ('manufacturer_company_id', 'Manufacturer company reference'),
            ('recalling_firm_company_id', 'Recalling firm company reference'),
            ('distributor_company_id', 'Distributor company reference')
        ]
        
        # Add columns that don't exist
        for column_name, description in columns_to_add:
            if column_name not in existing_columns:
                print(f"➕ Adding column: {column_name} ({description})")
                cursor.execute(f"""
                    ALTER TABLE public.regulatory_events 
                    ADD COLUMN {column_name} UUID REFERENCES safetydb.companies(id)
                """)
            else:
                print(f"✅ Column {column_name} already exists")
        
        # Create indexes for better performance
        print("\n📊 Creating performance indexes...")
        
        indexes_to_create = [
            ('idx_regulatory_events_manufacturer_company_id', 'manufacturer_company_id'),
            ('idx_regulatory_events_recalling_firm_company_id', 'recalling_firm_company_id'),
            ('idx_regulatory_events_distributor_company_id', 'distributor_company_id')
        ]
        
        for index_name, column_name in indexes_to_create:
            try:
                cursor.execute(f"""
                    CREATE INDEX IF NOT EXISTS {index_name} 
                    ON public.regulatory_events({column_name})
                """)
                print(f"  ✓ Created index: {index_name}")
            except psycopg2.Error as e:
                if "already exists" in str(e):
                    print(f"  ✓ Index {index_name} already exists")
                else:
                    print(f"  ❌ Failed to create index {index_name}: {e}")
        
        conn.commit()
        
        # Verify the changes
        print("\n📋 Verification Results:")
        cursor.execute("""
            SELECT 
                column_name, 
                data_type, 
                is_nullable,
                column_default
            FROM information_schema.columns 
            WHERE table_schema = 'public' 
            AND table_name = 'regulatory_events'
            AND column_name LIKE '%company_id'
            ORDER BY column_name
        """)
        
        columns = cursor.fetchall()
        print("  New foreign key columns:")
        for col in columns:
            nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
            print(f"    - {col['column_name']}: {col['data_type']} ({nullable})")
        
        # Show indexes
        cursor.execute("""
            SELECT 
                indexname, 
                indexdef
            FROM pg_indexes 
            WHERE tablename = 'regulatory_events' 
            AND schemaname = 'public'
            AND indexname LIKE '%company_id%'
            ORDER BY indexname
        """)
        
        indexes = cursor.fetchall()
        print(f"\n  Performance indexes created: {len(indexes)}")
        for idx in indexes:
            print(f"    - {idx['indexname']}")
        
        # Check current data
        cursor.execute("SELECT COUNT(*) as count FROM public.regulatory_events")
        total_events = cursor.fetchone()['count']
        
        print(f"\n📊 Current Data Status:")
        print(f"  - Total regulatory events: {total_events}")
        print(f"  - Foreign key columns: {len(columns)} added")
        print(f"  - Performance indexes: {len(indexes)} created")
        
        print(f"\n✅ Foreign key columns added successfully!")
        print("🎯 Ready for company relationship mapping")
        
        cursor.close()
        conn.close()
        
    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
        if 'conn' in locals():
            conn.rollback()
    except Exception as e:
        logger.error(f"Error adding foreign key columns: {e}")
        if 'conn' in locals():
            conn.rollback()

if __name__ == '__main__':
    add_foreign_key_columns()