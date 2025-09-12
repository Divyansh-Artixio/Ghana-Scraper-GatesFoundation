#!/usr/bin/env python3
"""
Create three main tables in safetydb schema and migrate data from public schema
"""
import psycopg2
from psycopg2.extras import RealDictCursor
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def migrate_to_safetydb():
    """Create tables in safetydb schema and migrate data from public schema"""
    
    db_config = {
        'host': 'localhost',
        'database': 'African_Country',
        'user': 'divyanshsingh',
        'port': 5432
    }
    
    try:
        conn = psycopg2.connect(**db_config, cursor_factory=RealDictCursor)
        cursor = conn.cursor()
        
        print("🏗️  Creating Tables in SafetyDB Schema")
        print("=" * 40)
        
        # Check existing tables and create missing ones
        print("🔍 Checking existing tables in safetydb schema...")
        
        # Check what tables already exist
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'safetydb' 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """)
        existing_tables = [row['table_name'] for row in cursor.fetchall()]
        print(f"  Found existing tables: {', '.join(existing_tables)}")
        
        # Create missing tables only
        if 'countries' not in existing_tables:
            print("📍 Creating safetydb.countries table...")
            cursor.execute("""
                CREATE TABLE safetydb.countries (
                    code VARCHAR(10) PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    who_maturity_level INT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
        else:
            print("📍 safetydb.countries table already exists")
        
        # For companies table, we'll work with the existing structure
        if 'companies' not in existing_tables:
            print("🏢 Creating safetydb.companies table...")
            cursor.execute("""
                CREATE TABLE safetydb.companies (
                    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                    name VARCHAR(255) NOT NULL,
                    country_of_origin VARCHAR(100),
                    address TEXT,
                    beneficial_owners JSONB,
                    risk_score INT CHECK (risk_score >= 0 AND risk_score <= 100),
                    total_violations INT DEFAULT 0,
                    logo_url VARCHAR(500),
                    website VARCHAR(500),
                    established_year INT,
                    company_size VARCHAR(50) CHECK (company_size IN ('small','medium','large','multinational')),
                    primary_products TEXT[],
                    regulatory_status VARCHAR(100),
                    last_inspection_date DATE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
        else:
            print("🏢 safetydb.companies table already exists")
        
        # Create a simplified regulatory events table that works with existing structure
        if 'regulatory_events' not in existing_tables:
            print("📋 Creating safetydb.regulatory_events table...")
            cursor.execute("""
                CREATE TABLE safetydb.regulatory_events (
                    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                    url VARCHAR(500) NOT NULL UNIQUE,
                    headline VARCHAR(500),
                    date_published DATE,
                    date_scraped TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    category VARCHAR(100),
                    subcategory VARCHAR(100),
                    tags TEXT[],
                    content TEXT,
                    companies_mentioned UUID[],
                    product_names TEXT[],
                    api_enriched_content JSONB,
                    source_document_id VARCHAR(255),
                    is_latest_version BOOLEAN DEFAULT true,
                    
                    -- Event specific fields
                    event_type VARCHAR(50) CHECK (event_type IN ('Alert','Public Notice','Product Recall')),
                    alert_date DATE,
                    alert_name VARCHAR(255),
                    all_text TEXT,
                    notice_date DATE,
                    notice_text TEXT,
                    recall_date DATE,
                    product_name VARCHAR(255),
                    product_type VARCHAR(100),
                    manufacturer_id UUID REFERENCES safetydb.companies(id),
                    recalling_firm_id UUID REFERENCES safetydb.companies(id),
                    batches VARCHAR(255),
                    manufacturing_date DATE,
                    expiry_date DATE,
                    source_url TEXT,
                    pdf_path TEXT,
                    reason_for_action TEXT,
                    
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
        else:
            print("📋 safetydb.regulatory_events table already exists")
        
        conn.commit()
        print("✅ Table structure verified!")
        
        # Now migrate data from public schema
        print("\n📦 Migrating Data from Public Schema")
        print("=" * 35)
        
        # 1. Migrate countries (mapping to existing structure)
        print("📍 Migrating countries...")
        cursor.execute("SELECT COUNT(*) FROM public.countries")
        countries_count = cursor.fetchone()['count']
        
        if countries_count > 0:
            cursor.execute("""
                INSERT INTO safetydb.countries (code, name, regulatory_maturity)
                SELECT 
                    code, 
                    name,
                    CASE 
                        WHEN who_maturity_level >= 4 THEN 'advanced'
                        WHEN who_maturity_level >= 2 THEN 'intermediate'
                        ELSE 'developing'
                    END as regulatory_maturity
                FROM public.countries
                ON CONFLICT (code) DO UPDATE SET
                    name = EXCLUDED.name,
                    regulatory_maturity = EXCLUDED.regulatory_maturity
            """)
            migrated_countries = cursor.rowcount
            print(f"  ✓ Migrated {migrated_countries} countries")
        else:
            print("  ⚠️ No countries found in public schema")
        
        # 2. Migrate companies (mapping to existing structure)
        print("🏢 Migrating companies...")
        cursor.execute("SELECT COUNT(*) FROM public.companies")
        companies_count = cursor.fetchone()['count']
        
        if companies_count > 0:
            cursor.execute("""
                INSERT INTO safetydb.companies (
                    name, country_of_origin, established_year, created_at, updated_at
                )
                SELECT 
                    name, 
                    COALESCE(country_code, 'Unknown') as country_of_origin,
                    EXTRACT(YEAR FROM founding_date) as established_year,
                    created_at, 
                    updated_at
                FROM public.companies
                ON CONFLICT (name) DO UPDATE SET
                    country_of_origin = EXCLUDED.country_of_origin,
                    established_year = EXCLUDED.established_year,
                    updated_at = CURRENT_TIMESTAMP
            """)
            migrated_companies = cursor.rowcount
            print(f"  ✓ Migrated {migrated_companies} companies")
        else:
            print("  ⚠️ No companies found in public schema")
        
        # 3. Migrate regulatory events (simplified for existing structure)
        print("📋 Migrating regulatory events...")
        cursor.execute("SELECT COUNT(*) FROM public.regulatory_events")
        events_count = cursor.fetchone()['count']
        
        if events_count > 0:
            # First, let's create a mapping table for company IDs if needed
            cursor.execute("""
                INSERT INTO safetydb.regulatory_events (
                    url, headline, date_published, date_scraped, category, subcategory,
                    tags, content, product_names, api_enriched_content,
                    source_document_id, is_latest_version, event_type, alert_date, alert_name,
                    all_text, notice_date, notice_text, recall_date, product_name, product_type,
                    batches, manufacturing_date, expiry_date, source_url, pdf_path, 
                    reason_for_action, created_at, updated_at
                )
                SELECT 
                    url, headline, date_published, date_scraped, category, subcategory,
                    tags, content, product_names, api_enriched_content,
                    source_document_id, is_latest_version, event_type, alert_date, alert_name,
                    all_text, notice_date, notice_text, recall_date, product_name, product_type,
                    batches, manufacturing_date, expiry_date, source_url, pdf_path,
                    reason_for_action, created_at, updated_at
                FROM public.regulatory_events
                ON CONFLICT (url) DO UPDATE SET
                    headline = EXCLUDED.headline,
                    date_published = EXCLUDED.date_published,
                    category = EXCLUDED.category,
                    subcategory = EXCLUDED.subcategory,
                    tags = EXCLUDED.tags,
                    content = EXCLUDED.content,
                    product_names = EXCLUDED.product_names,
                    api_enriched_content = EXCLUDED.api_enriched_content,
                    updated_at = CURRENT_TIMESTAMP
            """)
            migrated_events = cursor.rowcount
            print(f"  ✓ Migrated {migrated_events} regulatory events")
        else:
            print("  ⚠️ No regulatory events found in public schema")
        
        conn.commit()
        
        # Show final counts
        print("\n📊 Final Data Counts in SafetyDB:")
        cursor.execute("SELECT COUNT(*) FROM safetydb.countries")
        final_countries = cursor.fetchone()['count']
        
        cursor.execute("SELECT COUNT(*) FROM safetydb.companies")
        final_companies = cursor.fetchone()['count']
        
        cursor.execute("SELECT COUNT(*) FROM safetydb.regulatory_events")
        final_events = cursor.fetchone()['count']
        
        print(f"  - Countries: {final_countries}")
        print(f"  - Companies: {final_companies}")
        print(f"  - Regulatory Events: {final_events}")
        
        print(f"\n✅ Migration completed successfully!")
        print("🎯 Three main tables created and populated in safetydb schema")
        
        cursor.close()
        conn.close()
        
    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
        if 'conn' in locals():
            conn.rollback()
    except Exception as e:
        logger.error(f"Error during migration: {e}")
        if 'conn' in locals():
            conn.rollback()

if __name__ == '__main__':
    migrate_to_safetydb()