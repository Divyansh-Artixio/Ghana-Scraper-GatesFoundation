#!/usr/bin/env python3
"""
Setup SafetyDB schema with correct structure for Ghana scraper
"""
import psycopg2
from psycopg2.extras import RealDictCursor
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def setup_safetydb_schema():
    """Create or update safetydb schema with correct structure"""
    
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
        
        logger.info("🔧 Setting up SafetyDB schema...")
        
        # Create safetydb schema if it doesn't exist
        cursor.execute("CREATE SCHEMA IF NOT EXISTS safetydb")
        
        # Enable UUID extension
        cursor.execute("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"")
        
        # Drop existing tables to recreate with correct structure
        cursor.execute("DROP TABLE IF EXISTS safetydb.regulatory_events CASCADE")
        cursor.execute("DROP TABLE IF EXISTS safetydb.companies CASCADE")
        cursor.execute("DROP TABLE IF EXISTS safetydb.countries CASCADE")
        
        # Create countries table
        cursor.execute("""
            CREATE TABLE safetydb.countries (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                code VARCHAR(3) NOT NULL UNIQUE,
                name VARCHAR(100) NOT NULL UNIQUE,
                region VARCHAR(100),
                population BIGINT,
                gdp_per_capita NUMERIC(10,2),
                healthcare_index NUMERIC(5,2),
                regulatory_maturity VARCHAR(50) CHECK (regulatory_maturity IN ('developing','intermediate','advanced')),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create companies table
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
        
        # Create regulatory_events table
        cursor.execute("""
            CREATE TABLE safetydb.regulatory_events (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                url VARCHAR(500) NOT NULL UNIQUE,
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
                -- Additional detailed fields from PDF extraction
                detailed_content TEXT,
                manufacturing_firm TEXT,
                importing_firm TEXT,
                distributing_firm TEXT,
                product_description TEXT,
                hazard_description TEXT,
                corrective_action TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_safetydb_companies_name ON safetydb.companies(name)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_safetydb_events_type ON safetydb.regulatory_events(event_type)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_safetydb_events_date ON safetydb.regulatory_events(alert_date, notice_date, recall_date)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_safetydb_events_url ON safetydb.regulatory_events(url)")
        
        # Insert Ghana country
        cursor.execute("""
            INSERT INTO safetydb.countries (code, name, region, regulatory_maturity)
            VALUES ('GH', 'Ghana', 'West Africa', 'intermediate')
            ON CONFLICT (code) DO UPDATE SET
                name = EXCLUDED.name,
                region = EXCLUDED.region,
                regulatory_maturity = EXCLUDED.regulatory_maturity
        """)
        
        conn.commit()
        logger.info("✅ SafetyDB schema setup complete!")
        
        # Verify tables exist
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'safetydb'
            ORDER BY table_name
        """)
        tables = cursor.fetchall()
        logger.info(f"📊 Created tables: {[t['table_name'] for t in tables]}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"❌ Error setting up SafetyDB schema: {e}")
        raise

if __name__ == '__main__':
    setup_safetydb_schema()