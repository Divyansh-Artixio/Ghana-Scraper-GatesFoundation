"""
Database configuration and connection management for Ghana Regulatory Scraper
"""
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
from typing import Optional, Dict, Any, List
import logging

logger = logging.getLogger(__name__)

class DatabaseManager:
    """Manages database connections and operations"""
    
    def __init__(self):
        self.host = os.getenv('DB_HOST', 'localhost')
        self.database = os.getenv('DB_NAME', 'safetyiq')
        self.user = os.getenv('DB_USER', 'sanatanupmanyu')
        self.password = os.getenv('DB_PASSWORD', 'ksDq2jazKmxxzv.VxXbkwR6Uxz')
        self.port = os.getenv('DB_PORT', '5432')
        
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = None
        try:
            conn = psycopg2.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
                port=self.port,
                cursor_factory=RealDictCursor
            )
            yield conn
        except psycopg2.Error as e:
            logger.error(f"Database connection error: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """Execute a SELECT query and return results"""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                return cursor.fetchall()
    
    def execute_insert(self, query: str, params: Optional[tuple] = None) -> Optional[int]:
        """Execute an INSERT query and return the ID"""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                result = cursor.fetchone()
                conn.commit()
                return result['id'] if result and 'id' in result else None
    
    def execute_update(self, query: str, params: Optional[tuple] = None) -> int:
        """Execute an UPDATE/DELETE query and return affected rows"""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                affected_rows = cursor.rowcount
                conn.commit()
                return affected_rows
    
    def get_or_create_company(self, name: str, company_type: str) -> int:
        """Get existing company or create new one"""
        # First, try to find existing company
        query = "SELECT id FROM companies WHERE LOWER(name) = LOWER(%s)"
        results = self.execute_query(query, (name,))
        
        if results:
            return results[0]['id']
        
        # Create new company
        insert_query = """
        INSERT INTO companies (name, type) 
        VALUES (%s, %s) 
        RETURNING id
        """
        return self.execute_insert(insert_query, (name, company_type))
    
    def insert_regulatory_event(self, event_data: Dict[str, Any]) -> int:
        """Insert a new regulatory event"""
        query = """
        INSERT INTO regulatory_events (
            event_type, alert_date, alert_name, all_text,
            notice_date, notice_text, recall_date, product_name,
            product_type, manufacturer_id, recalling_firm_id,
            batches, manufacturing_date, expiry_date,
            source_url, pdf_path, reason_for_action
        ) VALUES (
            %(event_type)s, %(alert_date)s, %(alert_name)s, %(all_text)s,
            %(notice_date)s, %(notice_text)s, %(recall_date)s, %(product_name)s,
            %(product_type)s, %(manufacturer_id)s, %(recalling_firm_id)s,
            %(batches)s, %(manufacturing_date)s, %(expiry_date)s,
            %(source_url)s, %(pdf_path)s, %(reason_for_action)s
        ) RETURNING id
        """
        
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, event_data)
                result = cursor.fetchone()
                conn.commit()
                return result['id'] if result else None
    
    def update_company_details(self, company_id: int, details: Dict[str, Any]):
        """Update company with AI enrichment details"""
        query = """
        UPDATE companies 
        SET founding_date = %s, 
            promoter_founder_name = %s, 
            company_brief = %s,
            country_code = %s,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = %s
        """
        
        self.execute_update(query, (
            details.get('founding_date'),
            details.get('promoter_founder_name'),
            details.get('company_brief'),
            details.get('country_code'),
            company_id
        ))
    
    def check_event_exists(self, event_type: str, source_url: str) -> bool:
        """Check if event already exists to avoid duplicates"""
        query = "SELECT id FROM regulatory_events WHERE event_type = %s AND source_url = %s"
        results = self.execute_query(query, (event_type, source_url))
        return len(results) > 0
    
    def get_countries(self) -> List[Dict[str, Any]]:
        """Get all countries for reference"""
        return self.execute_query("SELECT code, name FROM countries ORDER BY name")

# Global database manager instance
db_manager = DatabaseManager()
