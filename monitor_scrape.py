#!/usr/bin/env python3
"""
Monitor the scraping progress
"""
import psycopg2
from psycopg2.extras import RealDictCursor
import time
import os

def monitor_progress():
    """Monitor scraping progress"""
    
    db_config = {
        'host': 'localhost',
        'database': 'safetyiq',
        'user': 'sanatanupmanyu',
        'password': 'ksDq2jazKmxxzv.VxXbkwR6Uxz',
        'port': 5432
    }
    
    print("📊 Monitoring Ghana FDA Scraper Progress")
    print("=" * 50)
    
    try:
        conn = psycopg2.connect(**db_config, cursor_factory=RealDictCursor)
        cursor = conn.cursor()
        
        while True:
            # Get current counts
            cursor.execute("""
                SELECT event_type, COUNT(*) as count
                FROM regulatory_events 
                GROUP BY event_type
                ORDER BY count DESC
            """)
            results = cursor.fetchall()
            
            total_events = sum(row['count'] for row in results)
            
            # Clear screen and show progress
            os.system('clear' if os.name == 'posix' else 'cls')
            print("📊 Ghana FDA Scraper - Live Progress")
            print("=" * 40)
            print(f"🕒 {time.strftime('%H:%M:%S')}")
            print()
            
            if results:
                for row in results:
                    print(f"📋 {row['event_type']}: {row['count']} records")
                print(f"📁 Total Events: {total_events}")
            else:
                print("⏳ No data yet - scraper starting...")
            
            # Show recent events
            cursor.execute("""
                SELECT event_type, 
                       COALESCE(alert_name, notice_text, product_name) as title,
                       created_at
                FROM regulatory_events 
                ORDER BY created_at DESC 
                LIMIT 5
            """)
            recent = cursor.fetchall()
            
            if recent:
                print("\n🕒 Latest Events:")
                print("-" * 25)
                for event in recent:
                    title = (event['title'] or 'Untitled')[:40]
                    time_str = event['created_at'].strftime('%H:%M:%S')
                    print(f"• {time_str} - {event['event_type']}: {title}")
            
            print("\n⏹️  Press Ctrl+C to stop monitoring")
            time.sleep(5)
            
    except KeyboardInterrupt:
        print("\n👋 Monitoring stopped")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == '__main__':
    monitor_progress()