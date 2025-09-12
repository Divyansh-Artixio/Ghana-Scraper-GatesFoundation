#!/usr/bin/env python3
"""
Setup script for Ghana FDA Regulatory Scraper
"""
import os
import subprocess
import sys

def run_command(command, description):
    """Run a command and handle errors"""
    print(f"🔄 {description}...")
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print(f"✅ {description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed: {e}")
        if e.stdout:
            print(f"Output: {e.stdout}")
        if e.stderr:
            print(f"Error: {e.stderr}")
        return False

def main():
    """Main setup function"""
    print("🇬🇭 Ghana FDA Regulatory Scraper Setup")
    print("=" * 50)
    
    # Check Python version
    if sys.version_info < (3, 8):
        print("❌ Python 3.8+ is required")
        sys.exit(1)
    
    print(f"✅ Python {sys.version_info.major}.{sys.version_info.minor} detected")
    
    # Create virtual environment
    if not os.path.exists('venv'):
        if not run_command('python3 -m venv venv', 'Creating virtual environment'):
            sys.exit(1)
    else:
        print("✅ Virtual environment already exists")
    
    # Activate virtual environment and install dependencies
    if os.name == 'nt':  # Windows
        activate_cmd = 'venv\\Scripts\\activate'
        pip_cmd = 'venv\\Scripts\\pip'
    else:  # Unix/Linux/macOS
        activate_cmd = 'source venv/bin/activate'
        pip_cmd = 'venv/bin/pip'
    
    # Install requirements
    if not run_command(f'{pip_cmd} install -r requirements.txt', 'Installing Python dependencies'):
        sys.exit(1)
    
    # Install Playwright browsers
    if os.name == 'nt':  # Windows
        playwright_cmd = 'venv\\Scripts\\playwright install'
    else:  # Unix/Linux/macOS
        playwright_cmd = 'venv/bin/playwright install'
    
    if not run_command(playwright_cmd, 'Installing Playwright browsers'):
        sys.exit(1)
    
    # Create .env file if it doesn't exist
    if not os.path.exists('.env'):
        if os.path.exists('.env.example'):
            run_command('cp .env.example .env', 'Creating .env file from template')
            print("📝 Please edit .env file with your database credentials")
        else:
            print("⚠️  .env.example not found, please create .env manually")
    else:
        print("✅ .env file already exists")
    
    # Create output directory
    if not os.path.exists('output'):
        os.makedirs('output')
        print("✅ Created output directory")
    
    print("\n🎉 Setup completed successfully!")
    print("\n📋 Next steps:")
    print("1. Edit .env file with your database credentials")
    print("2. Create PostgreSQL database: createdb ghana_regulatory")
    print("3. Run database schema: psql -d ghana_regulatory -f schema.sql")
    print("4. Test setup: python test_db_connection.py")
    print("5. Run scraper: python run_scraper.py")
    print("\n📖 See README.md for detailed instructions")

if __name__ == '__main__':
    main()