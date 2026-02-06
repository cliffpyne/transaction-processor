#!/usr/bin/env python3
"""
Check if google.json is properly configured for Service Account
"""

import json
import os

def check_google_json():
    print("=" * 60)
    print("Checking google.json configuration (Service Account)")
    print("=" * 60)
    print()
    
    # Check if file exists
    if not os.path.exists('google.json'):
        print("❌ ERROR: google.json file not found!")
        print("   Please add your Google Service Account credentials as 'google.json'")
        print()
        print("   How to get it:")
        print("   1. Go to Google Cloud Console")
        print("   2. IAM & Admin → Service Accounts")
        print("   3. Create or select a service account")
        print("   4. Keys tab → Add Key → Create new key → JSON")
        print("   5. Rename downloaded file to 'google.json'")
        return False
    
    print("✅ google.json file found")
    
    # Try to read and parse JSON
    try:
        with open('google.json', 'r') as f:
            data = json.load(f)
        print("✅ google.json is valid JSON")
    except json.JSONDecodeError as e:
        print(f"❌ ERROR: google.json is not valid JSON: {e}")
        return False
    
    # Check if it's a Service Account
    if data.get('type') != 'service_account':
        print("❌ ERROR: This is not a Service Account JSON file")
        print(f"   Found type: {data.get('type')}")
        print("   Expected: 'service_account'")
        print()
        print("   You may have downloaded OAuth credentials instead.")
        print("   Please download Service Account JSON key.")
        return False
    
    print("✅ Correct type: Service Account")
    
    # Check required fields
    required_fields = [
        'type', 'project_id', 'private_key_id', 'private_key', 
        'client_email', 'client_id', 'auth_uri', 'token_uri'
    ]
    
    missing_fields = []
    for field in required_fields:
        if field not in data:
            missing_fields.append(field)
        else:
            print(f"✅ Has {field}")
    
    if missing_fields:
        print()
        print(f"❌ ERROR: Missing required fields: {', '.join(missing_fields)}")
        return False
    
    # Show service account email
    print()
    print("=" * 60)
    print("📧 Service Account Email:")
    print(f"   {data['client_email']}")
    print("=" * 60)
    print()
    print("⚠️  IMPORTANT: You MUST share your Google Sheets with this email!")
    print()
    print("Steps to share:")
    print("1. Open your Google Sheet")
    print("2. Click 'Share' button")
    print("3. Paste this email: " + data['client_email'])
    print("4. Give 'Editor' access")
    print("5. Click 'Send' (uncheck notify)")
    print()
    print("Sheets to share:")
    print("- PASSED/FAILED: https://docs.google.com/spreadsheets/d/1N3ZxahtaFBX0iK3cijDraDmyZM8573PVVf8D-WVqicE/")
    print("- Pikipiki Records: https://docs.google.com/spreadsheets/d/1XFwPITQgZmzZ8lbg8MKD9S4rwHyk2cDOKrcxO7SAjHA/")
    
    # Check private key format
    if not data['private_key'].startswith('-----BEGIN PRIVATE KEY-----'):
        print()
        print("⚠️  WARNING: Private key doesn't look correct")
        print("   It should start with: -----BEGIN PRIVATE KEY-----")
    
    print()
    print("=" * 60)
    print("✅ Configuration check complete!")
    print("=" * 60)
    print()
    print("Next steps:")
    print("1. Make sure Google Sheets API is enabled in Cloud Console")
    print("2. Share BOTH Google Sheets with the service account email above")
    print("3. Run: python app.py")
    print()
    return True

if __name__ == '__main__':
    check_google_json()
