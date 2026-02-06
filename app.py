from flask import Flask, render_template, request, jsonify, session
from werkzeug.utils import secure_filename
import os
import re
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import json
import pickle
from datetime import datetime

# app = Flask(__name__)
# app.secret_key = os.environ.get('SECRET_KEY', 'your-secret-key-change-this')
# app.config['UPLOAD_FOLDER'] = 'uploads'
# app.config['TEMP_FOLDER'] = 'temp_reviews'  # 🔥 NEW: For storing review data
# app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size

# # Ensure folders exist
# os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
# os.makedirs(app.config['TEMP_FOLDER'], exist_ok=True)  # 🔥 NEW

# # For Render deployment - read credentials from environment
# GOOGLE_CREDS = os.environ.get('GOOGLE_CREDENTIALS')
# if GOOGLE_CREDS:
#     with open('google.json', 'w') as f:
#         f.write(GOOGLE_CREDS)


app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'dev-secret-key-change-in-production')
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['TEMP_FOLDER'] = 'temp_reviews'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024

# Ensure folders exist
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['TEMP_FOLDER'], exist_ok=True)

# For Render deployment - read credentials from environment
GOOGLE_CREDS = os.environ.get('GOOGLE_CREDENTIALS_JSON')
if GOOGLE_CREDS:
    try:
        # Parse JSON string from environment variable
        creds_dict = json.loads(GOOGLE_CREDS)
        with open('google.json', 'w') as f:
            json.dump(creds_dict, f, indent=2)
        print("✅ Google credentials loaded from environment")
    except json.JSONDecodeError as e:
        print(f"❌ Error parsing GOOGLE_CREDENTIALS_JSON: {e}")
        raise
elif os.path.exists('google.json'):
    print("✅ Using local google.json file")
else:
    print("⚠️ No Google credentials found!")

# Google Sheets configuration
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
PASSED_SHEET_ID = '1rdSRNLdZPT5xXLRgV7wSn1beYwWZp41ZpYoLkbGmt0o'
PIKIPIKI_SHEET_ID = '1XFwPITQgZmzZ8lbg8MKD9S4rwHyk2cDOKrcxO7SAjHA'

def get_google_service():
    """Create Google Sheets service using Service Account"""
    try:
        credentials = service_account.Credentials.from_service_account_file(
            'google.json',
            scopes=SCOPES
        )
        service = build('sheets', 'v4', credentials=credentials)
        return service
    except Exception as e:
        print(f"Error creating service: {e}")
        raise

def extract_phone_number(text):
    """
    Extract phone number from text - IMPROVED to avoid account numbers
    Formats: 255XXXXXXXXX, 07XXXXXXXX, 06XXXXXXXX
    
    CRITICAL: Must NOT extract from account numbers like FRANKAB17701296648323397750
    """
    if not text or pd.isna(text):
        return None
    
    text = str(text).replace(' ', '').replace('-', '')
    
    # 🔥 IMPROVED: Exclude account numbers - they contain FRANKAB followed by long numbers
    if 'FRANKAB' in text.upper() or 'TOFRANKAB' in text.upper():
        # Split by common separators and only check parts that don't contain FRANKAB
        parts = re.split(r'[:\s]+', text)
        for part in parts:
            if 'FRANKAB' not in part.upper() and 'FRANK' not in part.upper():
                phone = _extract_phone_from_clean_text(part)
                if phone:
                    return phone
        return None
    
    return _extract_phone_from_clean_text(text)

def _extract_phone_from_clean_text(text):
    """Helper to extract phone from text without account numbers"""
    # Pattern for 255 followed by 9 digits (must not be part of longer number)
    pattern_255 = r'(?<!\d)255(\d{9})(?!\d)'
    match = re.search(pattern_255, text)
    if match:
        return f"255{match.group(1)}"
    
    # Pattern for 07 or 06 followed by 8 digits (must not be part of longer number)
    pattern_07_06 = r'(?<!\d)0([67])(\d{8})(?!\d)'
    match = re.search(pattern_07_06, text)
    if match:
        return f"0{match.group(1)}{match.group(2)}"
    
    return None

def extract_plate_number(text):
    """
    🔥 IMPROVED: Extract plate number with flexible matching
    Valid formats:
    - MC###XXX (standard: MC567EFL)
    - MC ### XXX (with spaces: MC 567 EFL)
    - mc###xxx (lowercase: mc567efl)
    - MC.###.XXX (with dots: MC.567.EFL)
    - MC-###-XXX (with hyphens: MC-567-EFL)
    - ###XXX (missing MC: 567EFL)
    
    CRITICAL: Must have EXACTLY 3 digits AND 3 letters to be valid
    """
    if not text or pd.isna(text):
        return None
    
    text = str(text).upper()
    
    # Pattern 1: Standard MC###XXX (with optional spaces/dots/hyphens)
    pattern1 = r'MC[\s\.\-]*(\d{3})[\s\.\-]*([A-Z]{3})'
    match = re.search(pattern1, text)
    if match:
        plate = f"MC{match.group(1)}{match.group(2)}"
        print(f"  ✓ Extracted plate (Pattern 1): {plate} from: {text[:80]}")
        return plate
    
    # Pattern 2: ###XXX without MC prefix (must have exactly 3 digits + 3 letters)
    pattern2 = r'(?<!MC)(?<![A-Z])(\d{3})[\s\.\-]*([A-Z]{3})(?![A-Z0-9])'
    match = re.search(pattern2, text)
    if match:
        plate = f"MC{match.group(1)}{match.group(2)}"
        print(f"  ✓ Extracted plate (Pattern 2 - added MC): {plate} from: {text[:80]}")
        return plate
    
    # Pattern 3: XXX### (letters first, then numbers) - uncommon but possible
    pattern3 = r'(?<!MC)(?<![A-Z])([A-Z]{3})[\s\.\-]*(\d{3})(?![A-Z0-9])'
    match = re.search(pattern3, text)
    if match:
        # Swap to correct format: MC###XXX
        plate = f"MC{match.group(2)}{match.group(1)}"
        print(f"  ✓ Extracted plate (Pattern 3 - reversed): {plate} from: {text[:80]}")
        return plate
    
    return None

def extract_plate_suggestions(text):
    """
    🔥 NEW: Extract potential plate numbers that need confirmation
    Returns list of (original_text, suggested_plate, confidence)
    """
    if not text or pd.isna(text):
        return []
    
    text_upper = str(text).upper()
    suggestions = []
    
    # Look for patterns that might be plates but need cleanup
    pattern_messy = r'MC[\s\.\-]*([A-Z]{3,4})[\s\.\-]*(\d{2,4})|([A-Z]{3,4})[\s\.\-]*(\d{2,4})[\s\.\-]*(?:MC)?'
    matches = re.finditer(pattern_messy, text_upper)
    
    for match in matches:
        original = match.group(0)
        
        # Extract numbers and letters
        numbers = ''.join(re.findall(r'\d', original))
        letters = ''.join(re.findall(r'[A-Z]', original.replace('MC', '')))
        
        # Must have exactly 3 numbers and 3 letters to be valid
        if len(numbers) == 3 and len(letters) == 3:
            suggested = f"MC{numbers}{letters}"
            suggestions.append({
                'original': original,
                'suggested': suggested,
                'confidence': 'medium',
                'reason': 'Rearranged format'
            })
    
    return suggestions

def extract_ref_number(text):
    """Extract reference number from message (format: REF:XXXXX)"""
    if not text or pd.isna(text):
        return None
    
    text = str(text)
    pattern = r'REF:\s*(\S+)'
    match = re.search(pattern, text, re.IGNORECASE)
    if match:
        return match.group(1)
    
    return None

def load_all_customers(service):
    """Load all customers from pikipiki records sheet"""
    try:
        sheet = service.spreadsheets()
        result = sheet.values().get(
            spreadsheetId=PIKIPIKI_SHEET_ID,
            range='pikipiki records!A:E'
        ).execute()
        
        values = result.get('values', [])
        if not values:
            return {}, {}
        
        phone_lookup = {}
        plate_lookup = {}
        
        for row in values[1:]:
            plate_col = row[1] if len(row) > 1 else ''
            phone_col = row[3] if len(row) > 3 else ''
            name_col = row[2] if len(row) > 2 else ''
            
            if not plate_col and not phone_col:
                continue
            
            if plate_col:
                plate_clean = str(plate_col).replace(' ', '').upper()
                if plate_clean:
                    plate_lookup[plate_clean] = name_col
            
            if phone_col:
                phone_clean = str(phone_col).replace(' ', '').replace('-', '')
                if phone_clean:
                    phone_lookup[phone_clean] = name_col
        
        print(f"Loaded {len(phone_lookup)} phone numbers and {len(plate_lookup)} plates from pikipiki records")
        return phone_lookup, plate_lookup
        
    except Exception as e:
        print(f"Error loading customers: {e}")
        return {}, {}

def load_all_customers_sav(service):
    """Load all customers from pikipiki records2 sheet (for PASSED_SAV)"""
    try:
        sheet = service.spreadsheets()
        result = sheet.values().get(
            spreadsheetId=PIKIPIKI_SHEET_ID,
            range='pikipiki records2!A:E'
        ).execute()
        
        values = result.get('values', [])
        if not values:
            print("⚠️ No data found in pikipiki records2")
            return {}, {}
        
        phone_lookup_sav = {}
        plate_lookup_sav = {}
        
        for row in values[1:]:
            plate_col = row[1] if len(row) > 1 else ''
            phone_col = row[3] if len(row) > 3 else ''
            name_col = row[2] if len(row) > 2 else ''
            
            if not plate_col and not phone_col:
                continue
            
            if plate_col:
                plate_clean = str(plate_col).replace(' ', '').upper()
                if plate_clean:
                    plate_lookup_sav[plate_clean] = name_col
            
            if phone_col:
                phone_clean = str(phone_col).replace(' ', '').replace('-', '')
                if phone_clean:
                    phone_lookup_sav[phone_clean] = name_col
        
        print(f"✅ Loaded {len(phone_lookup_sav)} phone numbers and {len(plate_lookup_sav)} plates from pikipiki records2 (SAV)")
        return phone_lookup_sav, plate_lookup_sav
        
    except Exception as e:
        print(f"⚠️ Error loading pikipiki records2 (SAV): {e}")
        return {}, {}

def lookup_customer_from_cache(identifier, lookup_type, phone_lookup, plate_lookup):
    """Look up customer from cached data"""
    if lookup_type == 'phone':
        name = phone_lookup.get(identifier)
        if name:
            return name
        
        if identifier.startswith('255'):
            alt_format = '0' + identifier[3:]
            name = phone_lookup.get(alt_format)
            if name:
                return name
        
        elif identifier.startswith('07') or identifier.startswith('06'):
            alt_format = '255' + identifier[1:]
            name = phone_lookup.get(alt_format)
            if name:
                return name
        
        return None
        
    elif lookup_type == 'plate':
        return plate_lookup.get(identifier)
    return None

def get_existing_refs(service, sheet_name='PASSED'):
    """Get existing reference numbers AND messages for duplicate detection"""
    try:
        sheet = service.spreadsheets()
        
        if sheet_name == 'FAILED':
            ref_column = 'I'
        elif sheet_name == 'PASSED_SAV':
            ref_column = 'H'
        else:  # PASSED
            ref_column = 'H'
        
        print(f"📖 Reading {sheet_name}: MESSAGE from column D, REFNUMBER from column {ref_column}")
        
        result = service.spreadsheets().values().batchGet(
            spreadsheetId=PASSED_SHEET_ID,
            ranges=[f'{sheet_name}!D1:D10000', f'{sheet_name}!{ref_column}1:{ref_column}10000']
        ).execute()
        
        value_ranges = result.get('valueRanges', [])
        
        refs = set()
        messages = set()
        
        if len(value_ranges) > 1:
            ref_values = value_ranges[1].get('values', [])
            for idx, row in enumerate(ref_values[1:], start=2):
                if row and len(row) > 0 and row[0]:
                    ref = str(row[0]).strip()
                    if ref and ref.lower() != 'refnumber':
                        refs.add(ref)
        
        if len(value_ranges) > 0:
            message_values = value_ranges[0].get('values', [])
            for idx, row in enumerate(message_values[1:], start=2):
                if row and len(row) > 0 and row[0]:
                    message = str(row[0]).strip()
                    messages.add(message)
                    
                    pattern = r'REF:\s*(\S+)'
                    match = re.search(pattern, message, re.IGNORECASE)
                    if match:
                        ref_from_msg = match.group(1)
                        if ref_from_msg not in refs:
                            refs.add(ref_from_msg)
        
        print(f"✅ {sheet_name}: Found {len(refs)} unique REFs, {len(messages)} unique messages")
        return refs, messages
        
    except Exception as e:
        print(f"❌ Error getting existing data from {sheet_name}: {e}")
        return set(), set()

def get_last_id(service, sheet_name):
    """Get the last ID from the sheet"""
    try:
        sheet = service.spreadsheets()
        result = sheet.values().get(
            spreadsheetId=PASSED_SHEET_ID,
            range=f'{sheet_name}!A:A'
        ).execute()
        
        values = result.get('values', [])
        
        if len(values) > 1:
            for row in reversed(values[1:]):
                if row and len(row) > 0 and row[0]:
                    try:
                        last_id = int(row[0])
                        print(f"Last ID in {sheet_name}: {last_id}")
                        return last_id
                    except (ValueError, TypeError):
                        continue
        
        print(f"No existing IDs found in {sheet_name}, starting from 0")
        return 0
        
    except Exception as e:
        print(f"Error getting last ID: {e}")
        return 0

def get_last_row_number(service, sheet_name):
    """Get the actual last row number (works even with filters)"""
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=PASSED_SHEET_ID,
            range=f'{sheet_name}!A:A'
        ).execute()
        
        values = result.get('values', [])
        return len(values)
    except Exception as e:
        print(f"Error getting last row: {e}")
        return 0

def append_to_sheet(service, sheet_name, data):
    """Append data to Google Sheet - WORKS WITH FILTERS"""
    try:
        last_row = get_last_row_number(service, sheet_name)
        start_row = last_row + 1
        range_name = f'{sheet_name}!A{start_row}'
        
        print(f"Attempting to append to {sheet_name} starting at row {start_row}")
        print(f"Adding {len(data)} rows")
        
        result = service.spreadsheets().values().update(
            spreadsheetId=PASSED_SHEET_ID,
            range=range_name,
            valueInputOption='USER_ENTERED',
            body={'values': data}
        ).execute()
        
        print(f"Update result: {result.get('updatedRows', 0)} rows added")
        return True
        
    except HttpError as e:
        print(f"❌ Google Sheets API Error: {e}")
        if e.resp.status == 403:
            print("Permission denied! Make sure the service account has Editor access to the sheet.")
        return False
    except Exception as e:
        print(f"❌ Error appending to sheet: {e}")
        import traceback
        traceback.print_exc()
        return False

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    
    if not file.filename.endswith('.xlsx'):
        return jsonify({'error': 'Please upload an Excel file (.xlsx)'}), 400
    
    filename = secure_filename(file.filename)
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    file.save(filepath)
    
    session['filepath'] = filepath
    
    return jsonify({'success': True, 'message': 'File uploaded successfully'})

@app.route('/process', methods=['POST'])
def process_transactions():
    try:
        filepath = session.get('filepath')
        if not filepath or not os.path.exists(filepath):
            return jsonify({'error': 'No file uploaded'}), 400
        
        # Read Excel file - CRDB format has headers at row 12
        df = pd.read_excel(filepath, header=12)
        df.columns = df.iloc[0]
        df = df[1:].reset_index(drop=True)
        
        print(f"Columns found: {list(df.columns)}")
        
        required_columns = ['Posting Date', 'Details', 'Credit']
        missing = [col for col in required_columns if col not in df.columns]
        
        if missing:
            return jsonify({
                'error': f'Missing required columns: {missing}. Found: {list(df.columns)}'
            }), 400
        
        # Filter only CREDIT transactions
        df['Credit'] = pd.to_numeric(df['Credit'].astype(str).str.replace(',', ''), errors='coerce')
        df['Debit'] = pd.to_numeric(df['Debit'].astype(str).str.replace(',', ''), errors='coerce')
        
        credit_df = df[(df['Credit'].notna()) & (df['Credit'] > 0) & 
                       ((df['Debit'].isna()) | (df['Debit'] == 0))].copy()
        
        print(f"Found {len(credit_df)} credit transactions")
        
        # Initialize Google Sheets service
        service = get_google_service()
        
        # Load customers
        print("Loading customer database from pikipiki records...")
        phone_lookup, plate_lookup = load_all_customers(service)
        
        print("\nLoading customer database from pikipiki records2 (SAV)...")
        phone_lookup_sav, plate_lookup_sav = load_all_customers_sav(service)
        
        # Get existing refs
        print("Loading existing references from PASSED sheet...")
        existing_passed_refs, existing_passed_messages = get_existing_refs(service, 'PASSED')
        
        print("Loading existing references from PASSED_SAV sheet...")
        existing_passed_sav_refs, existing_passed_sav_messages = get_existing_refs(service, 'PASSED_SAV')
        
        print("Loading existing references from FAILED sheet...")
        existing_failed_refs, existing_failed_messages = get_existing_refs(service, 'FAILED')
        
        all_existing_refs = existing_passed_refs.union(existing_passed_sav_refs).union(existing_failed_refs)
        all_existing_messages = existing_passed_messages.union(existing_passed_sav_messages).union(existing_failed_messages)
        print(f"Total unique refs in system: {len(all_existing_refs)}")
        
        # Get last IDs
        last_passed_id = get_last_id(service, 'PASSED')
        last_passed_sav_id = get_last_id(service, 'PASSED_SAV')
        last_failed_id = get_last_id(service, 'FAILED')
        
        passed_data = []
        passed_sav_data = []
        failed_data = []
        needs_review_data = []
        
        stats = {
            'total': len(credit_df),
            'passed': 0,
            'passed_sav': 0,
            'failed': 0,
            'needs_review': 0,
            'skipped': 0,
            'skipped_from_passed': 0,
            'skipped_from_passed_sav': 0,
            'skipped_from_failed': 0
        }
        
        for idx, row in credit_df.iterrows():
            posting_date = str(row.get('Posting Date', ''))
            details = str(row.get('Details', ''))
            credit_amount = row.get('Credit', 0)
            
            ref_number = extract_ref_number(details)
            
            # Check for duplicates
            is_duplicate = False
            
            if ref_number and ref_number in all_existing_refs:
                is_duplicate = True
                if ref_number in existing_passed_refs:
                    stats['skipped_from_passed'] += 1
                elif ref_number in existing_passed_sav_refs:
                    stats['skipped_from_passed_sav'] += 1
                else:
                    stats['skipped_from_failed'] += 1
            elif details in all_existing_messages:
                is_duplicate = True
                stats['skipped'] += 1
            
            if is_duplicate:
                stats['skipped'] += 1
                continue
            
            # Extract phone and plate
            phone = extract_phone_number(details)
            plate = extract_plate_number(details)
            
            identifier = None
            lookup_type = None
            
            if phone:
                identifier = phone
                lookup_type = 'phone'
                print(f"Found phone: {phone} in: {details[:80]}")
            elif plate:
                identifier = plate
                lookup_type = 'plate'
                print(f"Found plate: {plate} in: {details[:80]}")
            
            if identifier and lookup_type:
                # Check pikipiki records first
                customer_name = lookup_customer_from_cache(identifier, lookup_type, phone_lookup, plate_lookup)
                
                if customer_name:
                    last_passed_id += 1
                    passed_row = [
                        last_passed_id,
                        posting_date,
                        'CRDB',
                        details,
                        credit_amount,
                        identifier,
                        customer_name,
                        ref_number or ''
                    ]
                    passed_data.append(passed_row)
                    stats['passed'] += 1
                    print(f"✅ PASSED: {customer_name} - {identifier} - {credit_amount}")
                else:
                    # Check pikipiki records2 (SAV)
                    customer_name_sav = lookup_customer_from_cache(identifier, lookup_type, phone_lookup_sav, plate_lookup_sav)
                    
                    if customer_name_sav:
                        last_passed_sav_id += 1
                        passed_sav_row = [
                            last_passed_sav_id,
                            posting_date,
                            'CRDB',
                            details,
                            credit_amount,
                            identifier,
                            customer_name_sav,
                            ref_number or ''
                        ]
                        passed_sav_data.append(passed_sav_row)
                        stats['passed_sav'] += 1
                        print(f"✅ PASSED_SAV: {customer_name_sav} - {identifier} - {credit_amount}")
                    else:
                        # Not found - add to FAILED
                        last_failed_id += 1
                        reason = f"{lookup_type.upper()}({identifier}) not found"
                        
                        final_identifier = identifier
                        if lookup_type == 'phone':
                            if not identifier.startswith('255'):
                                if identifier.startswith('0'):
                                    final_identifier = '255' + identifier[1:]
                                else:
                                    final_identifier = '255' + identifier
                        
                        failed_row = [
                            last_failed_id,
                            posting_date,
                            'CRDB',
                            details,
                            credit_amount,
                            final_identifier,
                            reason,
                            ref_number or ''
                        ]
                        failed_data.append(failed_row)
                        stats['failed'] += 1
                        print(f"❌ FAILED: Customer not found for {final_identifier} (REF: {ref_number})")
            else:
                # Check for plate suggestions
                plate_suggestions = extract_plate_suggestions(details)
                
                if plate_suggestions:
                    for suggestion in plate_suggestions:
                        suggested_plate = suggestion['suggested']
                        
                        customer_name = lookup_customer_from_cache(suggested_plate, 'plate', phone_lookup, plate_lookup)
                        customer_name_sav = None
                        
                        if not customer_name:
                            customer_name_sav = lookup_customer_from_cache(suggested_plate, 'plate', phone_lookup_sav, plate_lookup_sav)
                        
                        if customer_name or customer_name_sav:
                            needs_review_data.append({
                                'posting_date': posting_date,
                                'details': details,
                                'credit_amount': credit_amount,
                                'ref_number': ref_number or '',
                                'original_text': suggestion['original'],
                                'suggested_plate': suggested_plate,
                                'customer_name': customer_name or customer_name_sav,
                                'target_sheet': 'PASSED' if customer_name else 'PASSED_SAV',
                                'confidence': suggestion['confidence'],
                                'reason': suggestion['reason']
                            })
                            stats['needs_review'] += 1
                            print(f"🔍 NEEDS REVIEW: {suggestion['original']} -> {suggested_plate} -> {customer_name or customer_name_sav}")
                            break
                    
                    if not needs_review_data or needs_review_data[-1]['details'] != details:
                        last_failed_id += 1
                        failed_row = [
                            last_failed_id,
                            posting_date,
                            'CRDB',
                            details,
                            credit_amount,
                            'No phone/plate',
                            'No identifier',
                            ref_number or ''
                        ]
                        failed_data.append(failed_row)
                        stats['failed'] += 1
                else:
                    last_failed_id += 1
                    failed_row = [
                        last_failed_id,
                        posting_date,
                        'CRDB',
                        details,
                        credit_amount,
                        'No phone/plate',
                        'No identifier',
                        ref_number or ''
                    ]
                    failed_data.append(failed_row)
                    stats['failed'] += 1
                    print(f"❌ FAILED: No phone/plate found in: {details[:80]} (REF: {ref_number})")
        
        # 🔥 FIX: Store review data in file instead of session
        if needs_review_data:
            review_file = os.path.join(app.config['TEMP_FOLDER'], f'review_{datetime.now().timestamp()}.pkl')
            with open(review_file, 'wb') as f:
                pickle.dump({
                    'needs_review': needs_review_data,
                    'passed_data': passed_data,
                    'passed_sav_data': passed_sav_data,
                    'failed_data': failed_data,
                    'stats': stats,
                    'last_ids': {
                        'passed': last_passed_id,
                        'passed_sav': last_passed_sav_id,
                        'failed': last_failed_id
                    }
                }, f)
            
            # Store only file path in session
            session['review_file'] = review_file
            
            return jsonify({
                'needs_review': True,
                'review_data': needs_review_data,
                'stats': stats,
                'message': f"Found {len(needs_review_data)} records that need your review before processing"
            })
        
        # No reviews needed - append directly
        if passed_data:
            append_to_sheet(service, 'PASSED', passed_data)
        
        if passed_sav_data:
            append_to_sheet(service, 'PASSED_SAV', passed_sav_data)
        
        if failed_data:
            append_to_sheet(service, 'FAILED', failed_data)
        
        # Clean up
        if os.path.exists(filepath):
            os.remove(filepath)
        
        return jsonify({
            'success': True,
            'stats': stats,
            'message': f"Processed {stats['total']} transactions: {stats['passed']} passed, {stats['passed_sav']} passed (SAV), {stats['failed']} failed"
        })
    
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/confirm-reviews', methods=['POST'])
def confirm_reviews():
    """🔥 FIXED: Process user confirmations with proper stats handling"""
    try:
        data = request.get_json()
        confirmations = data.get('confirmations', [])
        
        # 🔥 FIX: Load from file instead of session
        review_file = session.get('review_file')
        if not review_file or not os.path.exists(review_file):
            return jsonify({'error': 'Review data not found'}), 400
        
        with open(review_file, 'rb') as f:
            review_data = pickle.load(f)
        
        needs_review = review_data['needs_review']
        passed_data = review_data['passed_data']
        passed_sav_data = review_data['passed_sav_data']
        failed_data = review_data['failed_data']
        stats = review_data['stats']
        last_ids = review_data['last_ids']
        
        service = get_google_service()
        
        # Process confirmations
        for confirmation in confirmations:
            idx = confirmation['index']
            accept = confirmation['accept']
            
            if idx >= len(needs_review):
                continue
            
            review_item = needs_review[idx]
            
            if accept:
                if review_item['target_sheet'] == 'PASSED':
                    last_ids['passed'] += 1
                    row = [
                        last_ids['passed'],
                        review_item['posting_date'],
                        'CRDB',
                        review_item['details'],
                        review_item['credit_amount'],
                        review_item['suggested_plate'],
                        review_item['customer_name'],
                        review_item['ref_number']
                    ]
                    passed_data.append(row)
                    stats['passed'] += 1
                else:
                    last_ids['passed_sav'] += 1
                    row = [
                        last_ids['passed_sav'],
                        review_item['posting_date'],
                        'CRDB',
                        review_item['details'],
                        review_item['credit_amount'],
                        review_item['suggested_plate'],
                        review_item['customer_name'],
                        review_item['ref_number']
                    ]
                    passed_sav_data.append(row)
                    stats['passed_sav'] += 1
            else:
                last_ids['failed'] += 1
                row = [
                    last_ids['failed'],
                    review_item['posting_date'],
                    'CRDB',
                    review_item['details'],
                    review_item['credit_amount'],
                    review_item['suggested_plate'],
                    'Rejected by user',
                    review_item['ref_number']
                ]
                failed_data.append(row)
                stats['failed'] += 1
        
        # Append to sheets
        if passed_data:
            append_to_sheet(service, 'PASSED', passed_data)
        
        if passed_sav_data:
            append_to_sheet(service, 'PASSED_SAV', passed_sav_data)
        
        if failed_data:
            append_to_sheet(service, 'FAILED', failed_data)
        
        # Clean up
        filepath = session.get('filepath')
        if filepath and os.path.exists(filepath):
            os.remove(filepath)
        
        if os.path.exists(review_file):
            os.remove(review_file)
        
        session.pop('review_file', None)
        
        return jsonify({
            'success': True,
            'stats': stats,
            'message': f"Processing and update complete: {stats.get('passed', 0)} passed, {stats.get('passed_sav', 0)} passed (SAV), {stats.get('failed', 0)} failed"
        })
    
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/check-auth', methods=['GET'])
def check_auth():
    """Check if Google Service Account is configured"""
    try:
        service = get_google_service()
        return jsonify({'authenticated': True, 'message': 'Service Account configured'})
    except Exception as e:
        return jsonify({'authenticated': False, 'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)










# from flask import Flask, render_template, request, jsonify, session
# from werkzeug.utils import secure_filename
# import os
# import re
# import pandas as pd
# from google.oauth2 import service_account
# from googleapiclient.discovery import build
# from googleapiclient.errors import HttpError
# import json
# from datetime import datetime

# app = Flask(__name__)
# app.secret_key = os.environ.get('SECRET_KEY', 'your-secret-key-change-this')
# app.config['UPLOAD_FOLDER'] = 'uploads'
# app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size

# # Ensure upload folder exists
# os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# # For Render deployment - read credentials from environment
# GOOGLE_CREDS = os.environ.get('GOOGLE_CREDENTIALS')
# if GOOGLE_CREDS:
#     with open('google.json', 'w') as f:
#         f.write(GOOGLE_CREDS)

# # Google Sheets configuration
# SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
# PASSED_SHEET_ID = '1rdSRNLdZPT5xXLRgV7wSn1beYwWZp41ZpYoLkbGmt0o'  # PASSED and FAILED tabs (CORRECT)
# PIKIPIKI_SHEET_ID = '1XFwPITQgZmzZ8lbg8MKD9S4rwHyk2cDOKrcxO7SAjHA'  # pikipiki records (includes all customer data)

# def get_google_service():
#     """Create Google Sheets service using Service Account"""
#     try:
#         credentials = service_account.Credentials.from_service_account_file(
#             'google.json',
#             scopes=SCOPES
#         )
#         service = build('sheets', 'v4', credentials=credentials)
#         return service
#     except Exception as e:
#         print(f"Error creating service: {e}")
#         raise

# def extract_phone_number(text):
#     """Extract phone number from text in formats: 255XXXXXXXXX, 07XXXXXXXX, 06XXXXXXXX"""
#     if not text or pd.isna(text):
#         return None
    
#     text = str(text).replace(' ', '').replace('-', '')
    
#     # Pattern for 255 followed by 9 digits
#     pattern_255 = r'255(\d{9})'
#     match = re.search(pattern_255, text)
#     if match:
#         return f"255{match.group(1)}"
    
#     # Pattern for 07 or 06 followed by 8 digits
#     pattern_07_06 = r'0([67])(\d{8})(?!\d)'
#     match = re.search(pattern_07_06, text)
#     if match:
#         return f"0{match.group(1)}{match.group(2)}"
    
#     return None

# def extract_plate_number(text):
#     """Extract plate number in format: MC###XXX (MC followed by 3 numbers then 3 letters)"""
#     if not text or pd.isna(text):
#         return None
    
#     text = str(text).replace(' ', '').upper()
    
#     # Pattern for MC followed by 3 digits then 3 letters
#     pattern = r'MC(\d{3})([A-Z]{3})'
#     match = re.search(pattern, text)
#     if match:
#         return f"MC{match.group(1)}{match.group(2)}"
    
#     return None

# def extract_ref_number(text):
#     """Extract reference number from message (format: REF:XXXXX)"""
#     if not text or pd.isna(text):
#         return None
    
#     text = str(text)
#     pattern = r'REF:\s*(\S+)'
#     match = re.search(pattern, text, re.IGNORECASE)
#     if match:
#         return match.group(1)
    
#     return None

# def normalize_phone_for_comparison(phone):
#     """
#     Normalize phone number for comparison
#     - Removes 255 prefix if present
#     - Removes 0 prefix if present
#     - Returns just the 9-digit number (e.g., 752900450)
#     """
#     if not phone:
#         return None
    
#     phone = str(phone).replace(' ', '').replace('-', '')
    
#     # Remove 255 prefix
#     if phone.startswith('255'):
#         phone = phone[3:]
    
#     # Remove 0 prefix
#     if phone.startswith('0'):
#         phone = phone[1:]
    
#     # Should now have 9 digits
#     if len(phone) == 9 and phone.isdigit():
#         return phone
    
#     return None

# # Records validation functions removed - all data now in pikipiki records only

# def load_all_customers(service):
#     """Load all customers from pikipiki records sheet into memory (to avoid API quota issues)"""
#     try:
#         sheet = service.spreadsheets()
#         result = sheet.values().get(
#             spreadsheetId=PIKIPIKI_SHEET_ID,
#             range='pikipiki records!A:E'
#         ).execute()
        
#         values = result.get('values', [])
#         if not values:
#             return {}, {}
        
#         # Build lookup dictionaries
#         phone_lookup = {}  # phone -> customer name
#         plate_lookup = {}  # plate -> customer name
        
#         # Skip header row
#         for row in values[1:]:
#             # Don't skip rows - just check if we have enough data
#             # Columns: ID, Plate Number, Customer Name, Phone Number, (optional 5th column)
#             plate_col = row[1] if len(row) > 1 else ''
#             phone_col = row[3] if len(row) > 3 else ''
#             name_col = row[2] if len(row) > 2 else ''
            
#             # Skip only if we have neither plate nor phone
#             if not plate_col and not phone_col:
#                 continue
            
#             # Clean and store plate
#             if plate_col:
#                 plate_clean = str(plate_col).replace(' ', '').upper()
#                 if plate_clean:
#                     plate_lookup[plate_clean] = name_col
#                     # Debug: Show first 5 plates loaded
#                     if len(plate_lookup) <= 5:
#                         print(f"  📍 Loaded plate: {plate_clean} → {name_col}")
            
#             # Clean and store phone
#             if phone_col:
#                 phone_clean = str(phone_col).replace(' ', '').replace('-', '')
#                 if phone_clean:
#                     phone_lookup[phone_clean] = name_col
        
#         print(f"Loaded {len(phone_lookup)} phone numbers and {len(plate_lookup)} plates from pikipiki records")
        
#         # 🔍 DEBUG: Check if specific plates exist
#         test_plates = ['MC697FLT', 'MC760FLT', 'MC572FLW', 'MC697FML']
#         print(f"🔍 Checking test plates in lookup:")
#         for test_plate in test_plates:
#             if test_plate in plate_lookup:
#                 print(f"  ✅ {test_plate} FOUND → {plate_lookup[test_plate]}")
#             else:
#                 print(f"  ❌ {test_plate} NOT FOUND")
        
#         return phone_lookup, plate_lookup
        
#     except Exception as e:
#         print(f"Error loading customers: {e}")
#         return {}, {}

# def load_all_customers_sav(service):
#     """🔥 NEW: Load all customers from pikipiki records2 sheet (for PASSED_SAV)"""
#     try:
#         sheet = service.spreadsheets()
#         result = sheet.values().get(
#             spreadsheetId=PIKIPIKI_SHEET_ID,
#             range='pikipiki records2!A:E'  # Same structure as pikipiki records
#         ).execute()
        
#         values = result.get('values', [])
#         if not values:
#             print("⚠️ No data found in pikipiki records2")
#             return {}, {}
        
#         # Build lookup dictionaries
#         phone_lookup_sav = {}  # phone -> customer name
#         plate_lookup_sav = {}  # plate -> customer name
        
#         # Skip header row
#         for row in values[1:]:
#             # Columns: ID, Plate Number, Customer Name, Phone Number, (optional 5th column)
#             plate_col = row[1] if len(row) > 1 else ''
#             phone_col = row[3] if len(row) > 3 else ''
#             name_col = row[2] if len(row) > 2 else ''
            
#             # Skip only if we have neither plate nor phone
#             if not plate_col and not phone_col:
#                 continue
            
#             # Clean and store plate
#             if plate_col:
#                 plate_clean = str(plate_col).replace(' ', '').upper()
#                 if plate_clean:
#                     plate_lookup_sav[plate_clean] = name_col
#                     # Debug: Show first 5 plates loaded
#                     if len(plate_lookup_sav) <= 5:
#                         print(f"  📍 SAV Loaded plate: {plate_clean} → {name_col}")
            
#             # Clean and store phone
#             if phone_col:
#                 phone_clean = str(phone_col).replace(' ', '').replace('-', '')
#                 if phone_clean:
#                     phone_lookup_sav[phone_clean] = name_col
        
#         print(f"✅ Loaded {len(phone_lookup_sav)} phone numbers and {len(plate_lookup_sav)} plates from pikipiki records2 (SAV)")
#         return phone_lookup_sav, plate_lookup_sav
        
#     except Exception as e:
#         print(f"⚠️ Error loading pikipiki records2 (SAV): {e}")
#         return {}, {}

# def lookup_customer_from_cache(identifier, lookup_type, phone_lookup, plate_lookup):
#     """Look up customer from cached data"""
#     if lookup_type == 'phone':
#         # Try exact match first
#         name = phone_lookup.get(identifier)
#         if name:
#             return name
        
#         # If identifier starts with 255, also try 07 format
#         if identifier.startswith('255'):
#             alt_format = '0' + identifier[3:]  # 255752900450 -> 0752900450
#             name = phone_lookup.get(alt_format)
#             if name:
#                 return name
        
#         # If identifier starts with 07, also try 255 format
#         elif identifier.startswith('07'):
#             alt_format = '255' + identifier[1:]  # 0752900450 -> 255752900450
#             name = phone_lookup.get(alt_format)
#             if name:
#                 return name
        
#         # If identifier starts with 06, also try 255 format
#         elif identifier.startswith('06'):
#             alt_format = '255' + identifier[1:]  # 0652900450 -> 255652900450
#             name = phone_lookup.get(alt_format)
#             if name:
#                 return name
        
#         return None
        
#     elif lookup_type == 'plate':
#         return plate_lookup.get(identifier)
#     return None

# def get_existing_refs(service, sheet_name='PASSED'):
#     """
#     🔥 IMPROVED: Get existing reference numbers AND check message details for duplicates
#     Returns both a set of REFs and a set of message details for comprehensive duplicate detection
#     """
#     try:
#         sheet = service.spreadsheets()
        
#         # 🔥 FIXED: Read REFNUMBER from correct column for each sheet
#         if sheet_name == 'FAILED':
#             # FAILED: D=MESSAGE, I=REFNUMBER (column I!)
#             ref_column = 'I'
#         else:  # PASSED
#             # PASSED: D=MESSAGE, H=REFNUMBER  
#             ref_column = 'H'
        
#         print(f"📖 Reading {sheet_name}: MESSAGE from column D, REFNUMBER from column {ref_column}")
        
#         result = service.spreadsheets().values().batchGet(
#             spreadsheetId=PASSED_SHEET_ID,
#             ranges=[f'{sheet_name}!D1:D10000', f'{sheet_name}!{ref_column}1:{ref_column}10000']
#         ).execute()
        
#         value_ranges = result.get('valueRanges', [])
        
#         refs = set()
#         messages = set()
        
#         # Process REFNUMBER column
#         if len(value_ranges) > 1:
#             ref_values = value_ranges[1].get('values', [])
#             print(f"📊 {sheet_name} - Column {ref_column} (REFNUMBER): {len(ref_values)} rows")
            
#             for idx, row in enumerate(ref_values[1:], start=2):  # Skip header
#                 if row and len(row) > 0 and row[0]:
#                     ref = str(row[0]).strip()
#                     if ref and ref.lower() != 'refnumber':
#                         refs.add(ref)
#                         if len(refs) <= 3:  # Show first 3 for debugging
#                             print(f"  ✓ Row {idx} REF: '{ref}'")
        
#         # Process MESSAGE column - extract REF from message as backup
#         if len(value_ranges) > 0:
#             message_values = value_ranges[0].get('values', [])
#             print(f"📊 {sheet_name} - Column D (MESSAGE): {len(message_values)} rows")
            
#             for idx, row in enumerate(message_values[1:], start=2):  # Skip header
#                 if row and len(row) > 0 and row[0]:
#                     message = str(row[0]).strip()
#                     messages.add(message)
                    
#                     # Also extract REF from message as backup
#                     import re
#                     pattern = r'REF:\s*(\S+)'
#                     match = re.search(pattern, message, re.IGNORECASE)
#                     if match:
#                         ref_from_msg = match.group(1)
#                         if ref_from_msg not in refs:
#                             refs.add(ref_from_msg)
#                             if len(refs) <= 5:
#                                 print(f"  ✓ Row {idx} REF from MSG: '{ref_from_msg}'")
        
#         print(f"✅ {sheet_name}: Found {len(refs)} unique REFs, {len(messages)} unique messages")
#         if refs:
#             print(f"   Sample REFs: {list(refs)[:3]}")
        
#         return refs, messages
        
#     except Exception as e:
#         print(f"❌ Error getting existing data from {sheet_name}: {e}")
#         import traceback
#         traceback.print_exc()
#         return set(), set()

# def get_last_id(service, sheet_name):
#     """Get the last ID from the sheet (ignores filters, reads all data)"""
#     try:
#         sheet = service.spreadsheets()
#         # Read ALL values from column A (ID column)
#         result = sheet.values().get(
#             spreadsheetId=PASSED_SHEET_ID,
#             range=f'{sheet_name}!A:A'
#         ).execute()
        
#         values = result.get('values', [])
        
#         if len(values) > 1:
#             # Get the last non-empty ID, starting from the end
#             for row in reversed(values[1:]):  # Skip header
#                 if row and len(row) > 0 and row[0]:
#                     try:
#                         last_id = int(row[0])
#                         print(f"Last ID in {sheet_name}: {last_id}")
#                         return last_id
#                     except (ValueError, TypeError):
#                         continue
        
#         print(f"No existing IDs found in {sheet_name}, starting from 0")
#         return 0
        
#     except Exception as e:
#         print(f"Error getting last ID: {e}")
#         return 0

# def get_last_row_number(service, sheet_name):
#     """Get the actual last row number (works even with filters)"""
#     try:
#         # Get all data from column A to find the true last row
#         result = service.spreadsheets().values().get(
#             spreadsheetId=PASSED_SHEET_ID,
#             range=f'{sheet_name}!A:A'
#         ).execute()
        
#         values = result.get('values', [])
#         # Return the number of rows (including header)
#         return len(values)
#     except Exception as e:
#         print(f"Error getting last row: {e}")
#         return 0

# def append_to_sheet(service, sheet_name, data):
#     """Append data to Google Sheet - WORKS WITH FILTERS"""
#     try:
#         # Get the actual last row number (ignores filters)
#         last_row = get_last_row_number(service, sheet_name)
        
#         # Calculate the starting row for new data
#         start_row = last_row + 1
        
#         # Build the range for new data
#         range_name = f'{sheet_name}!A{start_row}'
        
#         print(f"Attempting to append to {sheet_name} starting at row {start_row}")
#         print(f"Adding {len(data)} rows")
#         print(f"Data preview: {data[0] if data else 'No data'}")
        
#         # Use UPDATE instead of APPEND (works with filters!)
#         result = service.spreadsheets().values().update(
#             spreadsheetId=PASSED_SHEET_ID,
#             range=range_name,
#             valueInputOption='USER_ENTERED',
#             body={'values': data}
#         ).execute()
        
#         print(f"Update result: {result.get('updatedRows', 0)} rows added")
#         return True
        
#     except HttpError as e:
#         print(f"❌ Google Sheets API Error: {e}")
#         print(f"Error details: {e.error_details if hasattr(e, 'error_details') else 'No details'}")
#         if e.resp.status == 403:
#             print("Permission denied! Make sure the service account has Editor access to the sheet.")
#         return False
#     except Exception as e:
#         print(f"❌ Error appending to sheet: {e}")
#         import traceback
#         traceback.print_exc()
#         return False

# @app.route('/')
# def index():
#     return render_template('index.html')

# @app.route('/upload', methods=['POST'])
# def upload_file():
#     if 'file' not in request.files:
#         return jsonify({'error': 'No file uploaded'}), 400
    
#     file = request.files['file']
#     if file.filename == '':
#         return jsonify({'error': 'No file selected'}), 400
    
#     if not file.filename.endswith('.xlsx'):
#         return jsonify({'error': 'Please upload an Excel file (.xlsx)'}), 400
    
#     # Save file
#     filename = secure_filename(file.filename)
#     filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
#     file.save(filepath)
    
#     # Store filepath in session
#     session['filepath'] = filepath
    
#     return jsonify({'success': True, 'message': 'File uploaded successfully'})

# @app.route('/process', methods=['POST'])
# def process_transactions():
#     try:
#         # Get filepath from session
#         filepath = session.get('filepath')
#         if not filepath or not os.path.exists(filepath):
#             return jsonify({'error': 'No file uploaded'}), 400
        
#         # Read Excel file - CRDB format has headers at row 12
#         df = pd.read_excel(filepath, header=12)
        
#         # First row contains the actual column names
#         df.columns = df.iloc[0]
#         df = df[1:].reset_index(drop=True)
        
#         print(f"Columns found: {list(df.columns)}")
        
#         # Now we should have: Posting Date, Details, Value Date, Debit, Credit, Book Balance
#         required_columns = ['Posting Date', 'Details', 'Credit']
#         missing = [col for col in required_columns if col not in df.columns]
        
#         if missing:
#             return jsonify({
#                 'error': f'Missing required columns: {missing}. Found: {list(df.columns)}'
#             }), 400
        
#         # Filter only CREDIT transactions (money coming IN)
#         # Convert Credit column to numeric, handle commas
#         df['Credit'] = pd.to_numeric(df['Credit'].astype(str).str.replace(',', ''), errors='coerce')
#         df['Debit'] = pd.to_numeric(df['Debit'].astype(str).str.replace(',', ''), errors='coerce')
        
#         # Only credit transactions (Credit > 0 and Debit is 0 or NaN)
#         credit_df = df[(df['Credit'].notna()) & (df['Credit'] > 0) & 
#                        ((df['Debit'].isna()) | (df['Debit'] == 0))].copy()
        
#         print(f"Found {len(credit_df)} credit transactions")
        
#         # Initialize Google Sheets service
#         service = get_google_service()
        
#         # Load ALL customers from pikipiki records ONCE (to avoid API quota limits)
#         print("Loading customer database from pikipiki records...")
#         phone_lookup, plate_lookup = load_all_customers(service)
        
#         # 🔥 NEW: Load customers from pikipiki records2 (for PASSED_SAV)
#         print("\nLoading customer database from pikipiki records2 (SAV)...")
#         phone_lookup_sav, plate_lookup_sav = load_all_customers_sav(service)
        
#         # 🔥 IMPROVED: Get existing reference numbers AND messages from PASSED, PASSED_SAV, and FAILED sheets
#         print("Loading existing references from PASSED sheet...")
#         existing_passed_refs, existing_passed_messages = get_existing_refs(service, 'PASSED')
        
#         print("Loading existing references from PASSED_SAV sheet...")
#         existing_passed_sav_refs, existing_passed_sav_messages = get_existing_refs(service, 'PASSED_SAV')
        
#         print("Loading existing references from FAILED sheet...")
#         existing_failed_refs, existing_failed_messages = get_existing_refs(service, 'FAILED')
        
#         # 🔥 COMBINE all sets to check against all existing refs AND messages
#         all_existing_refs = existing_passed_refs.union(existing_passed_sav_refs).union(existing_failed_refs)
#         all_existing_messages = existing_passed_messages.union(existing_passed_sav_messages).union(existing_failed_messages)
#         print(f"Total unique refs in system: {len(all_existing_refs)} (PASSED: {len(existing_passed_refs)}, PASSED_SAV: {len(existing_passed_sav_refs)}, FAILED: {len(existing_failed_refs)})")
#         print(f"Total unique messages in system: {len(all_existing_messages)}")
        
#         # Get last IDs
#         last_passed_id = get_last_id(service, 'PASSED')
#         last_passed_sav_id = get_last_id(service, 'PASSED_SAV')
#         last_failed_id = get_last_id(service, 'FAILED')
        
#         passed_data = []
#         passed_sav_data = []  # 🔥 NEW: For pikipiki records2 matches
#         failed_data = []
        
#         stats = {
#             'total': len(credit_df),
#             'passed': 0,
#             'passed_sav': 0,  # 🔥 NEW: Track SAV matches
#             'failed': 0,
#             'skipped': 0,
#             'skipped_from_passed': 0,
#             'skipped_from_passed_sav': 0,  # 🔥 NEW
#             'skipped_from_failed': 0
#         }
        
#         for idx, row in credit_df.iterrows():
#             posting_date = str(row.get('Posting Date', ''))
#             details = str(row.get('Details', ''))
#             credit_amount = row.get('Credit', 0)
            
#             # Extract reference number
#             ref_number = extract_ref_number(details)
            
#             # 🔥 IMPROVED: Check for duplicates using BOTH ref number AND full message
#             is_duplicate = False
#             duplicate_reason = ""
            
#             # Check 1: REF number match
#             if ref_number and ref_number in all_existing_refs:
#                 is_duplicate = True
#                 duplicate_reason = "REF"
#                 if ref_number in existing_passed_refs:
#                     stats['skipped_from_passed'] += 1
#                     print(f"⏭️ DUPLICATE (REF in PASSED): {ref_number}")
#                 elif ref_number in existing_passed_sav_refs:
#                     stats['skipped_from_passed_sav'] += 1
#                     print(f"⏭️ DUPLICATE (REF in PASSED_SAV): {ref_number}")
#                 else:
#                     stats['skipped_from_failed'] += 1
#                     print(f"⏭️ DUPLICATE (REF in FAILED): {ref_number}")
            
#             # Check 2: Exact message match (backup check)
#             elif details in all_existing_messages:
#                 is_duplicate = True
#                 duplicate_reason = "MESSAGE"
#                 stats['skipped'] += 1
#                 print(f"⏭️ DUPLICATE (MESSAGE): {details[:80]}...")
            
#             if is_duplicate:
#                 stats['skipped'] += 1
#                 continue
            
#             # Try to extract phone number or plate number from details
#             phone = extract_phone_number(details)
#             plate = extract_plate_number(details)
            
#             identifier = None
#             lookup_type = None
            
#             if phone:
#                 identifier = phone
#                 lookup_type = 'phone'
#                 print(f"Found phone: {phone} in: {details[:50]}")
#             elif plate:
#                 identifier = plate
#                 lookup_type = 'plate'
#                 print(f"Found plate: {plate} in: {details[:50]}")
            
#             if identifier and lookup_type:
#                 # 🔥 PRIORITY 1: Check pikipiki records first
#                 customer_name = lookup_customer_from_cache(identifier, lookup_type, phone_lookup, plate_lookup)
                
#                 if customer_name:
#                     # Successfully found customer in pikipiki records - add to PASSED
#                     last_passed_id += 1
#                     passed_row = [
#                         last_passed_id,        # ID
#                         posting_date,          # Date
#                         'CRDB',                # Channel
#                         details,               # Message (full details)
#                         credit_amount,         # Amount
#                         identifier,            # Plate/Phone
#                         customer_name,         # Name
#                         ref_number or ''       # Ref Number
#                     ]
#                     passed_data.append(passed_row)
#                     stats['passed'] += 1
#                     print(f"✅ PASSED: {customer_name} - {identifier} - {credit_amount}")
#                 else:
#                     # 🔥 PRIORITY 2: Check pikipiki records2 (SAV)
#                     customer_name_sav = lookup_customer_from_cache(identifier, lookup_type, phone_lookup_sav, plate_lookup_sav)
                    
#                     if customer_name_sav:
#                         # Successfully found customer in pikipiki records2 - add to PASSED_SAV
#                         last_passed_sav_id += 1
#                         passed_sav_row = [
#                             last_passed_sav_id,    # ID
#                             posting_date,          # Date
#                             'CRDB',                # Channel
#                             details,               # Message (full details)
#                             credit_amount,         # Amount
#                             identifier,            # Plate/Phone
#                             customer_name_sav,     # Name
#                             ref_number or ''       # Ref Number
#                         ]
#                         passed_sav_data.append(passed_sav_row)
#                         stats['passed_sav'] += 1
#                         print(f"✅ PASSED_SAV: {customer_name_sav} - {identifier} - {credit_amount}")
#                     else:
#                         # 🔥 PRIORITY 3: Customer not found in either sheet - add to FAILED
#                         last_failed_id += 1
#                         reason = f"{lookup_type.upper()}({identifier}) not found"
                        
#                         # Ensure phone has 255 prefix before sending to FAILED sheet
#                         final_identifier = identifier
#                         if lookup_type == 'phone':
#                             if not identifier.startswith('255'):
#                                 # Add 255 prefix
#                                 if identifier.startswith('0'):
#                                     final_identifier = '255' + identifier[1:]
#                                 else:
#                                     final_identifier = '255' + identifier
                        
#                         failed_row = [
#                             last_failed_id,    # ID
#                             posting_date,      # Date
#                             'CRDB',            # Channel
#                             details,           # Message
#                             credit_amount,     # Amount
#                             final_identifier,  # Plate/Phone (with 255 prefix for phones)
#                             reason,            # Reason
#                             ref_number or ''   # REF
#                         ]
#                         failed_data.append(failed_row)
#                         stats['failed'] += 1
#                         print(f"❌ FAILED: Customer not found for {final_identifier} (REF: {ref_number})")
#             else:
#                 # No phone or plate found in details - add to FAILED
#                 last_failed_id += 1
#                 failed_row = [
#                     last_failed_id,        # ID
#                     posting_date,          # Date
#                     'CRDB',                # Channel
#                     details,               # Message
#                     credit_amount,         # Amount
#                     'No phone/plate',      # Plate/Phone column
#                     'No identifier',       # Reason
#                     ref_number or ''       # REF
#                 ]
#                 failed_data.append(failed_row)
#                 stats['failed'] += 1
#                 print(f"❌ FAILED: No phone/plate found in: {details[:50]} (REF: {ref_number})")
        
#         # Append to Google Sheets
#         if passed_data:
#             print(f"Appending {len(passed_data)} rows to PASSED sheet")
#             try:
#                 result = append_to_sheet(service, 'PASSED', passed_data)
#                 if result:
#                     print(f"✅ Successfully added {len(passed_data)} rows to PASSED")
#                 else:
#                     print(f"❌ Failed to add rows to PASSED")
#                     return jsonify({'error': 'Failed to write to PASSED sheet. Check permissions.'}), 500
#             except Exception as e:
#                 print(f"❌ Error writing to PASSED sheet: {e}")
#                 return jsonify({'error': f'Error writing to PASSED sheet: {str(e)}'}), 500
        
#         # 🔥 NEW: Append to PASSED_SAV sheet
#         if passed_sav_data:
#             print(f"Appending {len(passed_sav_data)} rows to PASSED_SAV sheet")
#             try:
#                 result = append_to_sheet(service, 'PASSED_SAV', passed_sav_data)
#                 if result:
#                     print(f"✅ Successfully added {len(passed_sav_data)} rows to PASSED_SAV")
#                 else:
#                     print(f"❌ Failed to add rows to PASSED_SAV")
#                     return jsonify({'error': 'Failed to write to PASSED_SAV sheet. Check permissions.'}), 500
#             except Exception as e:
#                 print(f"❌ Error writing to PASSED_SAV sheet: {e}")
#                 return jsonify({'error': f'Error writing to PASSED_SAV sheet: {str(e)}'}), 500
        
#         if failed_data:
#             print(f"Appending {len(failed_data)} rows to FAILED sheet")
#             try:
#                 result = append_to_sheet(service, 'FAILED', failed_data)
#                 if result:
#                     print(f"✅ Successfully added {len(failed_data)} rows to FAILED")
#                 else:
#                     print(f"❌ Failed to add rows to FAILED")
#                     return jsonify({'error': 'Failed to write to FAILED sheet. Check permissions.'}), 500
#             except Exception as e:
#                 print(f"❌ Error writing to FAILED sheet: {e}")
#                 return jsonify({'error': f'Error writing to FAILED sheet: {str(e)}'}), 500
        
#         # Clean up uploaded file
#         if os.path.exists(filepath):
#             os.remove(filepath)
        
#         # Build detailed message
#         skip_message = f"{stats['skipped']} skipped (duplicates)"
#         if stats['skipped'] > 0:
#             skip_details = []
#             if stats['skipped_from_passed'] > 0:
#                 skip_details.append(f"{stats['skipped_from_passed']} in PASSED")
#             if stats['skipped_from_passed_sav'] > 0:
#                 skip_details.append(f"{stats['skipped_from_passed_sav']} in PASSED_SAV")
#             if stats['skipped_from_failed'] > 0:
#                 skip_details.append(f"{stats['skipped_from_failed']} in FAILED")
#             skip_message += f" - {', '.join(skip_details)}"
        
#         return jsonify({
#             'success': True,
#             'stats': stats,
#             'message': f"Processed {stats['total']} credit transactions: {stats['passed']} passed, {stats['passed_sav']} passed (SAV), {stats['failed']} failed, {skip_message}"
#         })
    
#     except Exception as e:
#         import traceback
#         traceback.print_exc()
#         return jsonify({'error': str(e)}), 500

# @app.route('/check-auth', methods=['GET'])
# def check_auth():
#     """Check if Google Service Account is configured"""
#     try:
#         service = get_google_service()
#         return jsonify({'authenticated': True, 'message': 'Service Account configured'})
#     except Exception as e:
#         return jsonify({'authenticated': False, 'error': str(e)}), 500

# if __name__ == '__main__':
#     app.run(debug=True, host='0.0.0.0', port=5000)










# from flask import Flask, render_template, request, jsonify, session
# from werkzeug.utils import secure_filename
# import os
# import re
# import pandas as pd
# from google.oauth2 import service_account
# from googleapiclient.discovery import build
# from googleapiclient.errors import HttpError
# import json
# from datetime import datetime

# app = Flask(__name__)
# app.secret_key = os.environ.get('SECRET_KEY', 'your-secret-key-change-this')
# app.config['UPLOAD_FOLDER'] = 'uploads'
# app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size

# # Ensure upload folder exists
# os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# # For Render deployment - read credentials from environment
# GOOGLE_CREDS = os.environ.get('GOOGLE_CREDENTIALS')
# if GOOGLE_CREDS:
#     with open('google.json', 'w') as f:
#         f.write(GOOGLE_CREDS)

# # Google Sheets configuration
# SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
# PASSED_SHEET_ID = '1rdSRNLdZPT5xXLRgV7wSn1beYwWZp41ZpYoLkbGmt0o'  # PASSED and FAILED tabs (CORRECT)
# PIKIPIKI_SHEET_ID = '1XFwPITQgZmzZ8lbg8MKD9S4rwHyk2cDOKrcxO7SAjHA'  # pikipiki records (includes all customer data)

# def get_google_service():
#     """Create Google Sheets service using Service Account"""
#     try:
#         credentials = service_account.Credentials.from_service_account_file(
#             'google.json',
#             scopes=SCOPES
#         )
#         service = build('sheets', 'v4', credentials=credentials)
#         return service
#     except Exception as e:
#         print(f"Error creating service: {e}")
#         raise

# def extract_phone_number(text):
#     """Extract phone number from text in formats: 255XXXXXXXXX, 07XXXXXXXX, 06XXXXXXXX"""
#     if not text or pd.isna(text):
#         return None
    
#     text = str(text).replace(' ', '').replace('-', '')
    
#     # Pattern for 255 followed by 9 digits
#     pattern_255 = r'255(\d{9})'
#     match = re.search(pattern_255, text)
#     if match:
#         return f"255{match.group(1)}"
    
#     # Pattern for 07 or 06 followed by 8 digits
#     pattern_07_06 = r'0([67])(\d{8})(?!\d)'
#     match = re.search(pattern_07_06, text)
#     if match:
#         return f"0{match.group(1)}{match.group(2)}"
    
#     return None

# def extract_plate_number(text):
#     """Extract plate number in format: MC###XXX (MC followed by 3 numbers then 3 letters)"""
#     if not text or pd.isna(text):
#         return None
    
#     text = str(text).replace(' ', '').upper()
    
#     # Pattern for MC followed by 3 digits then 3 letters
#     pattern = r'MC(\d{3})([A-Z]{3})'
#     match = re.search(pattern, text)
#     if match:
#         return f"MC{match.group(1)}{match.group(2)}"
    
#     return None

# def extract_ref_number(text):
#     """Extract reference number from message (format: REF:XXXXX)"""
#     if not text or pd.isna(text):
#         return None
    
#     text = str(text)
#     pattern = r'REF:\s*(\S+)'
#     match = re.search(pattern, text, re.IGNORECASE)
#     if match:
#         return match.group(1)
    
#     return None

# def normalize_phone_for_comparison(phone):
#     """
#     Normalize phone number for comparison
#     - Removes 255 prefix if present
#     - Removes 0 prefix if present
#     - Returns just the 9-digit number (e.g., 752900450)
#     """
#     if not phone:
#         return None
    
#     phone = str(phone).replace(' ', '').replace('-', '')
    
#     # Remove 255 prefix
#     if phone.startswith('255'):
#         phone = phone[3:]
    
#     # Remove 0 prefix
#     if phone.startswith('0'):
#         phone = phone[1:]
    
#     # Should now have 9 digits
#     if len(phone) == 9 and phone.isdigit():
#         return phone
    
#     return None

# # Records validation functions removed - all data now in pikipiki records only

# def load_all_customers(service):
#     """Load all customers from pikipiki records sheet into memory (to avoid API quota issues)"""
#     try:
#         sheet = service.spreadsheets()
#         result = sheet.values().get(
#             spreadsheetId=PIKIPIKI_SHEET_ID,
#             range='pikipiki records!A:E'
#         ).execute()
        
#         values = result.get('values', [])
#         if not values:
#             return {}, {}
        
#         # Build lookup dictionaries
#         phone_lookup = {}  # phone -> customer name
#         plate_lookup = {}  # plate -> customer name
        
#         # Skip header row
#         for row in values[1:]:
#             # Don't skip rows - just check if we have enough data
#             # Columns: ID, Plate Number, Customer Name, Phone Number, (optional 5th column)
#             plate_col = row[1] if len(row) > 1 else ''
#             phone_col = row[3] if len(row) > 3 else ''
#             name_col = row[2] if len(row) > 2 else ''
            
#             # Skip only if we have neither plate nor phone
#             if not plate_col and not phone_col:
#                 continue
            
#             # Clean and store plate
#             if plate_col:
#                 plate_clean = str(plate_col).replace(' ', '').upper()
#                 if plate_clean:
#                     plate_lookup[plate_clean] = name_col
#                     # Debug: Show first 5 plates loaded
#                     if len(plate_lookup) <= 5:
#                         print(f"  📍 Loaded plate: {plate_clean} → {name_col}")
            
#             # Clean and store phone
#             if phone_col:
#                 phone_clean = str(phone_col).replace(' ', '').replace('-', '')
#                 if phone_clean:
#                     phone_lookup[phone_clean] = name_col
        
#         print(f"Loaded {len(phone_lookup)} phone numbers and {len(plate_lookup)} plates from pikipiki records")
        
#         # 🔍 DEBUG: Check if specific plates exist
#         test_plates = ['MC697FLT', 'MC760FLT', 'MC572FLW', 'MC697FML']
#         print(f"🔍 Checking test plates in lookup:")
#         for test_plate in test_plates:
#             if test_plate in plate_lookup:
#                 print(f"  ✅ {test_plate} FOUND → {plate_lookup[test_plate]}")
#             else:
#                 print(f"  ❌ {test_plate} NOT FOUND")
        
#         return phone_lookup, plate_lookup
        
#     except Exception as e:
#         print(f"Error loading customers: {e}")
#         return {}, {}

# def lookup_customer_from_cache(identifier, lookup_type, phone_lookup, plate_lookup):
#     """Look up customer from cached data"""
#     if lookup_type == 'phone':
#         # Try exact match first
#         name = phone_lookup.get(identifier)
#         if name:
#             return name
        
#         # If identifier starts with 255, also try 07 format
#         if identifier.startswith('255'):
#             alt_format = '0' + identifier[3:]  # 255752900450 -> 0752900450
#             name = phone_lookup.get(alt_format)
#             if name:
#                 return name
        
#         # If identifier starts with 07, also try 255 format
#         elif identifier.startswith('07'):
#             alt_format = '255' + identifier[1:]  # 0752900450 -> 255752900450
#             name = phone_lookup.get(alt_format)
#             if name:
#                 return name
        
#         # If identifier starts with 06, also try 255 format
#         elif identifier.startswith('06'):
#             alt_format = '255' + identifier[1:]  # 0652900450 -> 255652900450
#             name = phone_lookup.get(alt_format)
#             if name:
#                 return name
        
#         return None
        
#     elif lookup_type == 'plate':
#         return plate_lookup.get(identifier)
#     return None

# def get_existing_refs(service, sheet_name='PASSED'):
#     """
#     🔥 IMPROVED: Get existing reference numbers AND check message details for duplicates
#     Returns both a set of REFs and a set of message details for comprehensive duplicate detection
#     """
#     try:
#         sheet = service.spreadsheets()
        
#         # 🔥 FIXED: Read REFNUMBER from correct column for each sheet
#         if sheet_name == 'FAILED':
#             # FAILED: D=MESSAGE, I=REFNUMBER (column I!)
#             ref_column = 'I'
#         else:  # PASSED
#             # PASSED: D=MESSAGE, H=REFNUMBER  
#             ref_column = 'H'
        
#         print(f"📖 Reading {sheet_name}: MESSAGE from column D, REFNUMBER from column {ref_column}")
        
#         result = service.spreadsheets().values().batchGet(
#             spreadsheetId=PASSED_SHEET_ID,
#             ranges=[f'{sheet_name}!D1:D10000', f'{sheet_name}!{ref_column}1:{ref_column}10000']
#         ).execute()
        
#         value_ranges = result.get('valueRanges', [])
        
#         refs = set()
#         messages = set()
        
#         # Process REFNUMBER column
#         if len(value_ranges) > 1:
#             ref_values = value_ranges[1].get('values', [])
#             print(f"📊 {sheet_name} - Column {ref_column} (REFNUMBER): {len(ref_values)} rows")
            
#             for idx, row in enumerate(ref_values[1:], start=2):  # Skip header
#                 if row and len(row) > 0 and row[0]:
#                     ref = str(row[0]).strip()
#                     if ref and ref.lower() != 'refnumber':
#                         refs.add(ref)
#                         if len(refs) <= 3:  # Show first 3 for debugging
#                             print(f"  ✓ Row {idx} REF: '{ref}'")
        
#         # Process MESSAGE column - extract REF from message as backup
#         if len(value_ranges) > 0:
#             message_values = value_ranges[0].get('values', [])
#             print(f"📊 {sheet_name} - Column D (MESSAGE): {len(message_values)} rows")
            
#             for idx, row in enumerate(message_values[1:], start=2):  # Skip header
#                 if row and len(row) > 0 and row[0]:
#                     message = str(row[0]).strip()
#                     messages.add(message)
                    
#                     # Also extract REF from message as backup
#                     import re
#                     pattern = r'REF:\s*(\S+)'
#                     match = re.search(pattern, message, re.IGNORECASE)
#                     if match:
#                         ref_from_msg = match.group(1)
#                         if ref_from_msg not in refs:
#                             refs.add(ref_from_msg)
#                             if len(refs) <= 5:
#                                 print(f"  ✓ Row {idx} REF from MSG: '{ref_from_msg}'")
        
#         print(f"✅ {sheet_name}: Found {len(refs)} unique REFs, {len(messages)} unique messages")
#         if refs:
#             print(f"   Sample REFs: {list(refs)[:3]}")
        
#         return refs, messages
        
#     except Exception as e:
#         print(f"❌ Error getting existing data from {sheet_name}: {e}")
#         import traceback
#         traceback.print_exc()
#         return set(), set()

# def get_last_id(service, sheet_name):
#     """Get the last ID from the sheet (ignores filters, reads all data)"""
#     try:
#         sheet = service.spreadsheets()
#         # Read ALL values from column A (ID column)
#         result = sheet.values().get(
#             spreadsheetId=PASSED_SHEET_ID,
#             range=f'{sheet_name}!A:A'
#         ).execute()
        
#         values = result.get('values', [])
        
#         if len(values) > 1:
#             # Get the last non-empty ID, starting from the end
#             for row in reversed(values[1:]):  # Skip header
#                 if row and len(row) > 0 and row[0]:
#                     try:
#                         last_id = int(row[0])
#                         print(f"Last ID in {sheet_name}: {last_id}")
#                         return last_id
#                     except (ValueError, TypeError):
#                         continue
        
#         print(f"No existing IDs found in {sheet_name}, starting from 0")
#         return 0
        
#     except Exception as e:
#         print(f"Error getting last ID: {e}")
#         return 0

# def get_last_row_number(service, sheet_name):
#     """Get the actual last row number (works even with filters)"""
#     try:
#         # Get all data from column A to find the true last row
#         result = service.spreadsheets().values().get(
#             spreadsheetId=PASSED_SHEET_ID,
#             range=f'{sheet_name}!A:A'
#         ).execute()
        
#         values = result.get('values', [])
#         # Return the number of rows (including header)
#         return len(values)
#     except Exception as e:
#         print(f"Error getting last row: {e}")
#         return 0

# def append_to_sheet(service, sheet_name, data):
#     """Append data to Google Sheet - WORKS WITH FILTERS"""
#     try:
#         # Get the actual last row number (ignores filters)
#         last_row = get_last_row_number(service, sheet_name)
        
#         # Calculate the starting row for new data
#         start_row = last_row + 1
        
#         # Build the range for new data
#         range_name = f'{sheet_name}!A{start_row}'
        
#         print(f"Attempting to append to {sheet_name} starting at row {start_row}")
#         print(f"Adding {len(data)} rows")
#         print(f"Data preview: {data[0] if data else 'No data'}")
        
#         # Use UPDATE instead of APPEND (works with filters!)
#         result = service.spreadsheets().values().update(
#             spreadsheetId=PASSED_SHEET_ID,
#             range=range_name,
#             valueInputOption='USER_ENTERED',
#             body={'values': data}
#         ).execute()
        
#         print(f"Update result: {result.get('updatedRows', 0)} rows added")
#         return True
        
#     except HttpError as e:
#         print(f"❌ Google Sheets API Error: {e}")
#         print(f"Error details: {e.error_details if hasattr(e, 'error_details') else 'No details'}")
#         if e.resp.status == 403:
#             print("Permission denied! Make sure the service account has Editor access to the sheet.")
#         return False
#     except Exception as e:
#         print(f"❌ Error appending to sheet: {e}")
#         import traceback
#         traceback.print_exc()
#         return False

# @app.route('/')
# def index():
#     return render_template('index.html')

# @app.route('/upload', methods=['POST'])
# def upload_file():
#     if 'file' not in request.files:
#         return jsonify({'error': 'No file uploaded'}), 400
    
#     file = request.files['file']
#     if file.filename == '':
#         return jsonify({'error': 'No file selected'}), 400
    
#     if not file.filename.endswith('.xlsx'):
#         return jsonify({'error': 'Please upload an Excel file (.xlsx)'}), 400
    
#     # Save file
#     filename = secure_filename(file.filename)
#     filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
#     file.save(filepath)
    
#     # Store filepath in session
#     session['filepath'] = filepath
    
#     return jsonify({'success': True, 'message': 'File uploaded successfully'})

# @app.route('/process', methods=['POST'])
# def process_transactions():
#     try:
#         # Get filepath from session
#         filepath = session.get('filepath')
#         if not filepath or not os.path.exists(filepath):
#             return jsonify({'error': 'No file uploaded'}), 400
        
#         # Read Excel file - CRDB format has headers at row 12
#         df = pd.read_excel(filepath, header=12)
        
#         # First row contains the actual column names
#         df.columns = df.iloc[0]
#         df = df[1:].reset_index(drop=True)
        
#         print(f"Columns found: {list(df.columns)}")
        
#         # Now we should have: Posting Date, Details, Value Date, Debit, Credit, Book Balance
#         required_columns = ['Posting Date', 'Details', 'Credit']
#         missing = [col for col in required_columns if col not in df.columns]
        
#         if missing:
#             return jsonify({
#                 'error': f'Missing required columns: {missing}. Found: {list(df.columns)}'
#             }), 400
        
#         # Filter only CREDIT transactions (money coming IN)
#         # Convert Credit column to numeric, handle commas
#         df['Credit'] = pd.to_numeric(df['Credit'].astype(str).str.replace(',', ''), errors='coerce')
#         df['Debit'] = pd.to_numeric(df['Debit'].astype(str).str.replace(',', ''), errors='coerce')
        
#         # Only credit transactions (Credit > 0 and Debit is 0 or NaN)
#         credit_df = df[(df['Credit'].notna()) & (df['Credit'] > 0) & 
#                        ((df['Debit'].isna()) | (df['Debit'] == 0))].copy()
        
#         print(f"Found {len(credit_df)} credit transactions")
        
#         # Initialize Google Sheets service
#         service = get_google_service()
        
#         # Load ALL customers from pikipiki records ONCE (to avoid API quota limits)
#         print("Loading customer database from pikipiki records...")
#         phone_lookup, plate_lookup = load_all_customers(service)
        
#         # 🔥 IMPROVED: Get existing reference numbers AND messages from BOTH PASSED and FAILED sheets
#         print("Loading existing references from PASSED sheet...")
#         existing_passed_refs, existing_passed_messages = get_existing_refs(service, 'PASSED')
        
#         print("Loading existing references from FAILED sheet...")
#         existing_failed_refs, existing_failed_messages = get_existing_refs(service, 'FAILED')
        
#         # 🔥 COMBINE both sets to check against all existing refs AND messages
#         all_existing_refs = existing_passed_refs.union(existing_failed_refs)
#         all_existing_messages = existing_passed_messages.union(existing_failed_messages)
#         print(f"Total unique refs in system: {len(all_existing_refs)} (PASSED: {len(existing_passed_refs)}, FAILED: {len(existing_failed_refs)})")
#         print(f"Total unique messages in system: {len(all_existing_messages)}")
        
#         # Get last IDs
#         last_passed_id = get_last_id(service, 'PASSED')
#         last_failed_id = get_last_id(service, 'FAILED')
        
#         passed_data = []
#         failed_data = []
        
#         stats = {
#             'total': len(credit_df),
#             'passed': 0,
#             'failed': 0,
#             'skipped': 0,
#             'skipped_from_passed': 0,
#             'skipped_from_failed': 0
#         }
        
#         for idx, row in credit_df.iterrows():
#             posting_date = str(row.get('Posting Date', ''))
#             details = str(row.get('Details', ''))
#             credit_amount = row.get('Credit', 0)
            
#             # Extract reference number
#             ref_number = extract_ref_number(details)
            
#             # 🔥 IMPROVED: Check for duplicates using BOTH ref number AND full message
#             is_duplicate = False
#             duplicate_reason = ""
            
#             # Check 1: REF number match
#             if ref_number and ref_number in all_existing_refs:
#                 is_duplicate = True
#                 duplicate_reason = "REF"
#                 if ref_number in existing_passed_refs:
#                     stats['skipped_from_passed'] += 1
#                     print(f"⏭️ DUPLICATE (REF in PASSED): {ref_number}")
#                 else:
#                     stats['skipped_from_failed'] += 1
#                     print(f"⏭️ DUPLICATE (REF in FAILED): {ref_number}")
            
#             # Check 2: Exact message match (backup check)
#             elif details in all_existing_messages:
#                 is_duplicate = True
#                 duplicate_reason = "MESSAGE"
#                 stats['skipped'] += 1
#                 print(f"⏭️ DUPLICATE (MESSAGE): {details[:80]}...")
            
#             if is_duplicate:
#                 stats['skipped'] += 1
#                 continue
            
#             # Try to extract phone number or plate number from details
#             phone = extract_phone_number(details)
#             plate = extract_plate_number(details)
            
#             identifier = None
#             lookup_type = None
            
#             if phone:
#                 identifier = phone
#                 lookup_type = 'phone'
#                 print(f"Found phone: {phone} in: {details[:50]}")
#             elif plate:
#                 identifier = plate
#                 lookup_type = 'plate'
#                 print(f"Found plate: {plate} in: {details[:50]}")
            
#             if identifier and lookup_type:
#                 # Lookup customer name from cached pikipiki records
#                 customer_name = lookup_customer_from_cache(identifier, lookup_type, phone_lookup, plate_lookup)
                
#                 if customer_name:
#                     # Successfully found customer - add to PASSED
#                     last_passed_id += 1
#                     passed_row = [
#                         last_passed_id,        # ID
#                         posting_date,          # Date
#                         'CRDB',                # Channel
#                         details,               # Message (full details)
#                         credit_amount,         # Amount
#                         identifier,            # Plate/Phone
#                         customer_name,         # Name
#                         ref_number or ''       # Ref Number
#                     ]
#                     passed_data.append(passed_row)
#                     stats['passed'] += 1
#                     print(f"✅ PASSED: {customer_name} - {identifier} - {credit_amount}")
#                 else:
#                     # Customer not found in pikipiki records - add to FAILED
#                     last_failed_id += 1
#                     reason = f"{lookup_type.upper()}({identifier}) not found"
                    
#                     # Ensure phone has 255 prefix before sending to FAILED sheet
#                     final_identifier = identifier
#                     if lookup_type == 'phone':
#                         if not identifier.startswith('255'):
#                             # Add 255 prefix
#                             if identifier.startswith('0'):
#                                 final_identifier = '255' + identifier[1:]
#                             else:
#                                 final_identifier = '255' + identifier
                    
#                     failed_row = [
#                         last_failed_id,    # ID
#                         posting_date,      # Date
#                         'CRDB',            # Channel
#                         details,           # Message
#                         credit_amount,     # Amount
#                         final_identifier,  # Plate/Phone (with 255 prefix for phones)
#                         reason,            # Reason
#                         ref_number or ''   # REF
#                     ]
#                     failed_data.append(failed_row)
#                     stats['failed'] += 1
#                     print(f"❌ FAILED: Customer not found for {final_identifier} (REF: {ref_number})")
#             else:
#                 # No phone or plate found in details - add to FAILED
#                 last_failed_id += 1
#                 failed_row = [
#                     last_failed_id,        # ID
#                     posting_date,          # Date
#                     'CRDB',                # Channel
#                     details,               # Message
#                     credit_amount,         # Amount
#                     'No phone/plate',      # Plate/Phone column
#                     'No identifier',       # Reason
#                     ref_number or ''       # REF
#                 ]
#                 failed_data.append(failed_row)
#                 stats['failed'] += 1
#                 print(f"❌ FAILED: No phone/plate found in: {details[:50]} (REF: {ref_number})")
        
#         # Append to Google Sheets
#         if passed_data:
#             print(f"Appending {len(passed_data)} rows to PASSED sheet")
#             try:
#                 result = append_to_sheet(service, 'PASSED', passed_data)
#                 if result:
#                     print(f"✅ Successfully added {len(passed_data)} rows to PASSED")
#                 else:
#                     print(f"❌ Failed to add rows to PASSED")
#                     return jsonify({'error': 'Failed to write to PASSED sheet. Check permissions.'}), 500
#             except Exception as e:
#                 print(f"❌ Error writing to PASSED sheet: {e}")
#                 return jsonify({'error': f'Error writing to PASSED sheet: {str(e)}'}), 500
        
#         if failed_data:
#             print(f"Appending {len(failed_data)} rows to FAILED sheet")
#             try:
#                 result = append_to_sheet(service, 'FAILED', failed_data)
#                 if result:
#                     print(f"✅ Successfully added {len(failed_data)} rows to FAILED")
#                 else:
#                     print(f"❌ Failed to add rows to FAILED")
#                     return jsonify({'error': 'Failed to write to FAILED sheet. Check permissions.'}), 500
#             except Exception as e:
#                 print(f"❌ Error writing to FAILED sheet: {e}")
#                 return jsonify({'error': f'Error writing to FAILED sheet: {str(e)}'}), 500
        
#         # Clean up uploaded file
#         if os.path.exists(filepath):
#             os.remove(filepath)
        
#         # Build detailed message
#         skip_message = f"{stats['skipped']} skipped (duplicates)"
#         if stats['skipped'] > 0:
#             skip_message += f" - {stats['skipped_from_passed']} already in PASSED, {stats['skipped_from_failed']} already in FAILED"
        
#         return jsonify({
#             'success': True,
#             'stats': stats,
#             'message': f"Processed {stats['total']} credit transactions: {stats['passed']} passed, {stats['failed']} failed, {skip_message}"
#         })
    
#     except Exception as e:
#         import traceback
#         traceback.print_exc()
#         return jsonify({'error': str(e)}), 500

# @app.route('/check-auth', methods=['GET'])
# def check_auth():
#     """Check if Google Service Account is configured"""
#     try:
#         service = get_google_service()
#         return jsonify({'authenticated': True, 'message': 'Service Account configured'})
#     except Exception as e:
#         return jsonify({'authenticated': False, 'error': str(e)}), 500

# if __name__ == '__main__':
#     app.run(debug=True, host='0.0.0.0', port=5000)
















































# from flask import Flask, render_template, request, jsonify, session
# from werkzeug.utils import secure_filename
# import os
# import re
# import pandas as pd
# from google.oauth2 import service_account
# from googleapiclient.discovery import build
# from googleapiclient.errors import HttpError
# import json
# from datetime import datetime

# app = Flask(__name__)
# app.secret_key = os.environ.get('SECRET_KEY', 'your-secret-key-change-this')
# app.config['UPLOAD_FOLDER'] = 'uploads'
# app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size

# # Ensure upload folder exists
# os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# # For Render deployment - read credentials from environment
# GOOGLE_CREDS = os.environ.get('GOOGLE_CREDENTIALS')
# if GOOGLE_CREDS:
#     with open('google.json', 'w') as f:
#         f.write(GOOGLE_CREDS)

# # Google Sheets configuration
# SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
# PASSED_SHEET_ID = '1rdSRNLdZPT5xXLRgV7wSn1beYwWZp41ZpYoLkbGmt0o'  # PASSED and FAILED tabs (CORRECT)
# PIKIPIKI_SHEET_ID = '1XFwPITQgZmzZ8lbg8MKD9S4rwHyk2cDOKrcxO7SAjHA'  # pikipiki records
# RECORDS_SHEET_ID = '1rOUnFHVO4MUwCsermoUNu0BDzmCqJTF2'  # Records sheet for validation

# def get_google_service():
#     """Create Google Sheets service using Service Account"""
#     try:
#         credentials = service_account.Credentials.from_service_account_file(
#             'google.json',
#             scopes=SCOPES
#         )
#         service = build('sheets', 'v4', credentials=credentials)
#         return service
#     except Exception as e:
#         print(f"Error creating service: {e}")
#         raise

# def extract_phone_number(text):
#     """Extract phone number from text in formats: 255XXXXXXXXX, 07XXXXXXXX, 06XXXXXXXX"""
#     if not text or pd.isna(text):
#         return None
    
#     text = str(text).replace(' ', '').replace('-', '')
    
#     # Pattern for 255 followed by 9 digits
#     pattern_255 = r'255(\d{9})'
#     match = re.search(pattern_255, text)
#     if match:
#         return f"255{match.group(1)}"
    
#     # Pattern for 07 or 06 followed by 8 digits
#     pattern_07_06 = r'0([67])(\d{8})(?!\d)'
#     match = re.search(pattern_07_06, text)
#     if match:
#         return f"0{match.group(1)}{match.group(2)}"
    
#     return None

# def extract_plate_number(text):
#     """Extract plate number in format: MC###XXX (MC followed by 3 numbers then 3 letters)"""
#     if not text or pd.isna(text):
#         return None
    
#     text = str(text).replace(' ', '').upper()
    
#     # Pattern for MC followed by 3 digits then 3 letters
#     pattern = r'MC(\d{3})([A-Z]{3})'
#     match = re.search(pattern, text)
#     if match:
#         return f"MC{match.group(1)}{match.group(2)}"
    
#     return None

# def extract_ref_number(text):
#     """Extract reference number from message (format: REF:XXXXX)"""
#     if not text or pd.isna(text):
#         return None
    
#     text = str(text)
#     pattern = r'REF:\s*(\S+)'
#     match = re.search(pattern, text, re.IGNORECASE)
#     if match:
#         return match.group(1)
    
#     return None

# def normalize_phone_for_comparison(phone):
#     """
#     Normalize phone number for comparison
#     - Removes 255 prefix if present
#     - Removes 0 prefix if present
#     - Returns just the 9-digit number (e.g., 752900450)
#     """
#     if not phone:
#         return None
    
#     phone = str(phone).replace(' ', '').replace('-', '')
    
#     # Remove 255 prefix
#     if phone.startswith('255'):
#         phone = phone[3:]
    
#     # Remove 0 prefix
#     if phone.startswith('0'):
#         phone = phone[1:]
    
#     # Should now have 9 digits
#     if len(phone) == 9 and phone.isdigit():
#         return phone
    
#     return None

# def load_records_data(service):
#     """
#     🔥 NEW: Load all data from Records sheet (columns D, E, G, H, I, J)
#     Returns sets of normalized phone numbers and plate numbers
#     """
#     # Try both possible tab names
#     possible_tab_names = ['Records', 'Record']
    
#     for tab_name in possible_tab_names:
#         try:
#             sheet = service.spreadsheets()
            
#             print(f"🔍 Trying to read from tab: '{tab_name}'...")
            
#             # Read columns D through J from the tab
#             result = sheet.values().get(
#                 spreadsheetId=RECORDS_SHEET_ID,
#                 range=f'{tab_name}!D:J'
#             ).execute()
            
#             values = result.get('values', [])
#             if not values:
#                 print(f"⚠️ No data found in '{tab_name}' tab")
#                 continue
            
#             plates_in_records = set()
#             phones_in_records = set()
            
#             print(f"✅ Successfully read {len(values)} rows from '{tab_name}' tab")
            
#             # Skip header row (row 0 might be header)
#             data_start_row = 1
            
#             # If first row looks like a header, skip it
#             if len(values) > 0 and values[0]:
#                 first_cell = str(values[0][0]) if values[0] else ''
#                 # Check if it looks like a header (contains text like "Namba", "Column", etc.)
#                 if any(keyword in first_cell.upper() for keyword in ['COLUMN', 'NAMBA', 'JINA', 'PLATE', 'PHONE']):
#                     data_start_row = 1
#                     print(f"  → Detected header row, starting from row {data_start_row + 1}")
            
#             # Process data rows
#             for row_idx, row in enumerate(values[data_start_row:], start=data_start_row + 1):
#                 if len(row) < 1:
#                     continue
                
#                 # Column D (index 0) - Plate Number
#                 if len(row) > 0 and row[0]:
#                     plate = str(row[0]).replace(' ', '').upper()
#                     if plate.startswith('MC'):
#                         plates_in_records.add(plate)
#                         if len(plates_in_records) <= 3:
#                             print(f"  → Found plate: {plate}")
                
#                 # Column E (index 1) - Customer Name (we don't need this for validation)
                
#                 # Columns G, H, I, J (indices 3, 4, 5, 6) - Phone Numbers
#                 for col_idx in [3, 4, 5, 6]:
#                     if len(row) > col_idx and row[col_idx]:
#                         phone_raw = str(row[col_idx]).replace(' ', '').replace('-', '')
                        
#                         # Normalize phone (remove prefixes, get just 9 digits)
#                         normalized = normalize_phone_for_comparison(phone_raw)
#                         if normalized:
#                             phones_in_records.add(normalized)
#                             if len(phones_in_records) <= 3:
#                                 print(f"  → Found phone: {phone_raw} (normalized: {normalized})")
            
#             print(f"📋 Loaded from '{tab_name}': {len(plates_in_records)} unique plates, {len(phones_in_records)} unique phones")
#             return phones_in_records, plates_in_records
            
#         except Exception as e:
#             print(f"⚠️ Could not read from '{tab_name}' tab: {e}")
#             continue
    
#     # If we get here, none of the tab names worked
#     print(f"❌ Could not load Records sheet from any tab name: {possible_tab_names}")
#     print(f"⚠️ CONTINUING WITHOUT RECORDS VALIDATION - all failed records will go to FAILED tab")
#     print(f"")
#     print(f"💡 To fix this:")
#     print(f"   1. Check the exact tab name in your sheet")
#     print(f"   2. Make sure service account has VIEW access")
#     print(f"   3. Verify the sheet ID is correct")
#     return set(), set()

# def check_exists_in_records(identifier, identifier_type, phones_in_records, plates_in_records):
#     """
#     🔥 NEW: Check if a phone or plate exists in the Records sheet
#     Returns True if exists (should be DISCARDED from FAILED)
#     Returns False if not exists (should be SENT to FAILED)
#     """
#     if identifier_type == 'phone':
#         # Normalize the phone number for comparison
#         normalized = normalize_phone_for_comparison(identifier)
#         if normalized and normalized in phones_in_records:
#             print(f"✅ Found phone {identifier} (normalized: {normalized}) in Records - DISCARDING from FAILED")
#             return True
#         return False
    
#     elif identifier_type == 'plate':
#         # Plates are already normalized (uppercase, no spaces)
#         plate_clean = str(identifier).replace(' ', '').upper()
#         if plate_clean in plates_in_records:
#             print(f"✅ Found plate {plate_clean} in Records - DISCARDING from FAILED")
#             return True
#         return False
    
#     return False

# def load_all_customers(service):
#     """Load all customers from pikipiki records sheet into memory (to avoid API quota issues)"""
#     try:
#         sheet = service.spreadsheets()
#         result = sheet.values().get(
#             spreadsheetId=PIKIPIKI_SHEET_ID,
#             range='pikipiki records!A:E'
#         ).execute()
        
#         values = result.get('values', [])
#         if not values:
#             return {}, {}
        
#         # Build lookup dictionaries
#         phone_lookup = {}  # phone -> customer name
#         plate_lookup = {}  # plate -> customer name
        
#         # Skip header row
#         for row in values[1:]:
#             if len(row) < 5:
#                 continue
            
#             # Columns: ID, Plate Number, Customer Name, Phone Number, Customer Name (duplicate?)
#             plate_col = row[1] if len(row) > 1 else ''
#             phone_col = row[3] if len(row) > 3 else ''
#             name_col = row[2] if len(row) > 2 else ''
            
#             # Clean and store plate
#             if plate_col:
#                 plate_clean = str(plate_col).replace(' ', '').upper()
#                 if plate_clean:
#                     plate_lookup[plate_clean] = name_col
            
#             # Clean and store phone
#             if phone_col:
#                 phone_clean = str(phone_col).replace(' ', '').replace('-', '')
#                 if phone_clean:
#                     phone_lookup[phone_clean] = name_col
        
#         print(f"Loaded {len(phone_lookup)} phone numbers and {len(plate_lookup)} plates from pikipiki records")
#         return phone_lookup, plate_lookup
        
#     except Exception as e:
#         print(f"Error loading customers: {e}")
#         return {}, {}

# def lookup_customer_from_cache(identifier, lookup_type, phone_lookup, plate_lookup):
#     """Look up customer from cached data"""
#     if lookup_type == 'phone':
#         # Try exact match first
#         name = phone_lookup.get(identifier)
#         if name:
#             return name
        
#         # If identifier starts with 255, also try 07 format
#         if identifier.startswith('255'):
#             alt_format = '0' + identifier[3:]  # 255752900450 -> 0752900450
#             name = phone_lookup.get(alt_format)
#             if name:
#                 return name
        
#         # If identifier starts with 07, also try 255 format
#         elif identifier.startswith('07'):
#             alt_format = '255' + identifier[1:]  # 0752900450 -> 255752900450
#             name = phone_lookup.get(alt_format)
#             if name:
#                 return name
        
#         # If identifier starts with 06, also try 255 format
#         elif identifier.startswith('06'):
#             alt_format = '255' + identifier[1:]  # 0652900450 -> 255652900450
#             name = phone_lookup.get(alt_format)
#             if name:
#                 return name
        
#         return None
        
#     elif lookup_type == 'plate':
#         return plate_lookup.get(identifier)
#     return None

# def get_existing_refs(service, sheet_name='PASSED'):
#     """
#     🔥 IMPROVED: Get existing reference numbers AND check message details for duplicates
#     Returns both a set of REFs and a set of message details for comprehensive duplicate detection
#     """
#     try:
#         sheet = service.spreadsheets()
        
#         # 🔥 FIXED: Read REFNUMBER from correct column for each sheet
#         if sheet_name == 'FAILED':
#             # FAILED: D=MESSAGE, I=REFNUMBER (column I!)
#             ref_column = 'I'
#         else:  # PASSED
#             # PASSED: D=MESSAGE, H=REFNUMBER  
#             ref_column = 'H'
        
#         print(f"📖 Reading {sheet_name}: MESSAGE from column D, REFNUMBER from column {ref_column}")
        
#         result = service.spreadsheets().values().batchGet(
#             spreadsheetId=PASSED_SHEET_ID,
#             ranges=[f'{sheet_name}!D1:D10000', f'{sheet_name}!{ref_column}1:{ref_column}10000']
#         ).execute()
        
#         value_ranges = result.get('valueRanges', [])
        
#         refs = set()
#         messages = set()
        
#         # Process REFNUMBER column
#         if len(value_ranges) > 1:
#             ref_values = value_ranges[1].get('values', [])
#             print(f"📊 {sheet_name} - Column {ref_column} (REFNUMBER): {len(ref_values)} rows")
            
#             for idx, row in enumerate(ref_values[1:], start=2):  # Skip header
#                 if row and len(row) > 0 and row[0]:
#                     ref = str(row[0]).strip()
#                     if ref and ref.lower() != 'refnumber':
#                         refs.add(ref)
#                         if len(refs) <= 3:  # Show first 3 for debugging
#                             print(f"  ✓ Row {idx} REF: '{ref}'")
        
#         # Process MESSAGE column - extract REF from message as backup
#         if len(value_ranges) > 0:
#             message_values = value_ranges[0].get('values', [])
#             print(f"📊 {sheet_name} - Column D (MESSAGE): {len(message_values)} rows")
            
#             for idx, row in enumerate(message_values[1:], start=2):  # Skip header
#                 if row and len(row) > 0 and row[0]:
#                     message = str(row[0]).strip()
#                     messages.add(message)
                    
#                     # Also extract REF from message as backup
#                     import re
#                     pattern = r'REF:\s*(\S+)'
#                     match = re.search(pattern, message, re.IGNORECASE)
#                     if match:
#                         ref_from_msg = match.group(1)
#                         if ref_from_msg not in refs:
#                             refs.add(ref_from_msg)
#                             if len(refs) <= 5:
#                                 print(f"  ✓ Row {idx} REF from MSG: '{ref_from_msg}'")
        
#         print(f"✅ {sheet_name}: Found {len(refs)} unique REFs, {len(messages)} unique messages")
#         if refs:
#             print(f"   Sample REFs: {list(refs)[:3]}")
        
#         return refs, messages
        
#     except Exception as e:
#         print(f"❌ Error getting existing data from {sheet_name}: {e}")
#         import traceback
#         traceback.print_exc()
#         return set(), set()

# def get_last_id(service, sheet_name):
#     """Get the last ID from the sheet (ignores filters, reads all data)"""
#     try:
#         sheet = service.spreadsheets()
#         # Read ALL values from column A (ID column)
#         result = sheet.values().get(
#             spreadsheetId=PASSED_SHEET_ID,
#             range=f'{sheet_name}!A:A'
#         ).execute()
        
#         values = result.get('values', [])
        
#         if len(values) > 1:
#             # Get the last non-empty ID, starting from the end
#             for row in reversed(values[1:]):  # Skip header
#                 if row and len(row) > 0 and row[0]:
#                     try:
#                         last_id = int(row[0])
#                         print(f"Last ID in {sheet_name}: {last_id}")
#                         return last_id
#                     except (ValueError, TypeError):
#                         continue
        
#         print(f"No existing IDs found in {sheet_name}, starting from 0")
#         return 0
        
#     except Exception as e:
#         print(f"Error getting last ID: {e}")
#         return 0

# def get_last_row_number(service, sheet_name):
#     """Get the actual last row number (works even with filters)"""
#     try:
#         # Get all data from column A to find the true last row
#         result = service.spreadsheets().values().get(
#             spreadsheetId=PASSED_SHEET_ID,
#             range=f'{sheet_name}!A:A'
#         ).execute()
        
#         values = result.get('values', [])
#         # Return the number of rows (including header)
#         return len(values)
#     except Exception as e:
#         print(f"Error getting last row: {e}")
#         return 0

# def append_to_sheet(service, sheet_name, data):
#     """Append data to Google Sheet - WORKS WITH FILTERS"""
#     try:
#         # Get the actual last row number (ignores filters)
#         last_row = get_last_row_number(service, sheet_name)
        
#         # Calculate the starting row for new data
#         start_row = last_row + 1
        
#         # Build the range for new data
#         range_name = f'{sheet_name}!A{start_row}'
        
#         print(f"Attempting to append to {sheet_name} starting at row {start_row}")
#         print(f"Adding {len(data)} rows")
#         print(f"Data preview: {data[0] if data else 'No data'}")
        
#         # Use UPDATE instead of APPEND (works with filters!)
#         result = service.spreadsheets().values().update(
#             spreadsheetId=PASSED_SHEET_ID,
#             range=range_name,
#             valueInputOption='USER_ENTERED',
#             body={'values': data}
#         ).execute()
        
#         print(f"Update result: {result.get('updatedRows', 0)} rows added")
#         return True
        
#     except HttpError as e:
#         print(f"❌ Google Sheets API Error: {e}")
#         print(f"Error details: {e.error_details if hasattr(e, 'error_details') else 'No details'}")
#         if e.resp.status == 403:
#             print("Permission denied! Make sure the service account has Editor access to the sheet.")
#         return False
#     except Exception as e:
#         print(f"❌ Error appending to sheet: {e}")
#         import traceback
#         traceback.print_exc()
#         return False

# @app.route('/')
# def index():
#     return render_template('index.html')

# @app.route('/upload', methods=['POST'])
# def upload_file():
#     if 'file' not in request.files:
#         return jsonify({'error': 'No file uploaded'}), 400
    
#     file = request.files['file']
#     if file.filename == '':
#         return jsonify({'error': 'No file selected'}), 400
    
#     if not file.filename.endswith('.xlsx'):
#         return jsonify({'error': 'Please upload an Excel file (.xlsx)'}), 400
    
#     # Save file
#     filename = secure_filename(file.filename)
#     filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
#     file.save(filepath)
    
#     # Store filepath in session
#     session['filepath'] = filepath
    
#     return jsonify({'success': True, 'message': 'File uploaded successfully'})

# @app.route('/process', methods=['POST'])
# def process_transactions():
#     try:
#         # Get filepath from session
#         filepath = session.get('filepath')
#         if not filepath or not os.path.exists(filepath):
#             return jsonify({'error': 'No file uploaded'}), 400
        
#         # Read Excel file - CRDB format has headers at row 12
#         df = pd.read_excel(filepath, header=12)
        
#         # First row contains the actual column names
#         df.columns = df.iloc[0]
#         df = df[1:].reset_index(drop=True)
        
#         print(f"Columns found: {list(df.columns)}")
        
#         # Now we should have: Posting Date, Details, Value Date, Debit, Credit, Book Balance
#         required_columns = ['Posting Date', 'Details', 'Credit']
#         missing = [col for col in required_columns if col not in df.columns]
        
#         if missing:
#             return jsonify({
#                 'error': f'Missing required columns: {missing}. Found: {list(df.columns)}'
#             }), 400
        
#         # Filter only CREDIT transactions (money coming IN)
#         # Convert Credit column to numeric, handle commas
#         df['Credit'] = pd.to_numeric(df['Credit'].astype(str).str.replace(',', ''), errors='coerce')
#         df['Debit'] = pd.to_numeric(df['Debit'].astype(str).str.replace(',', ''), errors='coerce')
        
#         # Only credit transactions (Credit > 0 and Debit is 0 or NaN)
#         credit_df = df[(df['Credit'].notna()) & (df['Credit'] > 0) & 
#                        ((df['Debit'].isna()) | (df['Debit'] == 0))].copy()
        
#         print(f"Found {len(credit_df)} credit transactions")
        
#         # Initialize Google Sheets service
#         service = get_google_service()
        
#         # Load ALL customers from pikipiki records ONCE (to avoid API quota limits)
#         print("Loading customer database from pikipiki records...")
#         phone_lookup, plate_lookup = load_all_customers(service)
        
#         # 🔥 NEW: Load Records sheet data for validation
#         print("Loading Records sheet for validation...")
#         phones_in_records, plates_in_records = load_records_data(service)
        
#         # 🔥 IMPROVED: Get existing reference numbers AND messages from BOTH PASSED and FAILED sheets
#         print("Loading existing references from PASSED sheet...")
#         existing_passed_refs, existing_passed_messages = get_existing_refs(service, 'PASSED')
        
#         print("Loading existing references from FAILED sheet...")
#         existing_failed_refs, existing_failed_messages = get_existing_refs(service, 'FAILED')
        
#         # 🔥 COMBINE both sets to check against all existing refs AND messages
#         all_existing_refs = existing_passed_refs.union(existing_failed_refs)
#         all_existing_messages = existing_passed_messages.union(existing_failed_messages)
#         print(f"Total unique refs in system: {len(all_existing_refs)} (PASSED: {len(existing_passed_refs)}, FAILED: {len(existing_failed_refs)})")
#         print(f"Total unique messages in system: {len(all_existing_messages)}")
        
#         # Get last IDs
#         last_passed_id = get_last_id(service, 'PASSED')
#         last_failed_id = get_last_id(service, 'FAILED')
        
#         passed_data = []
#         failed_data = []
        
#         stats = {
#             'total': len(credit_df),
#             'passed': 0,
#             'failed': 0,
#             'skipped': 0,
#             'skipped_from_passed': 0,
#             'skipped_from_failed': 0,
#             'discarded_from_records': 0  # 🔥 NEW: Track records found in Records sheet
#         }
        
#         for idx, row in credit_df.iterrows():
#             posting_date = str(row.get('Posting Date', ''))
#             details = str(row.get('Details', ''))
#             credit_amount = row.get('Credit', 0)
            
#             # Extract reference number
#             ref_number = extract_ref_number(details)
            
#             # 🔥 IMPROVED: Check for duplicates using BOTH ref number AND full message
#             is_duplicate = False
#             duplicate_reason = ""
            
#             # Check 1: REF number match
#             if ref_number and ref_number in all_existing_refs:
#                 is_duplicate = True
#                 duplicate_reason = "REF"
#                 if ref_number in existing_passed_refs:
#                     stats['skipped_from_passed'] += 1
#                     print(f"⏭️ DUPLICATE (REF in PASSED): {ref_number}")
#                 else:
#                     stats['skipped_from_failed'] += 1
#                     print(f"⏭️ DUPLICATE (REF in FAILED): {ref_number}")
            
#             # Check 2: Exact message match (backup check)
#             elif details in all_existing_messages:
#                 is_duplicate = True
#                 duplicate_reason = "MESSAGE"
#                 stats['skipped'] += 1
#                 print(f"⏭️ DUPLICATE (MESSAGE): {details[:80]}...")
            
#             if is_duplicate:
#                 stats['skipped'] += 1
#                 continue
            
#             # Try to extract phone number or plate number from details
#             phone = extract_phone_number(details)
#             plate = extract_plate_number(details)
            
#             identifier = None
#             lookup_type = None
            
#             if phone:
#                 identifier = phone
#                 lookup_type = 'phone'
#                 print(f"Found phone: {phone} in: {details[:50]}")
#             elif plate:
#                 identifier = plate
#                 lookup_type = 'plate'
#                 print(f"Found plate: {plate} in: {details[:50]}")
            
#             if identifier and lookup_type:
#                 # Lookup customer name from cached pikipiki records
#                 customer_name = lookup_customer_from_cache(identifier, lookup_type, phone_lookup, plate_lookup)
                
#                 if customer_name:
#                     # Successfully found customer - add to PASSED
#                     last_passed_id += 1
#                     passed_row = [
#                         last_passed_id,        # ID
#                         posting_date,          # Date
#                         'CRDB',                # Channel
#                         details,               # Message (full details)
#                         credit_amount,         # Amount
#                         identifier,            # Plate/Phone
#                         customer_name,         # Name
#                         ref_number or ''       # Ref Number
#                     ]
#                     passed_data.append(passed_row)
#                     stats['passed'] += 1
#                     print(f"✅ PASSED: {customer_name} - {identifier} - {credit_amount}")
#                 else:
#                     # 🔥 NEW: Found phone/plate but no customer match - CHECK RECORDS SHEET FIRST
#                     exists_in_records = check_exists_in_records(identifier, lookup_type, phones_in_records, plates_in_records)
                    
#                     if exists_in_records:
#                         # Found in Records sheet - DISCARD, don't add to FAILED
#                         stats['discarded_from_records'] += 1
#                         print(f"🗑️ DISCARDED: {identifier} found in Records sheet - not adding to FAILED")
#                     else:
#                         # Not in Records sheet - add to FAILED
#                         last_failed_id += 1
#                         reason = f"{lookup_type.upper()}({identifier}) not found"
                        
#                         # Ensure phone has 255 prefix before sending to FAILED sheet
#                         final_identifier = identifier
#                         if lookup_type == 'phone':
#                             if not identifier.startswith('255'):
#                                 # Add 255 prefix
#                                 if identifier.startswith('0'):
#                                     final_identifier = '255' + identifier[1:]
#                                 else:
#                                     final_identifier = '255' + identifier
                        
#                         failed_row = [
#                             last_failed_id,    # ID
#                             posting_date,      # Date
#                             'CRDB',            # Channel
#                             details,           # Message
#                             credit_amount,     # Amount
#                             final_identifier,  # Plate/Phone (with 255 prefix for phones)
#                             reason,            # Reason
#                             ref_number or ''   # REF
#                         ]
#                         failed_data.append(failed_row)
#                         stats['failed'] += 1
#                         print(f"❌ FAILED: Customer not found for {final_identifier} (REF: {ref_number})")
#             else:
#                 # 🔥 NEW: No phone or plate found in details - CHECK IF WE SHOULD DISCARD
#                 # Since there's no identifier, we can't check Records, so add to FAILED
#                 last_failed_id += 1
#                 failed_row = [
#                     last_failed_id,        # ID
#                     posting_date,          # Date
#                     'CRDB',                # Channel
#                     details,               # Message
#                     credit_amount,         # Amount
#                     'No phone/plate',      # Plate/Phone column
#                     'No identifier',       # Reason
#                     ref_number or ''       # REF
#                 ]
#                 failed_data.append(failed_row)
#                 stats['failed'] += 1
#                 print(f"❌ FAILED: No phone/plate found in: {details[:50]} (REF: {ref_number})")
        
#         # Append to Google Sheets
#         if passed_data:
#             print(f"Appending {len(passed_data)} rows to PASSED sheet")
#             try:
#                 result = append_to_sheet(service, 'PASSED', passed_data)
#                 if result:
#                     print(f"✅ Successfully added {len(passed_data)} rows to PASSED")
#                 else:
#                     print(f"❌ Failed to add rows to PASSED")
#                     return jsonify({'error': 'Failed to write to PASSED sheet. Check permissions.'}), 500
#             except Exception as e:
#                 print(f"❌ Error writing to PASSED sheet: {e}")
#                 return jsonify({'error': f'Error writing to PASSED sheet: {str(e)}'}), 500
        
#         if failed_data:
#             print(f"Appending {len(failed_data)} rows to FAILED sheet")
#             try:
#                 result = append_to_sheet(service, 'FAILED', failed_data)
#                 if result:
#                     print(f"✅ Successfully added {len(failed_data)} rows to FAILED")
#                 else:
#                     print(f"❌ Failed to add rows to FAILED")
#                     return jsonify({'error': 'Failed to write to FAILED sheet. Check permissions.'}), 500
#             except Exception as e:
#                 print(f"❌ Error writing to FAILED sheet: {e}")
#                 return jsonify({'error': f'Error writing to FAILED sheet: {str(e)}'}), 500
        
#         # Clean up uploaded file
#         if os.path.exists(filepath):
#             os.remove(filepath)
        
#         # Build detailed message
#         skip_message = f"{stats['skipped']} skipped (duplicates)"
#         if stats['skipped'] > 0:
#             skip_message += f" - {stats['skipped_from_passed']} already in PASSED, {stats['skipped_from_failed']} already in FAILED"
        
#         discard_message = f", {stats['discarded_from_records']} discarded (found in Records sheet)"
        
#         return jsonify({
#             'success': True,
#             'stats': stats,
#             'message': f"Processed {stats['total']} credit transactions: {stats['passed']} passed, {stats['failed']} failed, {skip_message}{discard_message}"
#         })
    
#     except Exception as e:
#         import traceback
#         traceback.print_exc()
#         return jsonify({'error': str(e)}), 500

# @app.route('/check-auth', methods=['GET'])
# def check_auth():
#     """Check if Google Service Account is configured"""
#     try:
#         service = get_google_service()
#         return jsonify({'authenticated': True, 'message': 'Service Account configured'})
#     except Exception as e:
#         return jsonify({'authenticated': False, 'error': str(e)}), 500

# if __name__ == '__main__':
#     app.run(debug=True, host='0.0.0.0', port=5000)


























# from flask import Flask, render_template, request, jsonify, session
# from werkzeug.utils import secure_filename
# import os
# import re
# import pandas as pd
# from google.oauth2 import service_account
# from googleapiclient.discovery import build
# from googleapiclient.errors import HttpError
# import json
# from datetime import datetime

# app = Flask(__name__)
# app.secret_key = os.environ.get('SECRET_KEY', 'your-secret-key-change-this')
# app.config['UPLOAD_FOLDER'] = 'uploads'
# app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size

# # Ensure upload folder exists
# os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# # For Render deployment - read credentials from environment
# GOOGLE_CREDS = os.environ.get('GOOGLE_CREDENTIALS')
# if GOOGLE_CREDS:
#     with open('google.json', 'w') as f:
#         f.write(GOOGLE_CREDS)

# # Google Sheets configuration
# SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
# PASSED_SHEET_ID = '1rdSRNLdZPT5xXLRgV7wSn1beYwWZp41ZpYoLkbGmt0o'  # PASSED and FAILED tabs (CORRECT)
# PIKIPIKI_SHEET_ID = '1XFwPITQgZmzZ8lbg8MKD9S4rwHyk2cDOKrcxO7SAjHA'  # pikipiki records
# RECORDS_SHEET_ID = '1rOUnFHVO4MUwCsermoUNu0BDzmCqJTF2'  # Records sheet for validation

# def get_google_service():
#     """Create Google Sheets service using Service Account"""
#     try:
#         credentials = service_account.Credentials.from_service_account_file(
#             'google.json',
#             scopes=SCOPES
#         )
#         service = build('sheets', 'v4', credentials=credentials)
#         return service
#     except Exception as e:
#         print(f"Error creating service: {e}")
#         raise

# def extract_phone_number(text):
#     """Extract phone number from text in formats: 255XXXXXXXXX, 07XXXXXXXX, 06XXXXXXXX"""
#     if not text or pd.isna(text):
#         return None
    
#     text = str(text).replace(' ', '').replace('-', '')
    
#     # Pattern for 255 followed by 9 digits
#     pattern_255 = r'255(\d{9})'
#     match = re.search(pattern_255, text)
#     if match:
#         return f"255{match.group(1)}"
    
#     # Pattern for 07 or 06 followed by 8 digits
#     pattern_07_06 = r'0([67])(\d{8})(?!\d)'
#     match = re.search(pattern_07_06, text)
#     if match:
#         return f"0{match.group(1)}{match.group(2)}"
    
#     return None

# def extract_plate_number(text):
#     """Extract plate number in format: MC###XXX (MC followed by 3 numbers then 3 letters)"""
#     if not text or pd.isna(text):
#         return None
    
#     text = str(text).replace(' ', '').upper()
    
#     # Pattern for MC followed by 3 digits then 3 letters
#     pattern = r'MC(\d{3})([A-Z]{3})'
#     match = re.search(pattern, text)
#     if match:
#         return f"MC{match.group(1)}{match.group(2)}"
    
#     return None

# def extract_ref_number(text):
#     """Extract reference number from message (format: REF:XXXXX)"""
#     if not text or pd.isna(text):
#         return None
    
#     text = str(text)
#     pattern = r'REF:\s*(\S+)'
#     match = re.search(pattern, text, re.IGNORECASE)
#     if match:
#         return match.group(1)
    
#     return None

# def normalize_phone_for_comparison(phone):
#     """
#     Normalize phone number for comparison
#     - Removes 255 prefix if present
#     - Removes 0 prefix if present
#     - Returns just the 9-digit number (e.g., 752900450)
#     """
#     if not phone:
#         return None
    
#     phone = str(phone).replace(' ', '').replace('-', '')
    
#     # Remove 255 prefix
#     if phone.startswith('255'):
#         phone = phone[3:]
    
#     # Remove 0 prefix
#     if phone.startswith('0'):
#         phone = phone[1:]
    
#     # Should now have 9 digits
#     if len(phone) == 9 and phone.isdigit():
#         return phone
    
#     return None

# def load_records_data(service):
#     """
#     🔥 NEW: Load all data from Records sheet (columns D, E, G, H, I, J)
#     Returns sets of normalized phone numbers and plate numbers
#     """
#     # Try both possible tab names
#     possible_tab_names = ['Records', 'Record']
    
#     for tab_name in possible_tab_names:
#         try:
#             sheet = service.spreadsheets()
            
#             print(f"🔍 Trying to read from tab: '{tab_name}'...")
            
#             # Read columns D through J from the tab
#             result = sheet.values().get(
#                 spreadsheetId=RECORDS_SHEET_ID,
#                 range=f'{tab_name}!D:J'
#             ).execute()
            
#             values = result.get('values', [])
#             if not values:
#                 print(f"⚠️ No data found in '{tab_name}' tab")
#                 continue
            
#             plates_in_records = set()
#             phones_in_records = set()
            
#             print(f"✅ Successfully read {len(values)} rows from '{tab_name}' tab")
            
#             # Skip header row (row 0 might be header)
#             data_start_row = 1
            
#             # If first row looks like a header, skip it
#             if len(values) > 0 and values[0]:
#                 first_cell = str(values[0][0]) if values[0] else ''
#                 # Check if it looks like a header (contains text like "Namba", "Column", etc.)
#                 if any(keyword in first_cell.upper() for keyword in ['COLUMN', 'NAMBA', 'JINA', 'PLATE', 'PHONE']):
#                     data_start_row = 1
#                     print(f"  → Detected header row, starting from row {data_start_row + 1}")
            
#             # Process data rows
#             for row_idx, row in enumerate(values[data_start_row:], start=data_start_row + 1):
#                 if len(row) < 1:
#                     continue
                
#                 # Column D (index 0) - Plate Number
#                 if len(row) > 0 and row[0]:
#                     plate = str(row[0]).replace(' ', '').upper()
#                     if plate.startswith('MC'):
#                         plates_in_records.add(plate)
#                         if len(plates_in_records) <= 3:
#                             print(f"  → Found plate: {plate}")
                
#                 # Column E (index 1) - Customer Name (we don't need this for validation)
                
#                 # Columns G, H, I, J (indices 3, 4, 5, 6) - Phone Numbers
#                 for col_idx in [3, 4, 5, 6]:
#                     if len(row) > col_idx and row[col_idx]:
#                         phone_raw = str(row[col_idx]).replace(' ', '').replace('-', '')
                        
#                         # Normalize phone (remove prefixes, get just 9 digits)
#                         normalized = normalize_phone_for_comparison(phone_raw)
#                         if normalized:
#                             phones_in_records.add(normalized)
#                             if len(phones_in_records) <= 3:
#                                 print(f"  → Found phone: {phone_raw} (normalized: {normalized})")
            
#             print(f"📋 Loaded from '{tab_name}': {len(plates_in_records)} unique plates, {len(phones_in_records)} unique phones")
#             return phones_in_records, plates_in_records
            
#         except Exception as e:
#             print(f"⚠️ Could not read from '{tab_name}' tab: {e}")
#             continue
    
#     # If we get here, none of the tab names worked
#     print(f"❌ Could not load Records sheet from any tab name: {possible_tab_names}")
#     print(f"⚠️ CONTINUING WITHOUT RECORDS VALIDATION - all failed records will go to FAILED tab")
#     print(f"")
#     print(f"💡 To fix this:")
#     print(f"   1. Check the exact tab name in your sheet")
#     print(f"   2. Make sure service account has VIEW access")
#     print(f"   3. Verify the sheet ID is correct")
#     return set(), set()

# def check_exists_in_records(identifier, identifier_type, phones_in_records, plates_in_records):
#     """
#     🔥 NEW: Check if a phone or plate exists in the Records sheet
#     Returns True if exists (should be DISCARDED from FAILED)
#     Returns False if not exists (should be SENT to FAILED)
#     """
#     if identifier_type == 'phone':
#         # Normalize the phone number for comparison
#         normalized = normalize_phone_for_comparison(identifier)
#         if normalized and normalized in phones_in_records:
#             print(f"✅ Found phone {identifier} (normalized: {normalized}) in Records - DISCARDING from FAILED")
#             return True
#         return False
    
#     elif identifier_type == 'plate':
#         # Plates are already normalized (uppercase, no spaces)
#         plate_clean = str(identifier).replace(' ', '').upper()
#         if plate_clean in plates_in_records:
#             print(f"✅ Found plate {plate_clean} in Records - DISCARDING from FAILED")
#             return True
#         return False
    
#     return False

# def load_all_customers(service):
#     """Load all customers from pikipiki records sheet into memory (to avoid API quota issues)"""
#     try:
#         sheet = service.spreadsheets()
#         result = sheet.values().get(
#             spreadsheetId=PIKIPIKI_SHEET_ID,
#             range='pikipiki records!A:E'
#         ).execute()
        
#         values = result.get('values', [])
#         if not values:
#             return {}, {}
        
#         # Build lookup dictionaries
#         phone_lookup = {}  # phone -> customer name
#         plate_lookup = {}  # plate -> customer name
        
#         # Skip header row
#         for row in values[1:]:
#             if len(row) < 5:
#                 continue
            
#             # Columns: ID, Plate Number, Customer Name, Phone Number, Customer Name (duplicate?)
#             plate_col = row[1] if len(row) > 1 else ''
#             phone_col = row[3] if len(row) > 3 else ''
#             name_col = row[2] if len(row) > 2 else ''
            
#             # Clean and store plate
#             if plate_col:
#                 plate_clean = str(plate_col).replace(' ', '').upper()
#                 if plate_clean:
#                     plate_lookup[plate_clean] = name_col
            
#             # Clean and store phone
#             if phone_col:
#                 phone_clean = str(phone_col).replace(' ', '').replace('-', '')
#                 if phone_clean:
#                     phone_lookup[phone_clean] = name_col
        
#         print(f"Loaded {len(phone_lookup)} phone numbers and {len(plate_lookup)} plates from pikipiki records")
#         return phone_lookup, plate_lookup
        
#     except Exception as e:
#         print(f"Error loading customers: {e}")
#         return {}, {}

# def lookup_customer_from_cache(identifier, lookup_type, phone_lookup, plate_lookup):
#     """Look up customer from cached data"""
#     if lookup_type == 'phone':
#         # Try exact match first
#         name = phone_lookup.get(identifier)
#         if name:
#             return name
        
#         # If identifier starts with 255, also try 07 format
#         if identifier.startswith('255'):
#             alt_format = '0' + identifier[3:]  # 255752900450 -> 0752900450
#             name = phone_lookup.get(alt_format)
#             if name:
#                 return name
        
#         # If identifier starts with 07, also try 255 format
#         elif identifier.startswith('07'):
#             alt_format = '255' + identifier[1:]  # 0752900450 -> 255752900450
#             name = phone_lookup.get(alt_format)
#             if name:
#                 return name
        
#         # If identifier starts with 06, also try 255 format
#         elif identifier.startswith('06'):
#             alt_format = '255' + identifier[1:]  # 0652900450 -> 255652900450
#             name = phone_lookup.get(alt_format)
#             if name:
#                 return name
        
#         return None
        
#     elif lookup_type == 'plate':
#         return plate_lookup.get(identifier)
#     return None

# def get_existing_refs(service, sheet_name='PASSED'):
#     """
#     🔥 IMPROVED: Get existing reference numbers AND check message details for duplicates
#     Returns both a set of REFs and a set of message details for comprehensive duplicate detection
#     """
#     try:
#         sheet = service.spreadsheets()
        
#         # Read BOTH column D (MESSAGE) and column H (REFNUMBER)
#         if sheet_name == 'FAILED':
#             # FAILED: D=MESSAGE, H=REFNUMBER
#             range_to_read = f'{sheet_name}!D:D,H:H'
#         else:  # PASSED
#             # PASSED: D=MESSAGE, H=REFNUMBER  
#             range_to_read = f'{sheet_name}!D:D,H:H'
        
#         result = service.spreadsheets().values().batchGet(
#             spreadsheetId=PASSED_SHEET_ID,
#             ranges=[f'{sheet_name}!D1:D10000', f'{sheet_name}!H1:H10000']
#         ).execute()
        
#         value_ranges = result.get('valueRanges', [])
        
#         refs = set()
#         messages = set()
        
#         # Process column H (REFNUMBER)
#         if len(value_ranges) > 1:
#             ref_values = value_ranges[1].get('values', [])
#             print(f"📊 {sheet_name} - Column H (REFNUMBER): {len(ref_values)} rows")
            
#             for idx, row in enumerate(ref_values[1:], start=2):  # Skip header
#                 if row and len(row) > 0 and row[0]:
#                     ref = str(row[0]).strip()
#                     if ref and ref.lower() != 'refnumber':
#                         refs.add(ref)
#                         if len(refs) <= 3:  # Show first 3 for debugging
#                             print(f"  ✓ Row {idx} REF: '{ref}'")
        
#         # Process column D (MESSAGE) - extract REF from message as backup
#         if len(value_ranges) > 0:
#             message_values = value_ranges[0].get('values', [])
#             print(f"📊 {sheet_name} - Column D (MESSAGE): {len(message_values)} rows")
            
#             for idx, row in enumerate(message_values[1:], start=2):  # Skip header
#                 if row and len(row) > 0 and row[0]:
#                     message = str(row[0]).strip()
#                     messages.add(message)
                    
#                     # Also extract REF from message as backup
#                     import re
#                     pattern = r'REF:\s*(\S+)'
#                     match = re.search(pattern, message, re.IGNORECASE)
#                     if match:
#                         ref_from_msg = match.group(1)
#                         if ref_from_msg not in refs:
#                             refs.add(ref_from_msg)
#                             if len(refs) <= 5:
#                                 print(f"  ✓ Row {idx} REF from MSG: '{ref_from_msg}'")
        
#         print(f"✅ {sheet_name}: Found {len(refs)} unique REFs, {len(messages)} unique messages")
#         if refs:
#             print(f"   Sample REFs: {list(refs)[:3]}")
        
#         return refs, messages
        
#     except Exception as e:
#         print(f"❌ Error getting existing data from {sheet_name}: {e}")
#         import traceback
#         traceback.print_exc()
#         return set(), set()

# def get_last_id(service, sheet_name):
#     """Get the last ID from the sheet (ignores filters, reads all data)"""
#     try:
#         sheet = service.spreadsheets()
#         # Read ALL values from column A (ID column)
#         result = sheet.values().get(
#             spreadsheetId=PASSED_SHEET_ID,
#             range=f'{sheet_name}!A:A'
#         ).execute()
        
#         values = result.get('values', [])
        
#         if len(values) > 1:
#             # Get the last non-empty ID, starting from the end
#             for row in reversed(values[1:]):  # Skip header
#                 if row and len(row) > 0 and row[0]:
#                     try:
#                         last_id = int(row[0])
#                         print(f"Last ID in {sheet_name}: {last_id}")
#                         return last_id
#                     except (ValueError, TypeError):
#                         continue
        
#         print(f"No existing IDs found in {sheet_name}, starting from 0")
#         return 0
        
#     except Exception as e:
#         print(f"Error getting last ID: {e}")
#         return 0

# def get_last_row_number(service, sheet_name):
#     """Get the actual last row number (works even with filters)"""
#     try:
#         # Get all data from column A to find the true last row
#         result = service.spreadsheets().values().get(
#             spreadsheetId=PASSED_SHEET_ID,
#             range=f'{sheet_name}!A:A'
#         ).execute()
        
#         values = result.get('values', [])
#         # Return the number of rows (including header)
#         return len(values)
#     except Exception as e:
#         print(f"Error getting last row: {e}")
#         return 0

# def append_to_sheet(service, sheet_name, data):
#     """Append data to Google Sheet - WORKS WITH FILTERS"""
#     try:
#         # Get the actual last row number (ignores filters)
#         last_row = get_last_row_number(service, sheet_name)
        
#         # Calculate the starting row for new data
#         start_row = last_row + 1
        
#         # Build the range for new data
#         range_name = f'{sheet_name}!A{start_row}'
        
#         print(f"Attempting to append to {sheet_name} starting at row {start_row}")
#         print(f"Adding {len(data)} rows")
#         print(f"Data preview: {data[0] if data else 'No data'}")
        
#         # Use UPDATE instead of APPEND (works with filters!)
#         result = service.spreadsheets().values().update(
#             spreadsheetId=PASSED_SHEET_ID,
#             range=range_name,
#             valueInputOption='USER_ENTERED',
#             body={'values': data}
#         ).execute()
        
#         print(f"Update result: {result.get('updatedRows', 0)} rows added")
#         return True
        
#     except HttpError as e:
#         print(f"❌ Google Sheets API Error: {e}")
#         print(f"Error details: {e.error_details if hasattr(e, 'error_details') else 'No details'}")
#         if e.resp.status == 403:
#             print("Permission denied! Make sure the service account has Editor access to the sheet.")
#         return False
#     except Exception as e:
#         print(f"❌ Error appending to sheet: {e}")
#         import traceback
#         traceback.print_exc()
#         return False

# @app.route('/')
# def index():
#     return render_template('index.html')

# @app.route('/upload', methods=['POST'])
# def upload_file():
#     if 'file' not in request.files:
#         return jsonify({'error': 'No file uploaded'}), 400
    
#     file = request.files['file']
#     if file.filename == '':
#         return jsonify({'error': 'No file selected'}), 400
    
#     if not file.filename.endswith('.xlsx'):
#         return jsonify({'error': 'Please upload an Excel file (.xlsx)'}), 400
    
#     # Save file
#     filename = secure_filename(file.filename)
#     filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
#     file.save(filepath)
    
#     # Store filepath in session
#     session['filepath'] = filepath
    
#     return jsonify({'success': True, 'message': 'File uploaded successfully'})

# @app.route('/process', methods=['POST'])
# def process_transactions():
#     try:
#         # Get filepath from session
#         filepath = session.get('filepath')
#         if not filepath or not os.path.exists(filepath):
#             return jsonify({'error': 'No file uploaded'}), 400
        
#         # Read Excel file - CRDB format has headers at row 12
#         df = pd.read_excel(filepath, header=12)
        
#         # First row contains the actual column names
#         df.columns = df.iloc[0]
#         df = df[1:].reset_index(drop=True)
        
#         print(f"Columns found: {list(df.columns)}")
        
#         # Now we should have: Posting Date, Details, Value Date, Debit, Credit, Book Balance
#         required_columns = ['Posting Date', 'Details', 'Credit']
#         missing = [col for col in required_columns if col not in df.columns]
        
#         if missing:
#             return jsonify({
#                 'error': f'Missing required columns: {missing}. Found: {list(df.columns)}'
#             }), 400
        
#         # Filter only CREDIT transactions (money coming IN)
#         # Convert Credit column to numeric, handle commas
#         df['Credit'] = pd.to_numeric(df['Credit'].astype(str).str.replace(',', ''), errors='coerce')
#         df['Debit'] = pd.to_numeric(df['Debit'].astype(str).str.replace(',', ''), errors='coerce')
        
#         # Only credit transactions (Credit > 0 and Debit is 0 or NaN)
#         credit_df = df[(df['Credit'].notna()) & (df['Credit'] > 0) & 
#                        ((df['Debit'].isna()) | (df['Debit'] == 0))].copy()
        
#         print(f"Found {len(credit_df)} credit transactions")
        
#         # Initialize Google Sheets service
#         service = get_google_service()
        
#         # Load ALL customers from pikipiki records ONCE (to avoid API quota limits)
#         print("Loading customer database from pikipiki records...")
#         phone_lookup, plate_lookup = load_all_customers(service)
        
#         # 🔥 NEW: Load Records sheet data for validation
#         print("Loading Records sheet for validation...")
#         phones_in_records, plates_in_records = load_records_data(service)
        
#         # 🔥 IMPROVED: Get existing reference numbers AND messages from BOTH PASSED and FAILED sheets
#         print("Loading existing references from PASSED sheet...")
#         existing_passed_refs, existing_passed_messages = get_existing_refs(service, 'PASSED')
        
#         print("Loading existing references from FAILED sheet...")
#         existing_failed_refs, existing_failed_messages = get_existing_refs(service, 'FAILED')
        
#         # 🔥 COMBINE both sets to check against all existing refs AND messages
#         all_existing_refs = existing_passed_refs.union(existing_failed_refs)
#         all_existing_messages = existing_passed_messages.union(existing_failed_messages)
#         print(f"Total unique refs in system: {len(all_existing_refs)} (PASSED: {len(existing_passed_refs)}, FAILED: {len(existing_failed_refs)})")
#         print(f"Total unique messages in system: {len(all_existing_messages)}")
        
#         # Get last IDs
#         last_passed_id = get_last_id(service, 'PASSED')
#         last_failed_id = get_last_id(service, 'FAILED')
        
#         passed_data = []
#         failed_data = []
        
#         stats = {
#             'total': len(credit_df),
#             'passed': 0,
#             'failed': 0,
#             'skipped': 0,
#             'skipped_from_passed': 0,
#             'skipped_from_failed': 0,
#             'discarded_from_records': 0  # 🔥 NEW: Track records found in Records sheet
#         }
        
#         for idx, row in credit_df.iterrows():
#             posting_date = str(row.get('Posting Date', ''))
#             details = str(row.get('Details', ''))
#             credit_amount = row.get('Credit', 0)
            
#             # Extract reference number
#             ref_number = extract_ref_number(details)
            
#             # 🔥 IMPROVED: Check for duplicates using BOTH ref number AND full message
#             is_duplicate = False
#             duplicate_reason = ""
            
#             # Check 1: REF number match
#             if ref_number and ref_number in all_existing_refs:
#                 is_duplicate = True
#                 duplicate_reason = "REF"
#                 if ref_number in existing_passed_refs:
#                     stats['skipped_from_passed'] += 1
#                     print(f"⏭️ DUPLICATE (REF in PASSED): {ref_number}")
#                 else:
#                     stats['skipped_from_failed'] += 1
#                     print(f"⏭️ DUPLICATE (REF in FAILED): {ref_number}")
            
#             # Check 2: Exact message match (backup check)
#             elif details in all_existing_messages:
#                 is_duplicate = True
#                 duplicate_reason = "MESSAGE"
#                 stats['skipped'] += 1
#                 print(f"⏭️ DUPLICATE (MESSAGE): {details[:80]}...")
            
#             if is_duplicate:
#                 stats['skipped'] += 1
#                 continue
            
#             # Try to extract phone number or plate number from details
#             phone = extract_phone_number(details)
#             plate = extract_plate_number(details)
            
#             identifier = None
#             lookup_type = None
            
#             if phone:
#                 identifier = phone
#                 lookup_type = 'phone'
#                 print(f"Found phone: {phone} in: {details[:50]}")
#             elif plate:
#                 identifier = plate
#                 lookup_type = 'plate'
#                 print(f"Found plate: {plate} in: {details[:50]}")
            
#             if identifier and lookup_type:
#                 # Lookup customer name from cached pikipiki records
#                 customer_name = lookup_customer_from_cache(identifier, lookup_type, phone_lookup, plate_lookup)
                
#                 if customer_name:
#                     # Successfully found customer - add to PASSED
#                     last_passed_id += 1
#                     passed_row = [
#                         last_passed_id,        # ID
#                         posting_date,          # Date
#                         'CRDB',                # Channel
#                         details,               # Message (full details)
#                         credit_amount,         # Amount
#                         identifier,            # Plate/Phone
#                         customer_name,         # Name
#                         ref_number or ''       # Ref Number
#                     ]
#                     passed_data.append(passed_row)
#                     stats['passed'] += 1
#                     print(f"✅ PASSED: {customer_name} - {identifier} - {credit_amount}")
#                 else:
#                     # 🔥 NEW: Found phone/plate but no customer match - CHECK RECORDS SHEET FIRST
#                     exists_in_records = check_exists_in_records(identifier, lookup_type, phones_in_records, plates_in_records)
                    
#                     if exists_in_records:
#                         # Found in Records sheet - DISCARD, don't add to FAILED
#                         stats['discarded_from_records'] += 1
#                         print(f"🗑️ DISCARDED: {identifier} found in Records sheet - not adding to FAILED")
#                     else:
#                         # Not in Records sheet - add to FAILED
#                         last_failed_id += 1
#                         reason = f"{lookup_type.upper()}({identifier}) not found"
                        
#                         # Ensure phone has 255 prefix before sending to FAILED sheet
#                         final_identifier = identifier
#                         if lookup_type == 'phone':
#                             if not identifier.startswith('255'):
#                                 # Add 255 prefix
#                                 if identifier.startswith('0'):
#                                     final_identifier = '255' + identifier[1:]
#                                 else:
#                                     final_identifier = '255' + identifier
                        
#                         failed_row = [
#                             last_failed_id,    # ID
#                             posting_date,      # Date
#                             'CRDB',            # Channel
#                             details,           # Message
#                             credit_amount,     # Amount
#                             final_identifier,  # Plate/Phone (with 255 prefix for phones)
#                             reason,            # Reason
#                             ref_number or ''   # REF
#                         ]
#                         failed_data.append(failed_row)
#                         stats['failed'] += 1
#                         print(f"❌ FAILED: Customer not found for {final_identifier} (REF: {ref_number})")
#             else:
#                 # 🔥 NEW: No phone or plate found in details - CHECK IF WE SHOULD DISCARD
#                 # Since there's no identifier, we can't check Records, so add to FAILED
#                 last_failed_id += 1
#                 failed_row = [
#                     last_failed_id,        # ID
#                     posting_date,          # Date
#                     'CRDB',                # Channel
#                     details,               # Message
#                     credit_amount,         # Amount
#                     'No phone/plate',      # Plate/Phone column
#                     'No identifier',       # Reason
#                     ref_number or ''       # REF
#                 ]
#                 failed_data.append(failed_row)
#                 stats['failed'] += 1
#                 print(f"❌ FAILED: No phone/plate found in: {details[:50]} (REF: {ref_number})")
        
#         # Append to Google Sheets
#         if passed_data:
#             print(f"Appending {len(passed_data)} rows to PASSED sheet")
#             try:
#                 result = append_to_sheet(service, 'PASSED', passed_data)
#                 if result:
#                     print(f"✅ Successfully added {len(passed_data)} rows to PASSED")
#                 else:
#                     print(f"❌ Failed to add rows to PASSED")
#                     return jsonify({'error': 'Failed to write to PASSED sheet. Check permissions.'}), 500
#             except Exception as e:
#                 print(f"❌ Error writing to PASSED sheet: {e}")
#                 return jsonify({'error': f'Error writing to PASSED sheet: {str(e)}'}), 500
        
#         if failed_data:
#             print(f"Appending {len(failed_data)} rows to FAILED sheet")
#             try:
#                 result = append_to_sheet(service, 'FAILED', failed_data)
#                 if result:
#                     print(f"✅ Successfully added {len(failed_data)} rows to FAILED")
#                 else:
#                     print(f"❌ Failed to add rows to FAILED")
#                     return jsonify({'error': 'Failed to write to FAILED sheet. Check permissions.'}), 500
#             except Exception as e:
#                 print(f"❌ Error writing to FAILED sheet: {e}")
#                 return jsonify({'error': f'Error writing to FAILED sheet: {str(e)}'}), 500
        
#         # Clean up uploaded file
#         if os.path.exists(filepath):
#             os.remove(filepath)
        
#         # Build detailed message
#         skip_message = f"{stats['skipped']} skipped (duplicates)"
#         if stats['skipped'] > 0:
#             skip_message += f" - {stats['skipped_from_passed']} already in PASSED, {stats['skipped_from_failed']} already in FAILED"
        
#         discard_message = f", {stats['discarded_from_records']} discarded (found in Records sheet)"
        
#         return jsonify({
#             'success': True,
#             'stats': stats,
#             'message': f"Processed {stats['total']} credit transactions: {stats['passed']} passed, {stats['failed']} failed, {skip_message}{discard_message}"
#         })
    
#     except Exception as e:
#         import traceback
#         traceback.print_exc()
#         return jsonify({'error': str(e)}), 500

# @app.route('/check-auth', methods=['GET'])
# def check_auth():
#     """Check if Google Service Account is configured"""
#     try:
#         service = get_google_service()
#         return jsonify({'authenticated': True, 'message': 'Service Account configured'})
#     except Exception as e:
#         return jsonify({'authenticated': False, 'error': str(e)}), 500

# if __name__ == '__main__':
#     app.run(debug=True, host='0.0.0.0', port=5000)





































# from flask import Flask, render_template, request, jsonify, session
# from werkzeug.utils import secure_filename
# import os
# import re
# import pandas as pd
# from google.oauth2 import service_account
# from googleapiclient.discovery import build
# from googleapiclient.errors import HttpError
# import json
# from datetime import datetime

# app = Flask(__name__)
# app.secret_key = os.environ.get('SECRET_KEY', 'your-secret-key-change-this')
# app.config['UPLOAD_FOLDER'] = 'uploads'
# app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size

# # Ensure upload folder exists
# os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# # For Render deployment - read credentials from environment
# GOOGLE_CREDS = os.environ.get('GOOGLE_CREDENTIALS')
# if GOOGLE_CREDS:
#     with open('google.json', 'w') as f:
#         f.write(GOOGLE_CREDS)

# # Google Sheets configuration
# SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
# PASSED_SHEET_ID = '1rdSRNLdZPT5xXLRgV7wSn1beYwWZp41ZpYoLkbGmt0o'  # PASSED and FAILED tabs (CORRECT)
# PIKIPIKI_SHEET_ID = '1XFwPITQgZmzZ8lbg8MKD9S4rwHyk2cDOKrcxO7SAjHA'  # pikipiki records
# RECORDS_SHEET_ID = '1rOUnFHVO4MUwCsermoUNu0BDzmCqJTF2'  # Records sheet for validation

# def get_google_service():
#     """Create Google Sheets service using Service Account"""
#     try:
#         credentials = service_account.Credentials.from_service_account_file(
#             'google.json',
#             scopes=SCOPES
#         )
#         service = build('sheets', 'v4', credentials=credentials)
#         return service
#     except Exception as e:
#         print(f"Error creating service: {e}")
#         raise

# def extract_phone_number(text):
#     """Extract phone number from text in formats: 255XXXXXXXXX, 07XXXXXXXX, 06XXXXXXXX"""
#     if not text or pd.isna(text):
#         return None
    
#     text = str(text).replace(' ', '').replace('-', '')
    
#     # Pattern for 255 followed by 9 digits
#     pattern_255 = r'255(\d{9})'
#     match = re.search(pattern_255, text)
#     if match:
#         return f"255{match.group(1)}"
    
#     # Pattern for 07 or 06 followed by 8 digits
#     pattern_07_06 = r'0([67])(\d{8})(?!\d)'
#     match = re.search(pattern_07_06, text)
#     if match:
#         return f"0{match.group(1)}{match.group(2)}"
    
#     return None

# def extract_plate_number(text):
#     """Extract plate number in format: MC###XXX (MC followed by 3 numbers then 3 letters)"""
#     if not text or pd.isna(text):
#         return None
    
#     text = str(text).replace(' ', '').upper()
    
#     # Pattern for MC followed by 3 digits then 3 letters
#     pattern = r'MC(\d{3})([A-Z]{3})'
#     match = re.search(pattern, text)
#     if match:
#         return f"MC{match.group(1)}{match.group(2)}"
    
#     return None

# def extract_ref_number(text):
#     """Extract reference number from message (format: REF:XXXXX)"""
#     if not text or pd.isna(text):
#         return None
    
#     text = str(text)
#     pattern = r'REF:\s*(\S+)'
#     match = re.search(pattern, text, re.IGNORECASE)
#     if match:
#         return match.group(1)
    
#     return None

# def normalize_phone_for_comparison(phone):
#     """
#     Normalize phone number for comparison
#     - Removes 255 prefix if present
#     - Removes 0 prefix if present
#     - Returns just the 9-digit number (e.g., 752900450)
#     """
#     if not phone:
#         return None
    
#     phone = str(phone).replace(' ', '').replace('-', '')
    
#     # Remove 255 prefix
#     if phone.startswith('255'):
#         phone = phone[3:]
    
#     # Remove 0 prefix
#     if phone.startswith('0'):
#         phone = phone[1:]
    
#     # Should now have 9 digits
#     if len(phone) == 9 and phone.isdigit():
#         return phone
    
#     return None

# def load_records_data(service):
#     """
#     🔥 NEW: Load all data from Records sheet (columns D, E, G, H, I, J)
#     Returns sets of normalized phone numbers and plate numbers
#     """
#     try:
#         sheet = service.spreadsheets()
        
#         # Read columns D through J from Records tab
#         result = sheet.values().get(
#             spreadsheetId=RECORDS_SHEET_ID,
#             range='Records!D:J'
#         ).execute()
        
#         values = result.get('values', [])
#         if not values:
#             print("⚠️ No data found in Records sheet")
#             return set(), set()
        
#         plates_in_records = set()
#         phones_in_records = set()
        
#         # Skip header row
#         for row in values[1:]:
#             if len(row) < 1:
#                 continue
            
#             # Column D (index 0) - Plate Number
#             if len(row) > 0 and row[0]:
#                 plate = str(row[0]).replace(' ', '').upper()
#                 if plate.startswith('MC'):
#                     plates_in_records.add(plate)
            
#             # Column E (index 1) - Customer Name (we don't need this for validation)
            
#             # Columns G, H, I, J (indices 3, 4, 5, 6) - Phone Numbers
#             for col_idx in [3, 4, 5, 6]:
#                 if len(row) > col_idx and row[col_idx]:
#                     phone_raw = str(row[col_idx]).replace(' ', '').replace('-', '')
                    
#                     # Normalize phone (remove prefixes, get just 9 digits)
#                     normalized = normalize_phone_for_comparison(phone_raw)
#                     if normalized:
#                         phones_in_records.add(normalized)
        
#         print(f"📋 Loaded Records sheet: {len(plates_in_records)} unique plates, {len(phones_in_records)} unique phones")
#         return phones_in_records, plates_in_records
        
#     except Exception as e:
#         print(f"❌ Error loading Records sheet: {e}")
#         import traceback
#         traceback.print_exc()
#         return set(), set()

# def check_exists_in_records(identifier, identifier_type, phones_in_records, plates_in_records):
#     """
#     🔥 NEW: Check if a phone or plate exists in the Records sheet
#     Returns True if exists (should be DISCARDED from FAILED)
#     Returns False if not exists (should be SENT to FAILED)
#     """
#     if identifier_type == 'phone':
#         # Normalize the phone number for comparison
#         normalized = normalize_phone_for_comparison(identifier)
#         if normalized and normalized in phones_in_records:
#             print(f"✅ Found phone {identifier} (normalized: {normalized}) in Records - DISCARDING from FAILED")
#             return True
#         return False
    
#     elif identifier_type == 'plate':
#         # Plates are already normalized (uppercase, no spaces)
#         plate_clean = str(identifier).replace(' ', '').upper()
#         if plate_clean in plates_in_records:
#             print(f"✅ Found plate {plate_clean} in Records - DISCARDING from FAILED")
#             return True
#         return False
    
#     return False

# def load_all_customers(service):
#     """Load all customers from pikipiki records sheet into memory (to avoid API quota issues)"""
#     try:
#         sheet = service.spreadsheets()
#         result = sheet.values().get(
#             spreadsheetId=PIKIPIKI_SHEET_ID,
#             range='pikipiki records!A:E'
#         ).execute()
        
#         values = result.get('values', [])
#         if not values:
#             return {}, {}
        
#         # Build lookup dictionaries
#         phone_lookup = {}  # phone -> customer name
#         plate_lookup = {}  # plate -> customer name
        
#         # Skip header row
#         for row in values[1:]:
#             if len(row) < 5:
#                 continue
            
#             # Columns: ID, Plate Number, Customer Name, Phone Number, Customer Name (duplicate?)
#             plate_col = row[1] if len(row) > 1 else ''
#             phone_col = row[3] if len(row) > 3 else ''
#             name_col = row[2] if len(row) > 2 else ''
            
#             # Clean and store plate
#             if plate_col:
#                 plate_clean = str(plate_col).replace(' ', '').upper()
#                 if plate_clean:
#                     plate_lookup[plate_clean] = name_col
            
#             # Clean and store phone
#             if phone_col:
#                 phone_clean = str(phone_col).replace(' ', '').replace('-', '')
#                 if phone_clean:
#                     phone_lookup[phone_clean] = name_col
        
#         print(f"Loaded {len(phone_lookup)} phone numbers and {len(plate_lookup)} plates from pikipiki records")
#         return phone_lookup, plate_lookup
        
#     except Exception as e:
#         print(f"Error loading customers: {e}")
#         return {}, {}

# def lookup_customer_from_cache(identifier, lookup_type, phone_lookup, plate_lookup):
#     """Look up customer from cached data"""
#     if lookup_type == 'phone':
#         # Try exact match first
#         name = phone_lookup.get(identifier)
#         if name:
#             return name
        
#         # If identifier starts with 255, also try 07 format
#         if identifier.startswith('255'):
#             alt_format = '0' + identifier[3:]  # 255752900450 -> 0752900450
#             name = phone_lookup.get(alt_format)
#             if name:
#                 return name
        
#         # If identifier starts with 07, also try 255 format
#         elif identifier.startswith('07'):
#             alt_format = '255' + identifier[1:]  # 0752900450 -> 255752900450
#             name = phone_lookup.get(alt_format)
#             if name:
#                 return name
        
#         # If identifier starts with 06, also try 255 format
#         elif identifier.startswith('06'):
#             alt_format = '255' + identifier[1:]  # 0652900450 -> 255652900450
#             name = phone_lookup.get(alt_format)
#             if name:
#                 return name
        
#         return None
        
#     elif lookup_type == 'plate':
#         return plate_lookup.get(identifier)
#     return None

# def get_existing_refs(service, sheet_name='PASSED'):
#     """
#     🔥 IMPROVED: Get existing reference numbers AND check message details for duplicates
#     Returns both a set of REFs and a set of message details for comprehensive duplicate detection
#     """
#     try:
#         sheet = service.spreadsheets()
        
#         # Read BOTH column D (MESSAGE) and column H (REFNUMBER)
#         if sheet_name == 'FAILED':
#             # FAILED: D=MESSAGE, H=REFNUMBER
#             range_to_read = f'{sheet_name}!D:D,H:H'
#         else:  # PASSED
#             # PASSED: D=MESSAGE, H=REFNUMBER  
#             range_to_read = f'{sheet_name}!D:D,H:H'
        
#         result = service.spreadsheets().values().batchGet(
#             spreadsheetId=PASSED_SHEET_ID,
#             ranges=[f'{sheet_name}!D1:D10000', f'{sheet_name}!H1:H10000']
#         ).execute()
        
#         value_ranges = result.get('valueRanges', [])
        
#         refs = set()
#         messages = set()
        
#         # Process column H (REFNUMBER)
#         if len(value_ranges) > 1:
#             ref_values = value_ranges[1].get('values', [])
#             print(f"📊 {sheet_name} - Column H (REFNUMBER): {len(ref_values)} rows")
            
#             for idx, row in enumerate(ref_values[1:], start=2):  # Skip header
#                 if row and len(row) > 0 and row[0]:
#                     ref = str(row[0]).strip()
#                     if ref and ref.lower() != 'refnumber':
#                         refs.add(ref)
#                         if len(refs) <= 3:  # Show first 3 for debugging
#                             print(f"  ✓ Row {idx} REF: '{ref}'")
        
#         # Process column D (MESSAGE) - extract REF from message as backup
#         if len(value_ranges) > 0:
#             message_values = value_ranges[0].get('values', [])
#             print(f"📊 {sheet_name} - Column D (MESSAGE): {len(message_values)} rows")
            
#             for idx, row in enumerate(message_values[1:], start=2):  # Skip header
#                 if row and len(row) > 0 and row[0]:
#                     message = str(row[0]).strip()
#                     messages.add(message)
                    
#                     # Also extract REF from message as backup
#                     import re
#                     pattern = r'REF:\s*(\S+)'
#                     match = re.search(pattern, message, re.IGNORECASE)
#                     if match:
#                         ref_from_msg = match.group(1)
#                         if ref_from_msg not in refs:
#                             refs.add(ref_from_msg)
#                             if len(refs) <= 5:
#                                 print(f"  ✓ Row {idx} REF from MSG: '{ref_from_msg}'")
        
#         print(f"✅ {sheet_name}: Found {len(refs)} unique REFs, {len(messages)} unique messages")
#         if refs:
#             print(f"   Sample REFs: {list(refs)[:3]}")
        
#         return refs, messages
        
#     except Exception as e:
#         print(f"❌ Error getting existing data from {sheet_name}: {e}")
#         import traceback
#         traceback.print_exc()
#         return set(), set()

# def get_last_id(service, sheet_name):
#     """Get the last ID from the sheet (ignores filters, reads all data)"""
#     try:
#         sheet = service.spreadsheets()
#         # Read ALL values from column A (ID column)
#         result = sheet.values().get(
#             spreadsheetId=PASSED_SHEET_ID,
#             range=f'{sheet_name}!A:A'
#         ).execute()
        
#         values = result.get('values', [])
        
#         if len(values) > 1:
#             # Get the last non-empty ID, starting from the end
#             for row in reversed(values[1:]):  # Skip header
#                 if row and len(row) > 0 and row[0]:
#                     try:
#                         last_id = int(row[0])
#                         print(f"Last ID in {sheet_name}: {last_id}")
#                         return last_id
#                     except (ValueError, TypeError):
#                         continue
        
#         print(f"No existing IDs found in {sheet_name}, starting from 0")
#         return 0
        
#     except Exception as e:
#         print(f"Error getting last ID: {e}")
#         return 0

# def get_last_row_number(service, sheet_name):
#     """Get the actual last row number (works even with filters)"""
#     try:
#         # Get all data from column A to find the true last row
#         result = service.spreadsheets().values().get(
#             spreadsheetId=PASSED_SHEET_ID,
#             range=f'{sheet_name}!A:A'
#         ).execute()
        
#         values = result.get('values', [])
#         # Return the number of rows (including header)
#         return len(values)
#     except Exception as e:
#         print(f"Error getting last row: {e}")
#         return 0

# def append_to_sheet(service, sheet_name, data):
#     """Append data to Google Sheet - WORKS WITH FILTERS"""
#     try:
#         # Get the actual last row number (ignores filters)
#         last_row = get_last_row_number(service, sheet_name)
        
#         # Calculate the starting row for new data
#         start_row = last_row + 1
        
#         # Build the range for new data
#         range_name = f'{sheet_name}!A{start_row}'
        
#         print(f"Attempting to append to {sheet_name} starting at row {start_row}")
#         print(f"Adding {len(data)} rows")
#         print(f"Data preview: {data[0] if data else 'No data'}")
        
#         # Use UPDATE instead of APPEND (works with filters!)
#         result = service.spreadsheets().values().update(
#             spreadsheetId=PASSED_SHEET_ID,
#             range=range_name,
#             valueInputOption='USER_ENTERED',
#             body={'values': data}
#         ).execute()
        
#         print(f"Update result: {result.get('updatedRows', 0)} rows added")
#         return True
        
#     except HttpError as e:
#         print(f"❌ Google Sheets API Error: {e}")
#         print(f"Error details: {e.error_details if hasattr(e, 'error_details') else 'No details'}")
#         if e.resp.status == 403:
#             print("Permission denied! Make sure the service account has Editor access to the sheet.")
#         return False
#     except Exception as e:
#         print(f"❌ Error appending to sheet: {e}")
#         import traceback
#         traceback.print_exc()
#         return False

# @app.route('/')
# def index():
#     return render_template('index.html')

# @app.route('/upload', methods=['POST'])
# def upload_file():
#     if 'file' not in request.files:
#         return jsonify({'error': 'No file uploaded'}), 400
    
#     file = request.files['file']
#     if file.filename == '':
#         return jsonify({'error': 'No file selected'}), 400
    
#     if not file.filename.endswith('.xlsx'):
#         return jsonify({'error': 'Please upload an Excel file (.xlsx)'}), 400
    
#     # Save file
#     filename = secure_filename(file.filename)
#     filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
#     file.save(filepath)
    
#     # Store filepath in session
#     session['filepath'] = filepath
    
#     return jsonify({'success': True, 'message': 'File uploaded successfully'})

# @app.route('/process', methods=['POST'])
# def process_transactions():
#     try:
#         # Get filepath from session
#         filepath = session.get('filepath')
#         if not filepath or not os.path.exists(filepath):
#             return jsonify({'error': 'No file uploaded'}), 400
        
#         # Read Excel file - CRDB format has headers at row 12
#         df = pd.read_excel(filepath, header=12)
        
#         # First row contains the actual column names
#         df.columns = df.iloc[0]
#         df = df[1:].reset_index(drop=True)
        
#         print(f"Columns found: {list(df.columns)}")
        
#         # Now we should have: Posting Date, Details, Value Date, Debit, Credit, Book Balance
#         required_columns = ['Posting Date', 'Details', 'Credit']
#         missing = [col for col in required_columns if col not in df.columns]
        
#         if missing:
#             return jsonify({
#                 'error': f'Missing required columns: {missing}. Found: {list(df.columns)}'
#             }), 400
        
#         # Filter only CREDIT transactions (money coming IN)
#         # Convert Credit column to numeric, handle commas
#         df['Credit'] = pd.to_numeric(df['Credit'].astype(str).str.replace(',', ''), errors='coerce')
#         df['Debit'] = pd.to_numeric(df['Debit'].astype(str).str.replace(',', ''), errors='coerce')
        
#         # Only credit transactions (Credit > 0 and Debit is 0 or NaN)
#         credit_df = df[(df['Credit'].notna()) & (df['Credit'] > 0) & 
#                        ((df['Debit'].isna()) | (df['Debit'] == 0))].copy()
        
#         print(f"Found {len(credit_df)} credit transactions")
        
#         # Initialize Google Sheets service
#         service = get_google_service()
        
#         # Load ALL customers from pikipiki records ONCE (to avoid API quota limits)
#         print("Loading customer database from pikipiki records...")
#         phone_lookup, plate_lookup = load_all_customers(service)
        
#         # 🔥 NEW: Load Records sheet data for validation
#         print("Loading Records sheet for validation...")
#         phones_in_records, plates_in_records = load_records_data(service)
        
#         # 🔥 IMPROVED: Get existing reference numbers AND messages from BOTH PASSED and FAILED sheets
#         print("Loading existing references from PASSED sheet...")
#         existing_passed_refs, existing_passed_messages = get_existing_refs(service, 'PASSED')
        
#         print("Loading existing references from FAILED sheet...")
#         existing_failed_refs, existing_failed_messages = get_existing_refs(service, 'FAILED')
        
#         # 🔥 COMBINE both sets to check against all existing refs AND messages
#         all_existing_refs = existing_passed_refs.union(existing_failed_refs)
#         all_existing_messages = existing_passed_messages.union(existing_failed_messages)
#         print(f"Total unique refs in system: {len(all_existing_refs)} (PASSED: {len(existing_passed_refs)}, FAILED: {len(existing_failed_refs)})")
#         print(f"Total unique messages in system: {len(all_existing_messages)}")
        
#         # Get last IDs
#         last_passed_id = get_last_id(service, 'PASSED')
#         last_failed_id = get_last_id(service, 'FAILED')
        
#         passed_data = []
#         failed_data = []
        
#         stats = {
#             'total': len(credit_df),
#             'passed': 0,
#             'failed': 0,
#             'skipped': 0,
#             'skipped_from_passed': 0,
#             'skipped_from_failed': 0,
#             'discarded_from_records': 0  # 🔥 NEW: Track records found in Records sheet
#         }
        
#         for idx, row in credit_df.iterrows():
#             posting_date = str(row.get('Posting Date', ''))
#             details = str(row.get('Details', ''))
#             credit_amount = row.get('Credit', 0)
            
#             # Extract reference number
#             ref_number = extract_ref_number(details)
            
#             # 🔥 IMPROVED: Check for duplicates using BOTH ref number AND full message
#             is_duplicate = False
#             duplicate_reason = ""
            
#             # Check 1: REF number match
#             if ref_number and ref_number in all_existing_refs:
#                 is_duplicate = True
#                 duplicate_reason = "REF"
#                 if ref_number in existing_passed_refs:
#                     stats['skipped_from_passed'] += 1
#                     print(f"⏭️ DUPLICATE (REF in PASSED): {ref_number}")
#                 else:
#                     stats['skipped_from_failed'] += 1
#                     print(f"⏭️ DUPLICATE (REF in FAILED): {ref_number}")
            
#             # Check 2: Exact message match (backup check)
#             elif details in all_existing_messages:
#                 is_duplicate = True
#                 duplicate_reason = "MESSAGE"
#                 stats['skipped'] += 1
#                 print(f"⏭️ DUPLICATE (MESSAGE): {details[:80]}...")
            
#             if is_duplicate:
#                 stats['skipped'] += 1
#                 continue
            
#             # Try to extract phone number or plate number from details
#             phone = extract_phone_number(details)
#             plate = extract_plate_number(details)
            
#             identifier = None
#             lookup_type = None
            
#             if phone:
#                 identifier = phone
#                 lookup_type = 'phone'
#                 print(f"Found phone: {phone} in: {details[:50]}")
#             elif plate:
#                 identifier = plate
#                 lookup_type = 'plate'
#                 print(f"Found plate: {plate} in: {details[:50]}")
            
#             if identifier and lookup_type:
#                 # Lookup customer name from cached pikipiki records
#                 customer_name = lookup_customer_from_cache(identifier, lookup_type, phone_lookup, plate_lookup)
                
#                 if customer_name:
#                     # Successfully found customer - add to PASSED
#                     last_passed_id += 1
#                     passed_row = [
#                         last_passed_id,        # ID
#                         posting_date,          # Date
#                         'CRDB',                # Channel
#                         details,               # Message (full details)
#                         credit_amount,         # Amount
#                         identifier,            # Plate/Phone
#                         customer_name,         # Name
#                         ref_number or ''       # Ref Number
#                     ]
#                     passed_data.append(passed_row)
#                     stats['passed'] += 1
#                     print(f"✅ PASSED: {customer_name} - {identifier} - {credit_amount}")
#                 else:
#                     # 🔥 NEW: Found phone/plate but no customer match - CHECK RECORDS SHEET FIRST
#                     exists_in_records = check_exists_in_records(identifier, lookup_type, phones_in_records, plates_in_records)
                    
#                     if exists_in_records:
#                         # Found in Records sheet - DISCARD, don't add to FAILED
#                         stats['discarded_from_records'] += 1
#                         print(f"🗑️ DISCARDED: {identifier} found in Records sheet - not adding to FAILED")
#                     else:
#                         # Not in Records sheet - add to FAILED
#                         last_failed_id += 1
#                         reason = f"{lookup_type.upper()}({identifier}) not found"
                        
#                         # Ensure phone has 255 prefix before sending to FAILED sheet
#                         final_identifier = identifier
#                         if lookup_type == 'phone':
#                             if not identifier.startswith('255'):
#                                 # Add 255 prefix
#                                 if identifier.startswith('0'):
#                                     final_identifier = '255' + identifier[1:]
#                                 else:
#                                     final_identifier = '255' + identifier
                        
#                         failed_row = [
#                             last_failed_id,    # ID
#                             posting_date,      # Date
#                             'CRDB',            # Channel
#                             details,           # Message
#                             credit_amount,     # Amount
#                             final_identifier,  # Plate/Phone (with 255 prefix for phones)
#                             reason,            # Reason
#                             ref_number or ''   # REF
#                         ]
#                         failed_data.append(failed_row)
#                         stats['failed'] += 1
#                         print(f"❌ FAILED: Customer not found for {final_identifier} (REF: {ref_number})")
#             else:
#                 # 🔥 NEW: No phone or plate found in details - CHECK IF WE SHOULD DISCARD
#                 # Since there's no identifier, we can't check Records, so add to FAILED
#                 last_failed_id += 1
#                 failed_row = [
#                     last_failed_id,        # ID
#                     posting_date,          # Date
#                     'CRDB',                # Channel
#                     details,               # Message
#                     credit_amount,         # Amount
#                     'No phone/plate',      # Plate/Phone column
#                     'No identifier',       # Reason
#                     ref_number or ''       # REF
#                 ]
#                 failed_data.append(failed_row)
#                 stats['failed'] += 1
#                 print(f"❌ FAILED: No phone/plate found in: {details[:50]} (REF: {ref_number})")
        
#         # Append to Google Sheets
#         if passed_data:
#             print(f"Appending {len(passed_data)} rows to PASSED sheet")
#             try:
#                 result = append_to_sheet(service, 'PASSED', passed_data)
#                 if result:
#                     print(f"✅ Successfully added {len(passed_data)} rows to PASSED")
#                 else:
#                     print(f"❌ Failed to add rows to PASSED")
#                     return jsonify({'error': 'Failed to write to PASSED sheet. Check permissions.'}), 500
#             except Exception as e:
#                 print(f"❌ Error writing to PASSED sheet: {e}")
#                 return jsonify({'error': f'Error writing to PASSED sheet: {str(e)}'}), 500
        
#         if failed_data:
#             print(f"Appending {len(failed_data)} rows to FAILED sheet")
#             try:
#                 result = append_to_sheet(service, 'FAILED', failed_data)
#                 if result:
#                     print(f"✅ Successfully added {len(failed_data)} rows to FAILED")
#                 else:
#                     print(f"❌ Failed to add rows to FAILED")
#                     return jsonify({'error': 'Failed to write to FAILED sheet. Check permissions.'}), 500
#             except Exception as e:
#                 print(f"❌ Error writing to FAILED sheet: {e}")
#                 return jsonify({'error': f'Error writing to FAILED sheet: {str(e)}'}), 500
        
#         # Clean up uploaded file
#         if os.path.exists(filepath):
#             os.remove(filepath)
        
#         # Build detailed message
#         skip_message = f"{stats['skipped']} skipped (duplicates)"
#         if stats['skipped'] > 0:
#             skip_message += f" - {stats['skipped_from_passed']} already in PASSED, {stats['skipped_from_failed']} already in FAILED"
        
#         discard_message = f", {stats['discarded_from_records']} discarded (found in Records sheet)"
        
#         return jsonify({
#             'success': True,
#             'stats': stats,
#             'message': f"Processed {stats['total']} credit transactions: {stats['passed']} passed, {stats['failed']} failed, {skip_message}{discard_message}"
#         })
    
#     except Exception as e:
#         import traceback
#         traceback.print_exc()
#         return jsonify({'error': str(e)}), 500

# @app.route('/check-auth', methods=['GET'])
# def check_auth():
#     """Check if Google Service Account is configured"""
#     try:
#         service = get_google_service()
#         return jsonify({'authenticated': True, 'message': 'Service Account configured'})
#     except Exception as e:
#         return jsonify({'authenticated': False, 'error': str(e)}), 500

# if __name__ == '__main__':
#     app.run(debug=True, host='0.0.0.0', port=5000)














# from flask import Flask, render_template, request, jsonify, session
# from werkzeug.utils import secure_filename
# import os
# import re
# import pandas as pd
# from google.oauth2 import service_account
# from googleapiclient.discovery import build
# from googleapiclient.errors import HttpError
# import json
# from datetime import datetime

# app = Flask(__name__)
# app.secret_key = os.environ.get('SECRET_KEY', 'your-secret-key-change-this')
# app.config['UPLOAD_FOLDER'] = 'uploads'
# app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size

# # Ensure upload folder exists
# os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# # For Render deployment - read credentials from environment
# GOOGLE_CREDS = os.environ.get('GOOGLE_CREDENTIALS')
# if GOOGLE_CREDS:
#     with open('google.json', 'w') as f:
#         f.write(GOOGLE_CREDS)

# # Google Sheets configuration
# SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
# PASSED_SHEET_ID = '1rdSRNLdZPT5xXLRgV7wSn1beYwWZp41ZpYoLkbGmt0o'  # PASSED and FAILED tabs (CORRECT)
# PIKIPIKI_SHEET_ID = '1XFwPITQgZmzZ8lbg8MKD9S4rwHyk2cDOKrcxO7SAjHA'  # pikipiki records
# RECORDS_SHEET_ID = '1rOUnFHVO4MUwCsermoUNu0BDzmCqJTF2'  # Records sheet for validation

# def get_google_service():
#     """Create Google Sheets service using Service Account"""
#     try:
#         credentials = service_account.Credentials.from_service_account_file(
#             'google.json',
#             scopes=SCOPES
#         )
#         service = build('sheets', 'v4', credentials=credentials)
#         return service
#     except Exception as e:
#         print(f"Error creating service: {e}")
#         raise

# def extract_phone_number(text):
#     """Extract phone number from text in formats: 255XXXXXXXXX, 07XXXXXXXX, 06XXXXXXXX"""
#     if not text or pd.isna(text):
#         return None
    
#     text = str(text).replace(' ', '').replace('-', '')
    
#     # Pattern for 255 followed by 9 digits
#     pattern_255 = r'255(\d{9})'
#     match = re.search(pattern_255, text)
#     if match:
#         return f"255{match.group(1)}"
    
#     # Pattern for 07 or 06 followed by 8 digits
#     pattern_07_06 = r'0([67])(\d{8})(?!\d)'
#     match = re.search(pattern_07_06, text)
#     if match:
#         return f"0{match.group(1)}{match.group(2)}"
    
#     return None

# def extract_plate_number(text):
#     """Extract plate number in format: MC###XXX (MC followed by 3 numbers then 3 letters)"""
#     if not text or pd.isna(text):
#         return None
    
#     text = str(text).replace(' ', '').upper()
    
#     # Pattern for MC followed by 3 digits then 3 letters
#     pattern = r'MC(\d{3})([A-Z]{3})'
#     match = re.search(pattern, text)
#     if match:
#         return f"MC{match.group(1)}{match.group(2)}"
    
#     return None

# def extract_ref_number(text):
#     """Extract reference number from message (format: REF:XXXXX)"""
#     if not text or pd.isna(text):
#         return None
    
#     text = str(text)
#     pattern = r'REF:\s*(\S+)'
#     match = re.search(pattern, text, re.IGNORECASE)
#     if match:
#         return match.group(1)
    
#     return None

# def normalize_phone_for_comparison(phone):
#     """
#     Normalize phone number for comparison
#     - Removes 255 prefix if present
#     - Removes 0 prefix if present
#     - Returns just the 9-digit number (e.g., 752900450)
#     """
#     if not phone:
#         return None
    
#     phone = str(phone).replace(' ', '').replace('-', '')
    
#     # Remove 255 prefix
#     if phone.startswith('255'):
#         phone = phone[3:]
    
#     # Remove 0 prefix
#     if phone.startswith('0'):
#         phone = phone[1:]
    
#     # Should now have 9 digits
#     if len(phone) == 9 and phone.isdigit():
#         return phone
    
#     return None

# def load_records_data(service):
#     """
#     🔥 NEW: Load all data from Records sheet (columns D, E, G, H, I, J)
#     Returns sets of normalized phone numbers and plate numbers
#     """
#     try:
#         sheet = service.spreadsheets()
        
#         # Read columns D through J from Records tab
#         result = sheet.values().get(
#             spreadsheetId=RECORDS_SHEET_ID,
#             range='Records!D:J'
#         ).execute()
        
#         values = result.get('values', [])
#         if not values:
#             print("⚠️ No data found in Records sheet")
#             return set(), set()
        
#         plates_in_records = set()
#         phones_in_records = set()
        
#         # Skip header row
#         for row in values[1:]:
#             if len(row) < 1:
#                 continue
            
#             # Column D (index 0) - Plate Number
#             if len(row) > 0 and row[0]:
#                 plate = str(row[0]).replace(' ', '').upper()
#                 if plate.startswith('MC'):
#                     plates_in_records.add(plate)
            
#             # Column E (index 1) - Customer Name (we don't need this for validation)
            
#             # Columns G, H, I, J (indices 3, 4, 5, 6) - Phone Numbers
#             for col_idx in [3, 4, 5, 6]:
#                 if len(row) > col_idx and row[col_idx]:
#                     phone_raw = str(row[col_idx]).replace(' ', '').replace('-', '')
                    
#                     # Normalize phone (remove prefixes, get just 9 digits)
#                     normalized = normalize_phone_for_comparison(phone_raw)
#                     if normalized:
#                         phones_in_records.add(normalized)
        
#         print(f"📋 Loaded Records sheet: {len(plates_in_records)} unique plates, {len(phones_in_records)} unique phones")
#         return phones_in_records, plates_in_records
        
#     except Exception as e:
#         print(f"❌ Error loading Records sheet: {e}")
#         import traceback
#         traceback.print_exc()
#         return set(), set()

# def check_exists_in_records(identifier, identifier_type, phones_in_records, plates_in_records):
#     """
#     🔥 NEW: Check if a phone or plate exists in the Records sheet
#     Returns True if exists (should be DISCARDED from FAILED)
#     Returns False if not exists (should be SENT to FAILED)
#     """
#     if identifier_type == 'phone':
#         # Normalize the phone number for comparison
#         normalized = normalize_phone_for_comparison(identifier)
#         if normalized and normalized in phones_in_records:
#             print(f"✅ Found phone {identifier} (normalized: {normalized}) in Records - DISCARDING from FAILED")
#             return True
#         return False
    
#     elif identifier_type == 'plate':
#         # Plates are already normalized (uppercase, no spaces)
#         plate_clean = str(identifier).replace(' ', '').upper()
#         if plate_clean in plates_in_records:
#             print(f"✅ Found plate {plate_clean} in Records - DISCARDING from FAILED")
#             return True
#         return False
    
#     return False

# def load_all_customers(service):
#     """Load all customers from pikipiki records sheet into memory (to avoid API quota issues)"""
#     try:
#         sheet = service.spreadsheets()
#         result = sheet.values().get(
#             spreadsheetId=PIKIPIKI_SHEET_ID,
#             range='pikipiki records!A:E'
#         ).execute()
        
#         values = result.get('values', [])
#         if not values:
#             return {}, {}
        
#         # Build lookup dictionaries
#         phone_lookup = {}  # phone -> customer name
#         plate_lookup = {}  # plate -> customer name
        
#         # Skip header row
#         for row in values[1:]:
#             if len(row) < 5:
#                 continue
            
#             # Columns: ID, Plate Number, Customer Name, Phone Number, Customer Name (duplicate?)
#             plate_col = row[1] if len(row) > 1 else ''
#             phone_col = row[3] if len(row) > 3 else ''
#             name_col = row[2] if len(row) > 2 else ''
            
#             # Clean and store plate
#             if plate_col:
#                 plate_clean = str(plate_col).replace(' ', '').upper()
#                 if plate_clean:
#                     plate_lookup[plate_clean] = name_col
            
#             # Clean and store phone
#             if phone_col:
#                 phone_clean = str(phone_col).replace(' ', '').replace('-', '')
#                 if phone_clean:
#                     phone_lookup[phone_clean] = name_col
        
#         print(f"Loaded {len(phone_lookup)} phone numbers and {len(plate_lookup)} plates from pikipiki records")
#         return phone_lookup, plate_lookup
        
#     except Exception as e:
#         print(f"Error loading customers: {e}")
#         return {}, {}

# def lookup_customer_from_cache(identifier, lookup_type, phone_lookup, plate_lookup):
#     """Look up customer from cached data"""
#     if lookup_type == 'phone':
#         # Try exact match first
#         name = phone_lookup.get(identifier)
#         if name:
#             return name
        
#         # If identifier starts with 255, also try 07 format
#         if identifier.startswith('255'):
#             alt_format = '0' + identifier[3:]  # 255752900450 -> 0752900450
#             name = phone_lookup.get(alt_format)
#             if name:
#                 return name
        
#         # If identifier starts with 07, also try 255 format
#         elif identifier.startswith('07'):
#             alt_format = '255' + identifier[1:]  # 0752900450 -> 255752900450
#             name = phone_lookup.get(alt_format)
#             if name:
#                 return name
        
#         # If identifier starts with 06, also try 255 format
#         elif identifier.startswith('06'):
#             alt_format = '255' + identifier[1:]  # 0652900450 -> 255652900450
#             name = phone_lookup.get(alt_format)
#             if name:
#                 return name
        
#         return None
        
#     elif lookup_type == 'plate':
#         return plate_lookup.get(identifier)
#     return None

# def get_existing_refs(service, sheet_name='PASSED'):
#     """
#     🔥 UPDATED: Get all existing reference numbers from specified sheet (PASSED or FAILED)
#     This function now works for BOTH sheets to prevent duplicates
#     """
#     try:
#         sheet = service.spreadsheets()
        
#         # 🔥 FIXED: FAILED sheet has REF in column G, PASSED sheet has REF in column H
#         if sheet_name == 'FAILED':
#             ref_column = 'G'  # FAILED: ID, DATE, CHANNEL, MESSAGE, AMOUNT, PLATE/PHONE, REASON (G is 7th column but actually has REFs sometimes)
#         else:  # PASSED
#             ref_column = 'H'  # PASSED: ID, DATE, CHANNEL, MESSAGE, AMOUNT, PLATE/PHONE, NAME, REFNUMBER
        
#         # Read ALL values from the ref column
#         result = sheet.values().get(
#             spreadsheetId=PASSED_SHEET_ID,
#             range=f'{sheet_name}!{ref_column}:{ref_column}'
#         ).execute()
        
#         values = result.get('values', [])
#         refs = set()
        
#         # Skip header and collect all non-empty refs
#         for row in values[1:]:  # Skip header row
#             if row and len(row) > 0 and row[0]:
#                 ref = str(row[0]).strip()
#                 if ref:
#                     refs.add(ref)
        
#         print(f"Found {len(refs)} existing reference numbers in {sheet_name} sheet")
#         return refs
        
#     except Exception as e:
#         print(f"Error getting existing refs from {sheet_name}: {e}")
#         return set()

# def get_last_id(service, sheet_name):
#     """Get the last ID from the sheet (ignores filters, reads all data)"""
#     try:
#         sheet = service.spreadsheets()
#         # Read ALL values from column A (ID column)
#         result = sheet.values().get(
#             spreadsheetId=PASSED_SHEET_ID,
#             range=f'{sheet_name}!A:A'
#         ).execute()
        
#         values = result.get('values', [])
        
#         if len(values) > 1:
#             # Get the last non-empty ID, starting from the end
#             for row in reversed(values[1:]):  # Skip header
#                 if row and len(row) > 0 and row[0]:
#                     try:
#                         last_id = int(row[0])
#                         print(f"Last ID in {sheet_name}: {last_id}")
#                         return last_id
#                     except (ValueError, TypeError):
#                         continue
        
#         print(f"No existing IDs found in {sheet_name}, starting from 0")
#         return 0
        
#     except Exception as e:
#         print(f"Error getting last ID: {e}")
#         return 0

# def get_last_row_number(service, sheet_name):
#     """Get the actual last row number (works even with filters)"""
#     try:
#         # Get all data from column A to find the true last row
#         result = service.spreadsheets().values().get(
#             spreadsheetId=PASSED_SHEET_ID,
#             range=f'{sheet_name}!A:A'
#         ).execute()
        
#         values = result.get('values', [])
#         # Return the number of rows (including header)
#         return len(values)
#     except Exception as e:
#         print(f"Error getting last row: {e}")
#         return 0

# def append_to_sheet(service, sheet_name, data):
#     """Append data to Google Sheet - WORKS WITH FILTERS"""
#     try:
#         # Get the actual last row number (ignores filters)
#         last_row = get_last_row_number(service, sheet_name)
        
#         # Calculate the starting row for new data
#         start_row = last_row + 1
        
#         # Build the range for new data
#         range_name = f'{sheet_name}!A{start_row}'
        
#         print(f"Attempting to append to {sheet_name} starting at row {start_row}")
#         print(f"Adding {len(data)} rows")
#         print(f"Data preview: {data[0] if data else 'No data'}")
        
#         # Use UPDATE instead of APPEND (works with filters!)
#         result = service.spreadsheets().values().update(
#             spreadsheetId=PASSED_SHEET_ID,
#             range=range_name,
#             valueInputOption='USER_ENTERED',
#             body={'values': data}
#         ).execute()
        
#         print(f"Update result: {result.get('updatedRows', 0)} rows added")
#         return True
        
#     except HttpError as e:
#         print(f"❌ Google Sheets API Error: {e}")
#         print(f"Error details: {e.error_details if hasattr(e, 'error_details') else 'No details'}")
#         if e.resp.status == 403:
#             print("Permission denied! Make sure the service account has Editor access to the sheet.")
#         return False
#     except Exception as e:
#         print(f"❌ Error appending to sheet: {e}")
#         import traceback
#         traceback.print_exc()
#         return False

# @app.route('/')
# def index():
#     return render_template('index.html')

# @app.route('/upload', methods=['POST'])
# def upload_file():
#     if 'file' not in request.files:
#         return jsonify({'error': 'No file uploaded'}), 400
    
#     file = request.files['file']
#     if file.filename == '':
#         return jsonify({'error': 'No file selected'}), 400
    
#     if not file.filename.endswith('.xlsx'):
#         return jsonify({'error': 'Please upload an Excel file (.xlsx)'}), 400
    
#     # Save file
#     filename = secure_filename(file.filename)
#     filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
#     file.save(filepath)
    
#     # Store filepath in session
#     session['filepath'] = filepath
    
#     return jsonify({'success': True, 'message': 'File uploaded successfully'})

# @app.route('/process', methods=['POST'])
# def process_transactions():
#     try:
#         # Get filepath from session
#         filepath = session.get('filepath')
#         if not filepath or not os.path.exists(filepath):
#             return jsonify({'error': 'No file uploaded'}), 400
        
#         # Read Excel file - CRDB format has headers at row 12
#         df = pd.read_excel(filepath, header=12)
        
#         # First row contains the actual column names
#         df.columns = df.iloc[0]
#         df = df[1:].reset_index(drop=True)
        
#         print(f"Columns found: {list(df.columns)}")
        
#         # Now we should have: Posting Date, Details, Value Date, Debit, Credit, Book Balance
#         required_columns = ['Posting Date', 'Details', 'Credit']
#         missing = [col for col in required_columns if col not in df.columns]
        
#         if missing:
#             return jsonify({
#                 'error': f'Missing required columns: {missing}. Found: {list(df.columns)}'
#             }), 400
        
#         # Filter only CREDIT transactions (money coming IN)
#         # Convert Credit column to numeric, handle commas
#         df['Credit'] = pd.to_numeric(df['Credit'].astype(str).str.replace(',', ''), errors='coerce')
#         df['Debit'] = pd.to_numeric(df['Debit'].astype(str).str.replace(',', ''), errors='coerce')
        
#         # Only credit transactions (Credit > 0 and Debit is 0 or NaN)
#         credit_df = df[(df['Credit'].notna()) & (df['Credit'] > 0) & 
#                        ((df['Debit'].isna()) | (df['Debit'] == 0))].copy()
        
#         print(f"Found {len(credit_df)} credit transactions")
        
#         # Initialize Google Sheets service
#         service = get_google_service()
        
#         # Load ALL customers from pikipiki records ONCE (to avoid API quota limits)
#         print("Loading customer database from pikipiki records...")
#         phone_lookup, plate_lookup = load_all_customers(service)
        
#         # 🔥 NEW: Load Records sheet data for validation
#         print("Loading Records sheet for validation...")
#         phones_in_records, plates_in_records = load_records_data(service)
        
#         # 🔥 NEW: Get existing reference numbers from BOTH PASSED and FAILED sheets
#         print("Loading existing references from PASSED sheet...")
#         existing_passed_refs = get_existing_refs(service, 'PASSED')
        
#         print("Loading existing references from FAILED sheet...")
#         existing_failed_refs = get_existing_refs(service, 'FAILED')
        
#         # 🔥 COMBINE both sets to check against all existing refs
#         all_existing_refs = existing_passed_refs.union(existing_failed_refs)
#         print(f"Total unique refs already in system: {len(all_existing_refs)} (PASSED: {len(existing_passed_refs)}, FAILED: {len(existing_failed_refs)})")
        
#         # Get last IDs
#         last_passed_id = get_last_id(service, 'PASSED')
#         last_failed_id = get_last_id(service, 'FAILED')
        
#         passed_data = []
#         failed_data = []
        
#         stats = {
#             'total': len(credit_df),
#             'passed': 0,
#             'failed': 0,
#             'skipped': 0,
#             'skipped_from_passed': 0,
#             'skipped_from_failed': 0,
#             'discarded_from_records': 0  # 🔥 NEW: Track records found in Records sheet
#         }
        
#         for idx, row in credit_df.iterrows():
#             posting_date = str(row.get('Posting Date', ''))
#             details = str(row.get('Details', ''))
#             credit_amount = row.get('Credit', 0)
            
#             # Extract reference number
#             ref_number = extract_ref_number(details)
            
#             # Skip if reference already exists in EITHER PASSED or FAILED sheet
#             if ref_number and ref_number in all_existing_refs:
#                 stats['skipped'] += 1
                
#                 # Track where the duplicate came from
#                 if ref_number in existing_passed_refs:
#                     stats['skipped_from_passed'] += 1
#                     print(f"⏭️ Skipping duplicate REF (already in PASSED): {ref_number}")
#                 else:
#                     stats['skipped_from_failed'] += 1
#                     print(f"⏭️ Skipping duplicate REF (already in FAILED): {ref_number}")
                
#                 continue
            
#             # Try to extract phone number or plate number from details
#             phone = extract_phone_number(details)
#             plate = extract_plate_number(details)
            
#             identifier = None
#             lookup_type = None
            
#             if phone:
#                 identifier = phone
#                 lookup_type = 'phone'
#                 print(f"Found phone: {phone} in: {details[:50]}")
#             elif plate:
#                 identifier = plate
#                 lookup_type = 'plate'
#                 print(f"Found plate: {plate} in: {details[:50]}")
            
#             if identifier and lookup_type:
#                 # Lookup customer name from cached pikipiki records
#                 customer_name = lookup_customer_from_cache(identifier, lookup_type, phone_lookup, plate_lookup)
                
#                 if customer_name:
#                     # Successfully found customer - add to PASSED
#                     last_passed_id += 1
#                     passed_row = [
#                         last_passed_id,        # ID
#                         posting_date,          # Date
#                         'CRDB',                # Channel
#                         details,               # Message (full details)
#                         credit_amount,         # Amount
#                         identifier,            # Plate/Phone
#                         customer_name,         # Name
#                         ref_number or ''       # Ref Number
#                     ]
#                     passed_data.append(passed_row)
#                     stats['passed'] += 1
#                     print(f"✅ PASSED: {customer_name} - {identifier} - {credit_amount}")
#                 else:
#                     # 🔥 NEW: Found phone/plate but no customer match - CHECK RECORDS SHEET FIRST
#                     exists_in_records = check_exists_in_records(identifier, lookup_type, phones_in_records, plates_in_records)
                    
#                     if exists_in_records:
#                         # Found in Records sheet - DISCARD, don't add to FAILED
#                         stats['discarded_from_records'] += 1
#                         print(f"🗑️ DISCARDED: {identifier} found in Records sheet - not adding to FAILED")
#                     else:
#                         # Not in Records sheet - add to FAILED
#                         last_failed_id += 1
#                         reason = f"{lookup_type.upper()}({identifier}) not found"
                        
#                         # Ensure phone has 255 prefix before sending to FAILED sheet
#                         final_identifier = identifier
#                         if lookup_type == 'phone':
#                             if not identifier.startswith('255'):
#                                 # Add 255 prefix
#                                 if identifier.startswith('0'):
#                                     final_identifier = '255' + identifier[1:]
#                                 else:
#                                     final_identifier = '255' + identifier
                        
#                         failed_row = [
#                             last_failed_id,    # ID
#                             posting_date,      # Date
#                             'CRDB',            # Channel
#                             details,           # Message
#                             credit_amount,     # Amount
#                             final_identifier,  # Plate/Phone (with 255 prefix for phones)
#                             reason,            # Reason
#                             ref_number or ''   # REF
#                         ]
#                         failed_data.append(failed_row)
#                         stats['failed'] += 1
#                         print(f"❌ FAILED: Customer not found for {final_identifier} (REF: {ref_number})")
#             else:
#                 # 🔥 NEW: No phone or plate found in details - CHECK IF WE SHOULD DISCARD
#                 # Since there's no identifier, we can't check Records, so add to FAILED
#                 last_failed_id += 1
#                 failed_row = [
#                     last_failed_id,        # ID
#                     posting_date,          # Date
#                     'CRDB',                # Channel
#                     details,               # Message
#                     credit_amount,         # Amount
#                     'No phone/plate',      # Plate/Phone column
#                     'No identifier',       # Reason
#                     ref_number or ''       # REF
#                 ]
#                 failed_data.append(failed_row)
#                 stats['failed'] += 1
#                 print(f"❌ FAILED: No phone/plate found in: {details[:50]} (REF: {ref_number})")
        
#         # Append to Google Sheets
#         if passed_data:
#             print(f"Appending {len(passed_data)} rows to PASSED sheet")
#             try:
#                 result = append_to_sheet(service, 'PASSED', passed_data)
#                 if result:
#                     print(f"✅ Successfully added {len(passed_data)} rows to PASSED")
#                 else:
#                     print(f"❌ Failed to add rows to PASSED")
#                     return jsonify({'error': 'Failed to write to PASSED sheet. Check permissions.'}), 500
#             except Exception as e:
#                 print(f"❌ Error writing to PASSED sheet: {e}")
#                 return jsonify({'error': f'Error writing to PASSED sheet: {str(e)}'}), 500
        
#         if failed_data:
#             print(f"Appending {len(failed_data)} rows to FAILED sheet")
#             try:
#                 result = append_to_sheet(service, 'FAILED', failed_data)
#                 if result:
#                     print(f"✅ Successfully added {len(failed_data)} rows to FAILED")
#                 else:
#                     print(f"❌ Failed to add rows to FAILED")
#                     return jsonify({'error': 'Failed to write to FAILED sheet. Check permissions.'}), 500
#             except Exception as e:
#                 print(f"❌ Error writing to FAILED sheet: {e}")
#                 return jsonify({'error': f'Error writing to FAILED sheet: {str(e)}'}), 500
        
#         # Clean up uploaded file
#         if os.path.exists(filepath):
#             os.remove(filepath)
        
#         # Build detailed message
#         skip_message = f"{stats['skipped']} skipped (duplicates)"
#         if stats['skipped'] > 0:
#             skip_message += f" - {stats['skipped_from_passed']} already in PASSED, {stats['skipped_from_failed']} already in FAILED"
        
#         discard_message = f", {stats['discarded_from_records']} discarded (found in Records sheet)"
        
#         return jsonify({
#             'success': True,
#             'stats': stats,
#             'message': f"Processed {stats['total']} credit transactions: {stats['passed']} passed, {stats['failed']} failed, {skip_message}{discard_message}"
#         })
    
#     except Exception as e:
#         import traceback
#         traceback.print_exc()
#         return jsonify({'error': str(e)}), 500

# @app.route('/check-auth', methods=['GET'])
# def check_auth():
#     """Check if Google Service Account is configured"""
#     try:
#         service = get_google_service()
#         return jsonify({'authenticated': True, 'message': 'Service Account configured'})
#     except Exception as e:
#         return jsonify({'authenticated': False, 'error': str(e)}), 500

# if __name__ == '__main__':
#     app.run(debug=True, host='0.0.0.0', port=5000)





# from flask import Flask, render_template, request, jsonify, session
# from werkzeug.utils import secure_filename
# import os
# import re
# import pandas as pd
# from google.oauth2 import service_account
# from googleapiclient.discovery import build
# from googleapiclient.errors import HttpError
# import json
# from datetime import datetime

# app = Flask(__name__)
# app.secret_key = os.environ.get('SECRET_KEY', 'your-secret-key-change-this')
# app.config['UPLOAD_FOLDER'] = 'uploads'
# app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size

# # Ensure upload folder exists
# os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# # For Render deployment - read credentials from environment
# GOOGLE_CREDS = os.environ.get('GOOGLE_CREDENTIALS')
# if GOOGLE_CREDS:
#     with open('google.json', 'w') as f:
#         f.write(GOOGLE_CREDS)

# # Google Sheets configuration
# SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
# PASSED_SHEET_ID = '1rdSRNLdZPT5xXLRgV7wSn1beYwWZp41ZpYoLkbGmt0o'  # PASSED and FAILED tabs (CORRECT)
# PIKIPIKI_SHEET_ID = '1XFwPITQgZmzZ8lbg8MKD9S4rwHyk2cDOKrcxO7SAjHA'  # pikipiki records

# def get_google_service():
#     """Create Google Sheets service using Service Account"""
#     try:
#         credentials = service_account.Credentials.from_service_account_file(
#             'google.json',
#             scopes=SCOPES
#         )
#         service = build('sheets', 'v4', credentials=credentials)
#         return service
#     except Exception as e:
#         print(f"Error creating service: {e}")
#         raise

# def extract_phone_number(text):
#     """Extract phone number from text in formats: 255XXXXXXXXX, 07XXXXXXXX, 06XXXXXXXX"""
#     if not text or pd.isna(text):
#         return None
    
#     text = str(text).replace(' ', '').replace('-', '')
    
#     # Pattern for 255 followed by 9 digits
#     pattern_255 = r'255(\d{9})'
#     match = re.search(pattern_255, text)
#     if match:
#         return f"255{match.group(1)}"
    
#     # Pattern for 07 or 06 followed by 8 digits
#     pattern_07_06 = r'0([67])(\d{8})(?!\d)'
#     match = re.search(pattern_07_06, text)
#     if match:
#         return f"0{match.group(1)}{match.group(2)}"
    
#     return None

# def extract_plate_number(text):
#     """Extract plate number in format: MC###XXX (MC followed by 3 numbers then 3 letters)"""
#     if not text or pd.isna(text):
#         return None
    
#     text = str(text).replace(' ', '').upper()
    
#     # Pattern for MC followed by 3 digits then 3 letters
#     pattern = r'MC(\d{3})([A-Z]{3})'
#     match = re.search(pattern, text)
#     if match:
#         return f"MC{match.group(1)}{match.group(2)}"
    
#     return None

# def extract_ref_number(text):
#     """Extract reference number from message (format: REF:XXXXX)"""
#     if not text or pd.isna(text):
#         return None
    
#     text = str(text)
#     pattern = r'REF:\s*(\S+)'
#     match = re.search(pattern, text, re.IGNORECASE)
#     if match:
#         return match.group(1)
    
#     return None

# def load_all_customers(service):
#     """Load all customers from pikipiki records sheet into memory (to avoid API quota issues)"""
#     try:
#         sheet = service.spreadsheets()
#         result = sheet.values().get(
#             spreadsheetId=PIKIPIKI_SHEET_ID,
#             range='pikipiki records!A:E'
#         ).execute()
        
#         values = result.get('values', [])
#         if not values:
#             return {}, {}
        
#         # Build lookup dictionaries
#         phone_lookup = {}  # phone -> customer name
#         plate_lookup = {}  # plate -> customer name
        
#         # Skip header row
#         for row in values[1:]:
#             if len(row) < 5:
#                 continue
            
#             # Columns: ID, Plate Number, Customer Name, Phone Number, Customer Name (duplicate?)
#             plate_col = row[1] if len(row) > 1 else ''
#             phone_col = row[3] if len(row) > 3 else ''
#             name_col = row[2] if len(row) > 2 else ''
            
#             # Clean and store plate
#             if plate_col:
#                 plate_clean = str(plate_col).replace(' ', '').upper()
#                 if plate_clean:
#                     plate_lookup[plate_clean] = name_col
            
#             # Clean and store phone
#             if phone_col:
#                 phone_clean = str(phone_col).replace(' ', '').replace('-', '')
#                 if phone_clean:
#                     phone_lookup[phone_clean] = name_col
        
#         print(f"Loaded {len(phone_lookup)} phone numbers and {len(plate_lookup)} plates from pikipiki records")
#         return phone_lookup, plate_lookup
        
#     except Exception as e:
#         print(f"Error loading customers: {e}")
#         return {}, {}

# def lookup_customer_from_cache(identifier, lookup_type, phone_lookup, plate_lookup):
#     """Look up customer from cached data"""
#     if lookup_type == 'phone':
#         # Try exact match first
#         name = phone_lookup.get(identifier)
#         if name:
#             return name
        
#         # If identifier starts with 255, also try 07 format
#         if identifier.startswith('255'):
#             alt_format = '0' + identifier[3:]  # 255752900450 -> 0752900450
#             name = phone_lookup.get(alt_format)
#             if name:
#                 return name
        
#         # If identifier starts with 07, also try 255 format
#         elif identifier.startswith('07'):
#             alt_format = '255' + identifier[1:]  # 0752900450 -> 255752900450
#             name = phone_lookup.get(alt_format)
#             if name:
#                 return name
        
#         # If identifier starts with 06, also try 255 format
#         elif identifier.startswith('06'):
#             alt_format = '255' + identifier[1:]  # 0652900450 -> 255652900450
#             name = phone_lookup.get(alt_format)
#             if name:
#                 return name
        
#         return None
        
#     elif lookup_type == 'plate':
#         return plate_lookup.get(identifier)
#     return None

# def get_existing_refs(service, sheet_name='PASSED'):
#     """
#     🔥 UPDATED: Get all existing reference numbers from specified sheet (PASSED or FAILED)
#     This function now works for BOTH sheets to prevent duplicates
#     """
#     try:
#         sheet = service.spreadsheets()
        
#         # 🔥 FIXED: FAILED sheet has REF in column G, PASSED sheet has REF in column H
#         if sheet_name == 'FAILED':
#             ref_column = 'G'  # FAILED: ID, DATE, CHANNEL, MESSAGE, AMOUNT, PLATE/PHONE, REASON (G is 7th column but actually has REFs sometimes)
#         else:  # PASSED
#             ref_column = 'H'  # PASSED: ID, DATE, CHANNEL, MESSAGE, AMOUNT, PLATE/PHONE, NAME, REFNUMBER
        
#         # Read ALL values from the ref column
#         result = sheet.values().get(
#             spreadsheetId=PASSED_SHEET_ID,
#             range=f'{sheet_name}!{ref_column}:{ref_column}'
#         ).execute()
        
#         values = result.get('values', [])
#         refs = set()
        
#         # Skip header and collect all non-empty refs
#         for row in values[1:]:  # Skip header row
#             if row and len(row) > 0 and row[0]:
#                 ref = str(row[0]).strip()
#                 if ref:
#                     refs.add(ref)
        
#         print(f"Found {len(refs)} existing reference numbers in {sheet_name} sheet")
#         return refs
        
#     except Exception as e:
#         print(f"Error getting existing refs from {sheet_name}: {e}")
#         return set()

# def get_last_id(service, sheet_name):
#     """Get the last ID from the sheet (ignores filters, reads all data)"""
#     try:
#         sheet = service.spreadsheets()
#         # Read ALL values from column A (ID column)
#         result = sheet.values().get(
#             spreadsheetId=PASSED_SHEET_ID,
#             range=f'{sheet_name}!A:A'
#         ).execute()
        
#         values = result.get('values', [])
        
#         if len(values) > 1:
#             # Get the last non-empty ID, starting from the end
#             for row in reversed(values[1:]):  # Skip header
#                 if row and len(row) > 0 and row[0]:
#                     try:
#                         last_id = int(row[0])
#                         print(f"Last ID in {sheet_name}: {last_id}")
#                         return last_id
#                     except (ValueError, TypeError):
#                         continue
        
#         print(f"No existing IDs found in {sheet_name}, starting from 0")
#         return 0
        
#     except Exception as e:
#         print(f"Error getting last ID: {e}")
#         return 0

# def get_last_row_number(service, sheet_name):
#     """Get the actual last row number (works even with filters)"""
#     try:
#         # Get all data from column A to find the true last row
#         result = service.spreadsheets().values().get(
#             spreadsheetId=PASSED_SHEET_ID,
#             range=f'{sheet_name}!A:A'
#         ).execute()
        
#         values = result.get('values', [])
#         # Return the number of rows (including header)
#         return len(values)
#     except Exception as e:
#         print(f"Error getting last row: {e}")
#         return 0

# def append_to_sheet(service, sheet_name, data):
#     """Append data to Google Sheet - WORKS WITH FILTERS"""
#     try:
#         # Get the actual last row number (ignores filters)
#         last_row = get_last_row_number(service, sheet_name)
        
#         # Calculate the starting row for new data
#         start_row = last_row + 1
        
#         # Build the range for new data
#         range_name = f'{sheet_name}!A{start_row}'
        
#         print(f"Attempting to append to {sheet_name} starting at row {start_row}")
#         print(f"Adding {len(data)} rows")
#         print(f"Data preview: {data[0] if data else 'No data'}")
        
#         # Use UPDATE instead of APPEND (works with filters!)
#         result = service.spreadsheets().values().update(
#             spreadsheetId=PASSED_SHEET_ID,
#             range=range_name,
#             valueInputOption='USER_ENTERED',
#             body={'values': data}
#         ).execute()
        
#         print(f"Update result: {result.get('updatedRows', 0)} rows added")
#         return True
        
#     except HttpError as e:
#         print(f"❌ Google Sheets API Error: {e}")
#         print(f"Error details: {e.error_details if hasattr(e, 'error_details') else 'No details'}")
#         if e.resp.status == 403:
#             print("Permission denied! Make sure the service account has Editor access to the sheet.")
#         return False
#     except Exception as e:
#         print(f"❌ Error appending to sheet: {e}")
#         import traceback
#         traceback.print_exc()
#         return False

# @app.route('/')
# def index():
#     return render_template('index.html')

# @app.route('/upload', methods=['POST'])
# def upload_file():
#     if 'file' not in request.files:
#         return jsonify({'error': 'No file uploaded'}), 400
    
#     file = request.files['file']
#     if file.filename == '':
#         return jsonify({'error': 'No file selected'}), 400
    
#     if not file.filename.endswith('.xlsx'):
#         return jsonify({'error': 'Please upload an Excel file (.xlsx)'}), 400
    
#     # Save file
#     filename = secure_filename(file.filename)
#     filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
#     file.save(filepath)
    
#     # Store filepath in session
#     session['filepath'] = filepath
    
#     return jsonify({'success': True, 'message': 'File uploaded successfully'})

# @app.route('/process', methods=['POST'])
# def process_transactions():
#     try:
#         # Get filepath from session
#         filepath = session.get('filepath')
#         if not filepath or not os.path.exists(filepath):
#             return jsonify({'error': 'No file uploaded'}), 400
        
#         # Read Excel file - CRDB format has headers at row 12
#         df = pd.read_excel(filepath, header=12)
        
#         # First row contains the actual column names
#         df.columns = df.iloc[0]
#         df = df[1:].reset_index(drop=True)
        
#         print(f"Columns found: {list(df.columns)}")
        
#         # Now we should have: Posting Date, Details, Value Date, Debit, Credit, Book Balance
#         required_columns = ['Posting Date', 'Details', 'Credit']
#         missing = [col for col in required_columns if col not in df.columns]
        
#         if missing:
#             return jsonify({
#                 'error': f'Missing required columns: {missing}. Found: {list(df.columns)}'
#             }), 400
        
#         # Filter only CREDIT transactions (money coming IN)
#         # Convert Credit column to numeric, handle commas
#         df['Credit'] = pd.to_numeric(df['Credit'].astype(str).str.replace(',', ''), errors='coerce')
#         df['Debit'] = pd.to_numeric(df['Debit'].astype(str).str.replace(',', ''), errors='coerce')
        
#         # Only credit transactions (Credit > 0 and Debit is 0 or NaN)
#         credit_df = df[(df['Credit'].notna()) & (df['Credit'] > 0) & 
#                        ((df['Debit'].isna()) | (df['Debit'] == 0))].copy()
        
#         print(f"Found {len(credit_df)} credit transactions")
        
#         # Initialize Google Sheets service
#         service = get_google_service()
        
#         # Load ALL customers from pikipiki records ONCE (to avoid API quota limits)
#         print("Loading customer database from pikipiki records...")
#         phone_lookup, plate_lookup = load_all_customers(service)
        
#         # 🔥 NEW: Get existing reference numbers from BOTH PASSED and FAILED sheets
#         print("Loading existing references from PASSED sheet...")
#         existing_passed_refs = get_existing_refs(service, 'PASSED')
        
#         print("Loading existing references from FAILED sheet...")
#         existing_failed_refs = get_existing_refs(service, 'FAILED')
        
#         # 🔥 COMBINE both sets to check against all existing refs
#         all_existing_refs = existing_passed_refs.union(existing_failed_refs)
#         print(f"Total unique refs already in system: {len(all_existing_refs)} (PASSED: {len(existing_passed_refs)}, FAILED: {len(existing_failed_refs)})")
        
#         # Get last IDs
#         last_passed_id = get_last_id(service, 'PASSED')
#         last_failed_id = get_last_id(service, 'FAILED')
        
#         passed_data = []
#         failed_data = []
        
#         stats = {
#             'total': len(credit_df),
#             'passed': 0,
#             'failed': 0,
#             'skipped': 0,
#             'skipped_from_passed': 0,  # 🔥 NEW: Track where skips came from
#             'skipped_from_failed': 0   # 🔥 NEW: Track where skips came from
#         }
        
#         for idx, row in credit_df.iterrows():
#             posting_date = str(row.get('Posting Date', ''))
#             details = str(row.get('Details', ''))
#             credit_amount = row.get('Credit', 0)
            
#             # Extract reference number
#             ref_number = extract_ref_number(details)
            
#             # 🔥 UPDATED: Skip if reference already exists in EITHER PASSED or FAILED sheet
#             if ref_number and ref_number in all_existing_refs:
#                 stats['skipped'] += 1
                
#                 # Track where the duplicate came from
#                 if ref_number in existing_passed_refs:
#                     stats['skipped_from_passed'] += 1
#                     print(f"⏭️ Skipping duplicate REF (already in PASSED): {ref_number}")
#                 else:
#                     stats['skipped_from_failed'] += 1
#                     print(f"⏭️ Skipping duplicate REF (already in FAILED): {ref_number}")
                
#                 continue
            
#             # Try to extract phone number or plate number from details
#             phone = extract_phone_number(details)
#             plate = extract_plate_number(details)
            
#             identifier = None
#             lookup_type = None
            
#             if phone:
#                 identifier = phone
#                 lookup_type = 'phone'
#                 print(f"Found phone: {phone} in: {details[:50]}")
#             elif plate:
#                 identifier = plate
#                 lookup_type = 'plate'
#                 print(f"Found plate: {plate} in: {details[:50]}")
            
#             if identifier and lookup_type:
#                 # Lookup customer name from cached pikipiki records
#                 customer_name = lookup_customer_from_cache(identifier, lookup_type, phone_lookup, plate_lookup)
                
#                 if customer_name:
#                     # Successfully found customer - add to PASSED
#                     last_passed_id += 1
#                     passed_row = [
#                         last_passed_id,        # ID
#                         posting_date,          # Date
#                         'CRDB',                # Channel
#                         details,               # Message (full details)
#                         credit_amount,         # Amount
#                         identifier,            # Plate/Phone
#                         customer_name,         # Name
#                         ref_number or ''       # Ref Number
#                     ]
#                     passed_data.append(passed_row)
#                     stats['passed'] += 1
#                     print(f"✅ PASSED: {customer_name} - {identifier} - {credit_amount}")
#                 else:
#                     # Found phone/plate but no customer match - add to FAILED
#                     last_failed_id += 1
#                     reason = f"{lookup_type.upper()}({identifier}) not found"
                    
#                     # 🔥 FIXED: FAILED sheet structure
#                     # Columns: ID, DATE, CHANNEL, MESSAGE, AMOUNT, PLATE/PHONE, REASON (with REF in column G or appended)
#                     failed_row = [
#                         last_failed_id,    # ID
#                         posting_date,      # Date
#                         'CRDB',            # Channel
#                         details,           # Message
#                         credit_amount,     # Amount
#                         identifier,        # Plate/Phone
#                         reason,            # Reason (Name column shows reason)
#                         ref_number or ''   # 🔥 NEW: Add REF to FAILED sheet too (column G)
#                     ]
#                     failed_data.append(failed_row)
#                     stats['failed'] += 1
#                     print(f"❌ FAILED: Customer not found for {identifier} (REF: {ref_number})")
#             else:
#                 # No phone or plate found in details - add to FAILED
#                 last_failed_id += 1
#                 failed_row = [
#                     last_failed_id,        # ID
#                     posting_date,          # Date
#                     'CRDB',                # Channel
#                     details,               # Message
#                     credit_amount,         # Amount
#                     'No phone/plate',      # Plate/Phone column
#                     'No identifier',       # Reason
#                     ref_number or ''       # 🔥 NEW: Add REF to FAILED sheet too
#                 ]
#                 failed_data.append(failed_row)
#                 stats['failed'] += 1
#                 print(f"❌ FAILED: No phone/plate found in: {details[:50]} (REF: {ref_number})")
        
#         # Append to Google Sheets
#         if passed_data:
#             print(f"Appending {len(passed_data)} rows to PASSED sheet")
#             try:
#                 result = append_to_sheet(service, 'PASSED', passed_data)
#                 if result:
#                     print(f"✅ Successfully added {len(passed_data)} rows to PASSED")
#                 else:
#                     print(f"❌ Failed to add rows to PASSED")
#                     return jsonify({'error': 'Failed to write to PASSED sheet. Check permissions.'}), 500
#             except Exception as e:
#                 print(f"❌ Error writing to PASSED sheet: {e}")
#                 return jsonify({'error': f'Error writing to PASSED sheet: {str(e)}'}), 500
        
#         if failed_data:
#             print(f"Appending {len(failed_data)} rows to FAILED sheet")
#             try:
#                 result = append_to_sheet(service, 'FAILED', failed_data)
#                 if result:
#                     print(f"✅ Successfully added {len(failed_data)} rows to FAILED")
#                 else:
#                     print(f"❌ Failed to add rows to FAILED")
#                     return jsonify({'error': 'Failed to write to FAILED sheet. Check permissions.'}), 500
#             except Exception as e:
#                 print(f"❌ Error writing to FAILED sheet: {e}")
#                 return jsonify({'error': f'Error writing to FAILED sheet: {str(e)}'}), 500
        
#         # Clean up uploaded file
#         if os.path.exists(filepath):
#             os.remove(filepath)
        
#         # 🔥 UPDATED: More detailed message about skips
#         skip_message = f"{stats['skipped']} skipped (duplicates)"
#         if stats['skipped'] > 0:
#             skip_message += f" - {stats['skipped_from_passed']} already in PASSED, {stats['skipped_from_failed']} already in FAILED"
        
#         return jsonify({
#             'success': True,
#             'stats': stats,
#             'message': f"Processed {stats['total']} credit transactions: {stats['passed']} passed, {stats['failed']} failed, {skip_message}"
#         })
    
#     except Exception as e:
#         import traceback
#         traceback.print_exc()
#         return jsonify({'error': str(e)}), 500

# @app.route('/check-auth', methods=['GET'])
# def check_auth():
#     """Check if Google Service Account is configured"""
#     try:
#         service = get_google_service()
#         return jsonify({'authenticated': True, 'message': 'Service Account configured'})
#     except Exception as e:
#         return jsonify({'authenticated': False, 'error': str(e)}), 500

# if __name__ == '__main__':
#     app.run(debug=True, host='0.0.0.0', port=5000)











