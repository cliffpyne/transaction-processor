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
import pdfplumber  # For PDF extraction
import gc  # For garbage collection

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'dev-secret-key-change-in-production')
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['TEMP_FOLDER'] = 'temp_reviews'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024

# Ensure folders exist
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['TEMP_FOLDER'], exist_ok=True)

# For Render deployment - read credentials from environment
print("🔍 Checking for Google credentials...")
GOOGLE_CREDS = os.environ.get('GOOGLE_CREDENTIALS_JSON')
if GOOGLE_CREDS:
    print("✅ Google credentials found in environment")
else:
    print("⚠️ GOOGLE_CREDENTIALS_JSON not found")


# Google Sheets configuration
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
PASSED_SHEET_ID = '1rdSRNLdZPT5xXLRgV7wSn1beYwWZp41ZpYoLkbGmt0o'
PIKIPIKI_SHEET_ID = '1XFwPITQgZmzZ8lbg8MKD9S4rwHyk2cDOKrcxO7SAjHA'


def extract_data_from_pdf(filepath):
    """
    🔥 ULTRA-OPTIMIZED: Extract transaction data from PDF with minimal memory usage
    Strategy: Process one page at a time, write to temp CSV immediately, never store all in memory
    """
    import csv
    import tempfile
    
    try:
        print(f"📄 Opening PDF: {filepath}")
        
        # Create temporary CSV file to store results progressively
        temp_csv = tempfile.NamedTemporaryFile(mode='w', delete=False, newline='', suffix='.csv')
        csv_writer = csv.DictWriter(temp_csv, fieldnames=['Posting Date', 'Details', 'Credit', 'Debit'])
        csv_writer.writeheader()
        
        transaction_count = 0
        
        # 🔥 CRITICAL: Process ONE page at a time, close immediately
        with pdfplumber.open(filepath) as pdf:
            total_pages = len(pdf.pages)
            print(f"📚 Total pages: {total_pages}")
            
            # 🔥 HARD LIMIT: Reject large PDFs immediately
            MAX_PAGES = 100
            if total_pages > MAX_PAGES:
                temp_csv.close()
                os.unlink(temp_csv.name)
                raise ValueError(f"PDF too large ({total_pages} pages). Maximum allowed: {MAX_PAGES} pages. Please split into smaller files or use Excel format.")
            
            for page_num in range(total_pages):
                print(f"📖 Processing page {page_num + 1}/{total_pages}...")
                
                page = pdf.pages[page_num]
                tables = page.extract_tables()
                
                if not tables:
                    continue
                
                for table in tables:
                    if not table:
                        continue
                    
                    # Find header row
                    header_row_idx = None
                    for idx, row in enumerate(table):
                        if row and any(cell and ('TRANS DATE' in str(cell).upper() or 
                                                   'VALUE DATE' in str(cell).upper() or
                                                   'DETAILS' in str(cell).upper()) for cell in row):
                            header_row_idx = idx
                            break
                    
                    if header_row_idx is None:
                        continue
                    
                    headers = table[header_row_idx]
                    
                    # Map columns
                    col_map = {}
                    for idx, header in enumerate(headers):
                        if not header:
                            continue
                        header_upper = str(header).upper().strip()
                        
                        if 'VALUE DATE' in header_upper:
                            col_map['trans_date'] = idx
                        elif 'TRANS DATE' in header_upper and 'trans_date' not in col_map:
                            col_map['trans_date'] = idx
                        elif 'DETAILS' in header_upper:
                            col_map['details'] = idx
                        elif 'CREDIT' in header_upper:
                            col_map['credit'] = idx
                        elif 'DEBIT' in header_upper:
                            col_map['debit'] = idx
                    
                    if 'trans_date' not in col_map or 'details' not in col_map or 'credit' not in col_map:
                        continue
                    
                    # Process rows - write immediately to CSV
                    for row_idx in range(header_row_idx + 1, len(table)):
                        row = table[row_idx]
                        
                        if not row or len(row) <= max(col_map.values()):
                            continue
                        
                        if all(not cell or str(cell).strip() == '' for cell in row):
                            continue
                        
                        trans_date = str(row[col_map['trans_date']]).strip() if col_map.get('trans_date') is not None and row[col_map['trans_date']] else ''
                        details = str(row[col_map['details']]).strip() if col_map.get('details') is not None and row[col_map['details']] else ''
                        credit_str = str(row[col_map['credit']]).strip() if col_map.get('credit') is not None and row[col_map['credit']] else ''
                        debit_str = str(row[col_map.get('debit', -1)]).strip() if col_map.get('debit') is not None and col_map['debit'] < len(row) and row[col_map['debit']] else ''
                        
                        if not details or not trans_date:
                            continue
                        
                        if 'DETAILS' in details.upper() or 'TRANS DATE' in trans_date.upper():
                            continue
                        
                        # Parse amounts
                        credit_val = 0.0
                        if credit_str:
                            try:
                                credit_val = float(credit_str.replace(',', '').replace(' ', ''))
                            except ValueError:
                                credit_val = 0.0
                        
                        debit_val = 0.0
                        if debit_str:
                            try:
                                debit_val = float(debit_str.replace(',', '').replace(' ', ''))
                            except ValueError:
                                debit_val = 0.0
                        
                        # Only credit transactions
                        if credit_val > 0 and debit_val == 0:
                            csv_writer.writerow({
                                'Posting Date': trans_date,
                                'Details': details,
                                'Credit': credit_val,
                                'Debit': debit_val
                            })
                            transaction_count += 1
                
                # 🔥 CRITICAL: Force garbage collection every 5 pages
                if (page_num + 1) % 5 == 0:
                    gc.collect()
                    print(f"✅ Progress: {transaction_count} transactions extracted so far...")
        
        temp_csv.close()
        
        if transaction_count == 0:
            os.unlink(temp_csv.name)
            print("❌ No transactions found in PDF")
            return None
        
        # 🔥 Now read the CSV into DataFrame (much more memory efficient)
        print(f"📊 Reading {transaction_count} transactions from temp file...")
        df = pd.read_csv(temp_csv.name)
        
        # Clean up temp file
        os.unlink(temp_csv.name)
        
        print(f"✅ Extracted {len(df)} credit transactions from PDF")
        return df
    
    except ValueError as ve:
        print(f"❌ Validation error: {ve}")
        raise
    except Exception as e:
        print(f"❌ Error extracting PDF data: {e}")
        import traceback
        traceback.print_exc()
        return None


def get_google_service():
    """Create Google Sheets service using Service Account"""
    try:
        GOOGLE_CREDS = os.environ.get('GOOGLE_CREDENTIALS_JSON')
        if not GOOGLE_CREDS:
            raise ValueError("GOOGLE_CREDENTIALS_JSON not found")
        
        # 🔍 DEBUG: Check what we're getting
        print(f"📏 Raw env var length: {len(GOOGLE_CREDS)} characters")
        
        creds_dict = json.loads(GOOGLE_CREDS)
        
        # 🔍 DEBUG: Check the private key
        pk = creds_dict.get('private_key', '')
        print(f"🔑 Private key length: {len(pk)} characters")
        print(f"🔑 First 60 chars: {pk[:60]}")
        print(f"🔑 Last 60 chars: {pk[-60:]}")
        print(f"🔑 Contains \\n (literal): {'\\n' in pk}")
        print(f"🔑 Contains actual newlines: {chr(10) in pk}")
        
        # Load credentials
        credentials = service_account.Credentials.from_service_account_info(
            creds_dict,
            scopes=SCOPES
        )
        
        service = build('sheets', 'v4', credentials=credentials)
        return service
        
    except Exception as e:
        print(f"❌ Error creating service: {e}")
        import traceback
        traceback.print_exc()
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
    try:
        print("📤 Upload request received")
        
        if 'file' not in request.files:
            print("❌ No file in request")
            return jsonify({'error': 'No file uploaded'}), 400
        
        file = request.files['file']
        print(f"📁 File received: {file.filename}")
        
        if file.filename == '':
            print("❌ Empty filename")
            return jsonify({'error': 'No file selected'}), 400
        
        # 🔥 UPDATED: Accept both .xlsx and .pdf files (case-insensitive)
        filename_lower = file.filename.lower()
        if not (filename_lower.endswith('.xlsx') or filename_lower.endswith('.pdf')):
            print(f"❌ Invalid file type: {file.filename}")
            return jsonify({'error': f'Please upload an Excel file (.xlsx) or PDF file (.pdf). Got: {file.filename}'}), 400
        
        # 🔥 FILE SIZE CHECK: Strict limit for PDFs (free tier memory constraints)
        file.seek(0, os.SEEK_END)
        file_size = file.tell()
        file.seek(0)
        
        if filename_lower.endswith('.pdf') and file_size > 3 * 1024 * 1024:  # 3MB limit
            size_mb = file_size / (1024 * 1024)
            print(f"❌ PDF file too large: {size_mb:.1f}MB")
            return jsonify({'error': f'PDF file too large ({size_mb:.1f}MB). Maximum: 3MB.\n\nFor large bank statements:\n• Split PDF into smaller files (30-50 pages each)\n• Export as Excel (.xlsx) format instead\n• Use a paid hosting plan for larger files'}), 400
        
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        
        print(f"💾 Saving to: {filepath}")
        file.save(filepath)
        
        # Check if file was saved
        if not os.path.exists(filepath):
            print(f"❌ File not saved: {filepath}")
            return jsonify({'error': 'Failed to save file'}), 500
        
        print(f"✅ File saved successfully: {filename} ({file_size} bytes)")
        
        session['filepath'] = filepath
        
        return jsonify({'success': True, 'message': 'File uploaded successfully'})
    
    except Exception as e:
        print(f"❌ Upload error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': f'Upload failed: {str(e)}'}), 500

@app.route('/process', methods=['POST'])
def process_transactions():
    try:
        filepath = session.get('filepath')
        if not filepath or not os.path.exists(filepath):
            return jsonify({'error': 'No file uploaded'}), 400
        
        # 🔥 NEW: Determine file type and read accordingly
        if filepath.endswith('.pdf'):
            print("📄 Processing PDF file...")
            credit_df = extract_data_from_pdf(filepath)
            
            if credit_df is None or credit_df.empty:
                return jsonify({'error': 'Failed to extract data from PDF or no credit transactions found'}), 400
            
            print(f"✅ PDF: Found {len(credit_df)} credit transactions")
        
        elif filepath.endswith('.xlsx'):
            print("📊 Processing Excel file...")
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
            
            print(f"✅ Excel: Found {len(credit_df)} credit transactions")
        
        else:
            return jsonify({'error': 'Unsupported file format'}), 400
        
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
