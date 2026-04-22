from flask import Flask, render_template, request, jsonify, session
from werkzeug.utils import secure_filename
import os
import re
import gc
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import json
import pickle
from datetime import datetime
import pdfplumber  # For PDF extraction

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'dev-secret-key-change-in-production')
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['TEMP_FOLDER'] = 'temp_reviews'
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB — handles large NMB/CRDB Excel files

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
PASSED_SHEET_ID   = '1rdSRNLdZPT5xXLRgV7wSn1beYwWZp41ZpYoLkbGmt0o'
PIKIPIKI_SHEET_ID = '1XFwPITQgZmzZ8lbg8MKD9S4rwHyk2cDOKrcxO7SAjHA'

# 🔥 NEW: Separate Google Sheet for iPhone customer records
IPHONE_SHEET_ID   = '1Y2cOyObQvP502kvEbC-uGDP-3Sf5X9JKnDDYmR0BPRQ'

# 🔥 NEW: Separate Google Sheet for NMB channel output
NMB_SHEET_ID      = '1YchOygtfVyVNgz37sGX_KKud_Wr9KQsIkQKn_tEdbek'


def _resolve_sheet(sheet_name):
    """
    Returns (target_sheet_id, actual_tab_name) for any logical sheet_name.
    Logical names:
      PASSED_NMB        → NMB_SHEET_ID,   tab PASSED
      PASSED_SAV_NMB    → NMB_SHEET_ID,   tab PASSED_SAV_NMB
      FAILED_NMB        → NMB_SHEET_ID,   tab FAILED_NMB
      PASSED_SAV_NMB_OLD→ PASSED_SHEET_ID, tab PASSED_SAV_NMB  (old data)
      FAILED_NMB_OLD    → PASSED_SHEET_ID, tab FAILED_NMB       (old data)
      BANK_PASSED       → IPHONE_SHEET_ID, tab BANK_PASSED
      BANK_FAILED       → IPHONE_SHEET_ID, tab BANK_FAILED
      everything else   → PASSED_SHEET_ID, same tab name
    """
    if sheet_name in ('BANK_PASSED', 'BANK_FAILED'):
        return IPHONE_SHEET_ID, sheet_name
    elif sheet_name == 'PASSED_NMB':
        return NMB_SHEET_ID, 'PASSED'
    elif sheet_name in ('PASSED_SAV_NMB', 'FAILED_NMB'):
        return NMB_SHEET_ID, sheet_name
    elif sheet_name == 'PASSED_SAV_NMB_OLD':
        return PASSED_SHEET_ID, 'PASSED_SAV_NMB'
    elif sheet_name == 'FAILED_NMB_OLD':
        return PASSED_SHEET_ID, 'FAILED_NMB'
    else:
        return PASSED_SHEET_ID, sheet_name


def extract_nmb_datetime(description, fallback_date_str):
    """
    Extract date and time embedded inside an NMB description.
    Pattern found in descriptions: DDMM HH MM SS
    e.g. '2103 19 32 17'  →  day=21, month=03, time=19:32:17
    Year is taken from fallback_date_str (e.g. '22  Mar 2026').
    Returns: 'DD.MM.YYYY HH:MM:SS'  (same format CRDB uses in the sheet)
    Returns None if pattern not found.
    """
    if not description:
        return None

    match = re.search(r'\b(\d{2})(\d{2})\s+(\d{2})\s+(\d{2})\s+(\d{2})\b', str(description))
    if not match:
        return None

    day     = match.group(1)
    month   = match.group(2)
    hours   = match.group(3)
    minutes = match.group(4)
    seconds = match.group(5)

    # Extract year from fallback date string (e.g. '22  Mar 2026' or '2026-03-22')
    year = None
    if fallback_date_str:
        year_match = re.search(r'\b(20\d{2})\b', str(fallback_date_str))
        if year_match:
            year = year_match.group(1)
    if not year:
        year = str(datetime.now().year)

    return f"{day}.{month}.{year} {hours}:{minutes}:{seconds}"


def extract_data_from_pdf(filepath):
    """
    🔥 NEW: Extract transaction data from PDF bank statement
    PDF format: SN | TRANS DATE | DETAILS | CHANNEL ID | VALUE DATE | DEBIT | CREDIT | BOOK BALANCE
    Returns DataFrame with columns: Posting Date, Details, Credit
    """
    try:
        print(f"📄 Opening PDF: {filepath}")
        transactions = []
        
        with pdfplumber.open(filepath) as pdf:
            for page_num, page in enumerate(pdf.pages, 1):
                print(f"📖 Processing page {page_num}...")
                
                # Extract tables from the page
                tables = page.extract_tables()
                
                if not tables:
                    print(f"⚠️ No tables found on page {page_num}")
                    continue
                
                for table_idx, table in enumerate(tables):
                    if not table:
                        continue
                    
                    print(f"  📊 Table {table_idx + 1}: {len(table)} rows")
                    
                    # Find header row (contains "TRANS DATE" or "SN")
                    header_row_idx = None
                    for idx, row in enumerate(table):
                        if row and any(cell and ('TRANS DATE' in str(cell).upper() or 
                                                   'SN' in str(cell).upper() or 
                                                   'DETAILS' in str(cell).upper()) for cell in row):
                            header_row_idx = idx
                            print(f"  ✓ Found header at row {idx}: {row}")
                            break
                    
                    if header_row_idx is None:
                        print(f"  ⚠️ No header found in table {table_idx + 1}")
                        continue
                    
                    headers = table[header_row_idx]
                    
                    # Map column indices (handle variations in header names)
                    col_map = {}
                    for idx, header in enumerate(headers):
                        if not header:
                            continue
                        header_upper = str(header).upper().strip()
                        
                        if 'TRANS DATE' in header_upper or 'DATE' in header_upper:
                            col_map['trans_date'] = idx
                        elif 'DETAILS' in header_upper:
                            col_map['details'] = idx
                        elif 'CREDIT' in header_upper:
                            col_map['credit'] = idx
                        elif 'DEBIT' in header_upper:
                            col_map['debit'] = idx
                    
                    print(f"  📍 Column mapping: {col_map}")
                    
                    if 'trans_date' not in col_map or 'details' not in col_map or 'credit' not in col_map:
                        print(f"  ⚠️ Missing required columns in table {table_idx + 1}")
                        continue
                    
                    # Process data rows
                    for row_idx in range(header_row_idx + 1, len(table)):
                        row = table[row_idx]
                        
                        if not row or len(row) <= max(col_map.values()):
                            continue
                        
                        # Skip empty rows
                        if all(not cell or str(cell).strip() == '' for cell in row):
                            continue
                        
                        trans_date = row[col_map['trans_date']] if 'trans_date' in col_map else ''
                        details = row[col_map['details']] if 'details' in col_map else ''
                        credit = row[col_map['credit']] if 'credit' in col_map else ''
                        debit = row[col_map.get('debit', -1)] if 'debit' in col_map else ''
                        
                        # Clean up values
                        trans_date = str(trans_date).strip() if trans_date else ''
                        details = str(details).strip() if details else ''
                        credit_str = str(credit).strip() if credit else ''
                        debit_str = str(debit).strip() if debit else ''
                        
                        # Skip if no details or date
                        if not details or not trans_date:
                            continue
                        
                        # Skip header repetitions
                        if 'DETAILS' in details.upper() or 'TRANS DATE' in trans_date.upper():
                            continue
                        
                        # Parse credit amount
                        credit_val = 0.0
                        if credit_str:
                            try:
                                credit_val = float(credit_str.replace(',', '').replace(' ', ''))
                            except ValueError:
                                credit_val = 0.0
                        
                        # Parse debit amount
                        debit_val = 0.0
                        if debit_str:
                            try:
                                debit_val = float(debit_str.replace(',', '').replace(' ', ''))
                            except ValueError:
                                debit_val = 0.0
                        
                        # Only include credit transactions (credit > 0 and debit is 0 or empty)
                        if credit_val > 0 and debit_val == 0:
                            transactions.append({
                                'Posting Date': trans_date,
                                'Details': details,
                                'Credit': credit_val,
                                'Debit': debit_val
                            })
                            print(f"  ✓ Transaction: {trans_date} | {details[:50]}... | Credit: {credit_val}")
        
        if not transactions:
            print("❌ No transactions found in PDF")
            return None
        
        df = pd.DataFrame(transactions)
        print(f"✅ Extracted {len(df)} credit transactions from PDF")
        return df
    
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
    🔥 FIXED: Extract phone number from text
    
    CRITICAL FOR NMB:
    - Skip agency numbers (numbers in "agency @XXXXXXXXXX@" format)
    - ONLY extract numbers that appear AFTER the "Description" keyword
    - This ensures we get the customer's phone, NOT the agent's phone
    - If agency pattern exists but no phone in Description, return None to force plate lookup
    """
    if not text or pd.isna(text):
        return None
    
    original_text = str(text)
    
    # 🔥 CRITICAL FIX: For NMB messages with "agency @", extract ONLY from Description section
    if 'AGENCY' in original_text.upper() and '@' in original_text:
        # Find the Description section
        description_match = re.search(r'DESCRIPTION\s+(.+?)(?:FROM|!!|\Z)', original_text, re.IGNORECASE)
        if description_match:
            description_text = description_match.group(1).strip()
            print(f"  🔍 Searching for phone in Description: {description_text[:60]}...")
            
            # Extract phone from Description section ONLY
            phone = _extract_phone_from_clean_text(description_text.replace(' ', '').replace('-', ''))
            if phone:
                print(f"  ✅ Found customer phone in Description: {phone}")
                return phone
            else:
                print(f"  ⚠️ No phone found in Description section")
        else:
            print(f"  ⚠️ No Description section found in message")
        
        # 🔥 KEY FIX: If we have an agency number pattern, do NOT extract from the full text
        # Return None to force plate lookup instead of using the agency number
        return None
    
    # For non-agency messages, extract normally
    # 🔥 CRITICAL: Do NOT strip spaces — stripping merges adjacent numbers
    # e.g. "501-26506579314150 255775907225" → "50126506579314150255775907225"
    # which makes the (?<!\d) lookbehind fail on the real phone number.
    # Spaces are what keep numbers isolated; the regex handles them fine.
    text_cleaned = original_text
    
    # 🔥 IMPROVED: Exclude account numbers
    if 'FRANKAB' in text_cleaned.upper() or 'TOFRANKAB' in text_cleaned.upper():
        parts = re.split(r'[:\s]+', text_cleaned)
        for part in parts:
            if 'FRANKAB' not in part.upper() and 'FRANK' not in part.upper():
                phone = _extract_phone_from_clean_text(part)
                if phone:
                    return phone
        return None
    
    return _extract_phone_from_clean_text(text_cleaned)

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
    DESCRIPTION BOUNDARY RULE (strict):

    If message contains the word 'Description':
      - ONLY search text AFTER that word (before 'From' / '!!' / end).
      - NEVER look before 'Description'. No fallback to full text.
      - If nothing found after Description → return None immediately.
      This prevents TPS900, Ter ID, agency numbers from being picked up.

    If message has NO 'Description' keyword:
      - Search the entire cleaned message.
      - Rightmost match wins (patterns near the end get priority).
      - Numbers-then-letters patterns have priority over letters-then-numbers.

    Returns normalised plate "MC###XXX" or None.
    """
    if not text or pd.isna(text):
        return None

    original_text = str(text)

    # ── Description boundary present → search ONLY after it ──────────────────
    desc_m = re.search(r'\bDESCRIPTION\b\s+(.+?)(?:\s+FROM\b|!!|$)',
                       original_text, re.IGNORECASE | re.DOTALL)
    if desc_m:
        desc_text = desc_m.group(1).strip()
        print(f"  🔍 Description section: {desc_text[:80]}...")
        plate = _extract_plate_from_text(desc_text)
        if plate:
            print(f"  ✅ Plate from Description: {plate}")
        else:
            print(f"  ⚠️ No plate in Description — not falling back to full text")
        return plate  # None or found — STOP here, never search before Description

    # ── No Description → full cleaned message, rightmost match wins ───────────
    cleaned = _clean_nmb_message(original_text.upper())
    plate = _extract_plate_from_text_rightmost(cleaned)
    if plate:
        print(f"  ✅ Plate from full text (rightmost): {plate}")
    return plate


def _clean_nmb_message(text):
    """
    🔥 NEW: Clean NMB message text to remove false positive patterns
    Removes:
    - "101 - NMB" bank identifier
    - "Ter ID ###XXX" patterns (e.g., "Ter ID 2245105627")
    - "agency @###" patterns
    - "Trx ID" patterns
    """
    # Remove NMB bank identifier
    text = re.sub(r'\d{3}\s*-?\s*NMB\s+(HEAD\s+OFFICE|BANK)?', '', text, flags=re.IGNORECASE)
    
    # 🔥 Remove "Ter ID" followed by any numbers (these create false XXX### patterns)
    text = re.sub(r'TER\s+ID\s+\d+', '', text, flags=re.IGNORECASE)
    
    # 🔥 Remove "agency @" patterns (these create false phone numbers)
    text = re.sub(r'AGENCY\s+@\d+@', '', text, flags=re.IGNORECASE)
    
    # 🔥 Remove "Trx ID" patterns
    text = re.sub(r'TRX\s+ID\s+\w+', '', text, flags=re.IGNORECASE)
    
    return text


def _extract_plate_from_text(text):
    """
    Core plate extraction for a (typically short) text snippet.
    Used on:
      - The Description section of NMB messages (already scoped — no noise)
      - Full cleaned message when no Description keyword exists

    Rules:
      - No word-boundary restriction: plates can be embedded mid-word
        (e.g. kambangaMC264FNN, MC790FCEMaulid).
      - Priority order: MC-prefixed > bare-###XXX > bare-XXX### > 2-letter fallback.
      - Returns the FIRST match at each priority level.
    """
    if not text or pd.isna(text):
        return None
    tu = str(text).upper()
    INVALID = {'NMB', 'TER', 'TRX', 'AGD', 'TPS', 'ACC', 'FRO', 'LTD', 'HEAD', 'OFF'}

    # P1: MC + 3 digits + 3 letters (plate may be mid-word or followed by more letters)
    m = re.search(r'MC[ ]?(\d[ ]?\d[ ]?\d)[ ]?([A-Z]{3})', tu)
    if m:
        d = re.sub(r'\s', '', m.group(1))
        l = m.group(2)
        if l not in INVALID:
            print(f"  ✓ P1 MC###XXX: MC{d}{l}")
            return f"MC{d}{l}"

    # P2: MC + 3 letters + 3 digits
    m = re.search(r'MC[ ]?([A-Z]{3})[ ]?(\d[ ]?\d[ ]?\d)', tu)
    if m:
        l = m.group(1)
        d = re.sub(r'\s', '', m.group(2))
        if l not in INVALID:
            print(f"  ✓ P2 MCXXX###: MC{d}{l}")
            return f"MC{d}{l}"

    # P3: bare 3digits + 3letters (no lookbehind — catches CN607FLW, etc.)
    for m in re.finditer(r'(\d{3})[ ]?([A-Z]{3})(?![A-Z])', tu):
        pos = m.start()
        if pos >= 2 and tu[pos-2:pos] == 'MC':
            continue  # already covered by P1
        l = m.group(2)
        if l not in INVALID:
            print(f"  ✓ P3 ###XXX: MC{m.group(1)}{l}")
            return f"MC{m.group(1)}{l}"

    # P4: bare 3letters + 3digits
    for m in re.finditer(r'(?<![A-Z])([A-Z]{3})[ ]?(\d{3})(?!\d)', tu):
        l = m.group(1)
        pos = m.start()
        if pos >= 2 and tu[pos-2:pos] == 'MC':
            continue
        if l not in INVALID:
            print(f"  ✓ P4 XXX###: MC{m.group(2)}{l}")
            return f"MC{m.group(2)}{l}"

    # P5: MC + 3digits + 2letters fallback (truncated plates like mc266ey, mc628vj)
    m = re.search(r'MC[ ]?(\d{3})[ ]?([A-Z]{2})(?![A-Z])', tu)
    if m:
        print(f"  ✓ P5 MC###XX (2-letter fallback): MC{m.group(1)}{m.group(2)}")
        return f"MC{m.group(1)}{m.group(2)}"

    return None


def _extract_plate_from_text_rightmost(text):
    """
    Like _extract_plate_from_text but returns the RIGHTMOST (last) match.
    Used when there is no Description keyword — we prefer patterns near the
    end of the message.  Numbers-then-letters (priority 1) beat
    letters-then-numbers (priority 2).
    """
    if not text or pd.isna(text):
        return None

    tu = str(text).upper()
    INVALID = {'NMB', 'TER', 'TRX', 'AGD', 'TPS', 'ACC', 'FRO', 'LTD', 'HEAD', 'OFF'}
    all_matches = []  # (position, plate, priority)

    # MC-prefixed — priority 1
    for m in re.finditer(r'MC[ ]?(\d[ ]?\d[ ]?\d)[ ]?([A-Z]{3})', tu):
        l = m.group(2)
        if l not in INVALID:
            d = re.sub(r'\s', '', m.group(1))
            all_matches.append((m.start(), f"MC{d}{l}", 1))

    for m in re.finditer(r'MC[ ]?([A-Z]{3})[ ]?(\d[ ]?\d[ ]?\d)', tu):
        l = m.group(1)
        if l not in INVALID:
            d = re.sub(r'\s', '', m.group(2))
            all_matches.append((m.start(), f"MC{d}{l}", 1))

    # Bare ###XXX — priority 1
    for m in re.finditer(r'(\d{3})[ ]?([A-Z]{3})(?![A-Z])', tu):
        pos = m.start()
        if pos >= 2 and tu[pos-2:pos] == 'MC':
            continue
        l = m.group(2)
        if l not in INVALID:
            all_matches.append((pos, f"MC{m.group(1)}{l}", 1))

    # Bare XXX### — priority 2
    for m in re.finditer(r'(?<![A-Z])([A-Z]{3})[ ]?(\d{3})(?!\d)', tu):
        l = m.group(1)
        pos = m.start()
        if pos >= 2 and tu[pos-2:pos] == 'MC':
            continue
        if l not in INVALID:
            all_matches.append((pos, f"MC{m.group(2)}{l}", 2))

    if not all_matches:
        return None

    # Sort: priority 1 before 2, then rightmost (largest position) first
    all_matches.sort(key=lambda x: (x[2], -x[0]))
    plate = all_matches[0][1]
    print(f"  ✓ rightmost match: {plate}")
    return plate


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


# ── RESCUE: last-resort extraction before any transaction goes to FAILED ──────
# Original logic above is never touched. These functions run only when
# everything above has already returned nothing.

_RESCUE_TIERS = [
    ['MC'],
    ['NC'],
    ['M', 'C'],   # C added — catches c512FCY, c809EXN etc.
    ['MX', 'NX', 'NS', 'MS', 'MN', 'TX', 'MR', 'ML', 'N', 'T'],
]
_RESCUE_SINGLE_LETTER = {'M', 'N', 'T', 'C'}


def _rescue_extract_after_prefix(prefix, text):
    """Find prefix in text, grab next 3-digits+3-letters or reverse.
    Multi-letter prefixes: match anywhere (including mid-word).
    Single-letter prefixes: require non-letter before them."""
    found, seen = [], set()
    lb  = r'(?<![A-Z])' if prefix in _RESCUE_SINGLE_LETTER else ''
    esc = re.escape(prefix)
    for pat, rev in (
        (rf'{lb}{esc}[ ]?(\d[ ]?\d[ ]?\d)[ ]*([A-Z][ ]?[A-Z][ ]?[A-Z])', False),
        (rf'{lb}{esc}[ ]?([A-Z][ ]?[A-Z][ ]?[A-Z])[ ]*(\d[ ]?\d[ ]?\d)', True),
    ):
        for m in re.finditer(pat, text):
            g1 = re.sub(r'\s', '', m.group(1))
            g2 = re.sub(r'\s', '', m.group(2))
            digits, letters = (g2, g1) if rev else (g1, g2)
            if not digits.isdigit() or not letters.isalpha():
                continue
            plate = f"MC{digits}{letters}"
            if plate not in seen:
                seen.add(plate)
                found.append(plate)
    return found


def _rescue_find_plates(text):
    """
    Last-resort plate finder triggered just before a transaction goes to FAILED.

    Description boundary rule (same as extract_plate_number):
      - If 'Description' keyword present → search ONLY the text after it.
        Nothing before Description is considered. Ever.
      - If no 'Description' keyword → search full cleaned message.
        Rightmost / numbers-first patterns get priority.

    Returns [] (nothing found), [plate] (single result → auto-route),
    or [p1, p2, ...] (multiple → ask user to pick).
    """
    if not text or pd.isna(text):
        return []

    original = str(text)

    # ── Respect Description boundary ─────────────────────────────────────────
    desc_m = re.search(r'\bDESCRIPTION\b\s+(.+?)(?:\s+FROM\b|!!|$)',
                       original, re.IGNORECASE | re.DOTALL)
    if desc_m:
        search_text = desc_m.group(1).strip().upper()
        print(f"  🔍 RESCUE: searching Description section only: {search_text[:60]}...")
    else:
        search_text = original.upper()
        # Clean noise only when searching full text
        search_text = re.sub(r'\d{3}\s*-?\s*NMB\s*(HEAD\s*OFFICE|BANK)?', '', search_text)
        search_text = re.sub(r'TER\s+ID\s+\d+', '', search_text)
        search_text = re.sub(r'TRX\s+ID\s+\w+', '', search_text)
        search_text = re.sub(r'AGENCY\s+@\d+@', '', search_text)

    # ── Tier-based prefix search ──────────────────────────────────────────────
    for tier in _RESCUE_TIERS:
        tier_found, tier_seen = [], set()
        for prefix in tier:
            for plate in _rescue_extract_after_prefix(prefix, search_text):
                if plate not in tier_seen:
                    tier_seen.add(plate)
                    tier_found.append(plate)
        if tier_found:
            print(f"  🔍 RESCUE tier hit: {tier_found}")
            return tier_found

    # ── Bare fallback (3+3 without prefix) ───────────────────────────────────
    # Collect ALL matches, sort rightmost + numbers-first for priority
    all_found = []
    seen = set()
    for i, pat in enumerate([
        r'(?<![A-Z\d])(\d[ ]?\d[ ]?\d)[ ]*([A-Z][ ]?[A-Z][ ]?[A-Z])(?![A-Z])',
        r'(?<![A-Z])([A-Z][ ]?[A-Z][ ]?[A-Z])[ ]*(\d[ ]?\d[ ]?\d)(?!\d)',
    ]):
        for m in re.finditer(pat, search_text):
            g1 = re.sub(r'\s', '', m.group(1))
            g2 = re.sub(r'\s', '', m.group(2))
            digits, letters = (g1, g2) if i == 0 else (g2, g1)
            if not digits.isdigit() or not letters.isalpha():
                continue
            plate = f"MC{digits}{letters}"
            if plate not in seen:
                seen.add(plate)
                # Store (position, plate, priority) — priority 1=nums-first, 2=letters-first
                all_found.append((m.start(), plate, i + 1))

    if all_found:
        # Sort: priority 1 before 2, then rightmost position first
        all_found.sort(key=lambda x: (x[2], -x[0]))
        plates = [p for _, p, _ in all_found]
        print(f"  🔍 RESCUE bare fallback: {plates}")
        return plates

    return []


# ═══════════════════════════════════════════════════════════════════════════════
# 🔥 NEW: FUZZY PLATE MATCHER — LAST-RESORT RESCUE BEFORE FAILED
# Runs ONLY after all existing logic (extract_plate_number, _rescue_find_plates)
# has already failed. Finds close matches in pikipiki records / records2
# to catch customer typos like:
#   MC601DXH → MC601EXH  (1-letter suffix typo)
#   MC912ERW → MC912EWR  (suffix anagram/swap)
#   MC367EZ  → MC367EZT  (truncated suffix)
#   MC50EYP  → MC500EYP  (truncated number)
#   MC968EZW → MC969EZW  (1-digit number typo)
# ═══════════════════════════════════════════════════════════════════════════════

def _fuzzy_extract_candidate(text):
    """
    Loose plate extraction used ONLY by the fuzzy matcher.
    Accepts 1-3 digits + 2-3 letters (to catch truncations like MC50EYP, MC367EZ).
    Also accepts reversed order (MC895PFJ type letter-first-digit-after patterns).
    Respects the Description boundary (NMB messages) — only searches after the
    'Description' keyword if present, same as extract_plate_number.
    Returns (number_str, suffix_str) or None.
    """
    if not text or pd.isna(text):
        return None

    original = str(text)

    # Respect Description boundary — same as extract_plate_number
    desc_m = re.search(r'\bDESCRIPTION\b\s+(.+?)(?:\s+FROM\b|!!|$)',
                       original, re.IGNORECASE | re.DOTALL)
    if desc_m:
        search_text = desc_m.group(1).strip().upper()
    else:
        # Clean known noise when searching full text
        search_text = original.upper()
        search_text = re.sub(r'\d{3}\s*-?\s*NMB\s*(HEAD\s*OFFICE|BANK)?', '', search_text)
        search_text = re.sub(r'TER\s+ID\s+\d+', '', search_text)
        search_text = re.sub(r'TRX\s+ID\s+\w+', '', search_text)
        search_text = re.sub(r'AGENCY\s+@\d+@', '', search_text)

    # Pattern A: MC + digits + letters (digits-first)
    m = re.search(r'MC\s*(\d(?:\s*\d){0,2})\s*([A-Z](?:\s*[A-Z]){1,2})(?![A-Z])', search_text)
    if m:
        num = re.sub(r'\s', '', m.group(1))
        suf = re.sub(r'\s', '', m.group(2))
        if 1 <= len(num) <= 3 and 2 <= len(suf) <= 3:
            return (num, suf)

    # Pattern B: MC + letters + digits (letters-first, catches MC895PFJ type)
    m = re.search(r'MC\s*([A-Z](?:\s*[A-Z]){1,2})\s*(\d(?:\s*\d){0,2})(?!\d)', search_text)
    if m:
        suf = re.sub(r'\s', '', m.group(1))
        num = re.sub(r'\s', '', m.group(2))
        if 1 <= len(num) <= 3 and 2 <= len(suf) <= 3:
            return (num, suf)

    return None


def _find_fuzzy_plate_matches(number, suffix, plate_lookup, plate_lookup_sav,
                               id_lookup_sav, max_candidates=15):
    """
    Given an extracted (number, suffix), find close matches in plate DBs.

    Rules applied:
      A: same number, suffix differs by exactly 1 letter        (MC601DXH → MC601EXH)
      B: same number, suffix is anagram of target               (MC912ERW → MC912EWR)
      C: same number, suffix prefix-match for truncations       (MC367EZ  → MC367EZT)
      D: same suffix, number differs by exactly 1 digit         (MC968EZW → MC969EZW)
      E: same suffix, number is truncated (len mismatch)        (MC50EYP  → MC500EYP)

    Returns list of dicts:
      [{'plate': 'MC601EXH', 'name': 'JOHN DOE', 'customer_id': '', 'source': 'records'}, ...]

    Returns [] (triggers fallback to FAILED) if:
      - no candidates found
      - total candidates exceed max_candidates (too ambiguous to auto-rescue)
    """
    if not number or not suffix:
        return []

    # Merge both plate lookups — records1 wins on collision
    all_plates = {}
    for plate, name in plate_lookup.items():
        if plate and plate not in all_plates:
            all_plates[plate] = {'name': name, 'source': 'records', 'customer_id': ''}
    for plate, name in plate_lookup_sav.items():
        if plate and plate not in all_plates:
            cid = (id_lookup_sav or {}).get(plate, '')
            all_plates[plate] = {'name': name, 'source': 'records2', 'customer_id': cid}

    candidates = {}

    def _letter_diff(s1, s2):
        if len(s1) != len(s2):
            return 99
        return sum(1 for a, b in zip(s1, s2) if a != b)

    for plate, info in all_plates.items():
        m = re.match(r'^MC(\d{3})([A-Z]{3})$', plate)
        if not m:
            continue
        pnum, psuf = m.group(1), m.group(2)
        matched = False

        if len(number) == 3 and len(suffix) == 3:
            if pnum == number and psuf != suffix:
                # Rule A: 1-letter suffix typo
                if _letter_diff(suffix, psuf) == 1:
                    matched = True
                # Rule B: suffix anagram (letter swap)
                elif sorted(suffix) == sorted(psuf):
                    matched = True
            elif psuf == suffix and pnum != number:
                # Rule D: 1-digit number typo
                if _letter_diff(number, pnum) == 1:
                    matched = True

        # Rule C: truncated suffix (2 letters instead of 3)
        elif len(suffix) == 2 and len(number) == 3:
            if pnum == number and psuf.startswith(suffix):
                matched = True

        # Rule E: truncated number (1 or 2 digits instead of 3)
        elif len(number) != 3 and len(suffix) == 3:
            if psuf == suffix and (pnum.startswith(number) or number.startswith(pnum)):
                matched = True

        if matched:
            candidates[plate] = info

    # Too ambiguous — don't auto-rescue
    if len(candidates) == 0:
        return []
    if len(candidates) > max_candidates:
        print(f"  🔍 FUZZY: {len(candidates)} candidates exceeds max ({max_candidates}) — skipping rescue")
        return []

    result = [{'plate': p, **info} for p, info in candidates.items()]
    # Stable sort so output is deterministic (alphabetical by plate)
    result.sort(key=lambda c: c['plate'])
    return result


def fuzzy_rescue_to_passed_row(last_passed_id, posting_date, bank, details,
                                credit_amount, ref_number, fuzzy_candidates):
    """
    Build a PASSED row from fuzzy match candidates (matches PASSED schema: 9 cols).
      - Plate column (F): comma-separated plate list
      - Name column (G):  "PLATE=NAME, PLATE=NAME, ..." pairs
      - Customer ID (I):  comma-joined IDs from records2 entries (empty if none)
    """
    plates_str = ', '.join(c['plate'] for c in fuzzy_candidates)
    names_str  = ', '.join(f"{c['plate']}={c['name']}" for c in fuzzy_candidates)
    ids_list   = [c['customer_id'] for c in fuzzy_candidates if c['customer_id']]
    ids_str    = ', '.join(ids_list) if ids_list else ''

    return [
        last_passed_id,
        posting_date,
        bank,
        details,
        credit_amount,
        plates_str,
        names_str,
        ref_number or '',
        ids_str,
    ]


def apply_green_highlight(service, sheet_name, row_indices):
    """
    Apply bright green background (#00ff00) to specified 1-indexed row numbers
    in the given logical sheet. Uses batchUpdate with repeatCell requests.
    row_indices: list of 1-based row numbers (e.g. [152, 153, 154]).
    """
    if not row_indices:
        return

    try:
        target_sheet_id, actual_tab = _resolve_sheet(sheet_name)

        # We need the numeric sheetId (gid) for batchUpdate, not the tab name
        meta = service.spreadsheets().get(spreadsheetId=target_sheet_id).execute()
        tab_gid = None
        for s in meta.get('sheets', []):
            if s['properties']['title'] == actual_tab:
                tab_gid = s['properties']['sheetId']
                break

        if tab_gid is None:
            print(f"  ⚠️ Could not find tab '{actual_tab}' for green highlight")
            return

        requests = []
        for row_1based in row_indices:
            requests.append({
                'repeatCell': {
                    'range': {
                        'sheetId': tab_gid,
                        'startRowIndex': row_1based - 1,
                        'endRowIndex':   row_1based,
                        'startColumnIndex': 0,
                        'endColumnIndex': 9,
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'backgroundColor': {
                                'red':   0.0,
                                'green': 1.0,
                                'blue':  0.0,
                            }
                        }
                    },
                    'fields': 'userEnteredFormat.backgroundColor',
                }
            })

        service.spreadsheets().batchUpdate(
            spreadsheetId=target_sheet_id,
            body={'requests': requests}
        ).execute()
        print(f"  🟢 Applied green highlight to {len(row_indices)} fuzzy-rescued row(s) in {actual_tab}")

    except Exception as e:
        print(f"  ⚠️ Could not apply green highlight: {e}")


def try_fuzzy_rescue(details, plate_lookup, plate_lookup_sav, id_lookup_sav):
    """
    High-level wrapper: given a failing transaction description, try to find
    fuzzy plate matches. Returns list of candidate dicts (possibly empty).

    This is the ONLY function the main processing loops need to call.
    Returns [] when:
      - no plate-like pattern can be extracted
      - extracted plate already exists exactly in DB (shouldn't happen — means
        normal flow missed it — but we guard anyway)
      - no fuzzy matches found
      - too many fuzzy matches (>15) to be confident
    """
    extracted = _fuzzy_extract_candidate(details)
    if not extracted:
        return []

    number, suffix = extracted
    full_plate = f"MC{number}{suffix}"

    # Don't "rescue" a plate that already exists exactly — means something
    # upstream is broken, not a fuzzy case
    if full_plate in plate_lookup or full_plate in plate_lookup_sav:
        print(f"  ⚠️ FUZZY: MC{number}{suffix} already in DB — not a fuzzy case")
        return []

    print(f"  🔎 FUZZY: trying to rescue MC{number}{suffix} (from: {str(details)[:60]})")
    cands = _find_fuzzy_plate_matches(number, suffix, plate_lookup,
                                       plate_lookup_sav, id_lookup_sav)
    if cands:
        print(f"  🟢 FUZZY RESCUE ({len(cands)} candidates): {[c['plate'] for c in cands]}")
    return cands


# ═══════════════════════════════════════════════════════════════════════════════
# End Fuzzy Plate Matcher
# ═══════════════════════════════════════════════════════════════════════════════


def extract_ref_number(text):
    """Extract reference number from message (format: REF:XXXXX or REF XXXXX)"""
    if not text or pd.isna(text):
        return None
    
    text = str(text)
    # 🔥 FIXED: match both REF: and REF (with or without colon)
    # Ref numbers are hex strings of 10+ chars
    pattern = r'REF[:\s]\s*([A-Fa-f0-9]{10,})'
    match = re.search(pattern, text, re.IGNORECASE)
    if match:
        return match.group(1)
    
    return None

# ═══════════════════════════════════════════════════════════════════════════════
# 🔥 NEW: iPhone Channel Functions
# ═══════════════════════════════════════════════════════════════════════════════

def is_iphone_transaction(details):
    """
    Detect whether a transaction description contains the word 'iphone'
    in any capitalisation: iphone, iPhone, IPHONE, Iphone, etc.
    Returns True if it's an iPhone transaction (should bypass normal flow).
    """
    if not details or pd.isna(details):
        return False
    return bool(re.search(r'\biphone\b', str(details), re.IGNORECASE))


def normalize_phone_iphone(phone):
    """
    Normalize any phone number variant to a bare 9-digit suffix
    so we can do format-agnostic matching against IPHONE_RECORDS.

    Handles:
      0752900450    → 752900450
      +255752900450 → 752900450
      255752900450  → 752900450
      752900450     → 752900450   (already bare)
      0752900450,   → 752900450   (trailing comma stripped first)
    Returns None if the result is not 9 digits.
    """
    if not phone:
        return None
    # Strip commas, spaces, dashes, plus signs
    cleaned = re.sub(r'[,\s\-\+]', '', str(phone)).strip()
    if not cleaned:
        return None
    # Remove country code 255
    if cleaned.startswith('255') and len(cleaned) == 12:
        cleaned = cleaned[3:]
    # Remove leading 0
    elif cleaned.startswith('0') and len(cleaned) == 10:
        cleaned = cleaned[1:]
    # Must now be exactly 9 digits
    if len(cleaned) == 9 and cleaned.isdigit():
        return cleaned
    return None


def load_iphone_customers(service):
    """
    Load iPhone customer records from the separate IPHONE_SHEET_ID spreadsheet,
    tab 'IPHONE_RECORDS'.

    Sheet layout:
        Column A → Customer name
        Column B → Phone number 1  (stored as  "0752900450," – trailing comma)
        Column C → Phone number 2  (stored as  "0752900450," – trailing comma)

    Returns:
        iphone_lookup : dict  { '9-digit-normalized-phone' : customer_name }
    """
    iphone_lookup = {}
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=IPHONE_SHEET_ID,
            range='IPHONE_RECORDS!A:C'
        ).execute()

        values = result.get('values', [])
        if not values:
            print("⚠️ No data found in IPHONE_RECORDS tab")
            return iphone_lookup

        for row_idx, row in enumerate(values, start=1):
            # Skip completely empty rows
            if not row:
                continue

            name = str(row[0]).strip() if len(row) > 0 else ''
            phone_b = str(row[1]).strip() if len(row) > 1 else ''
            phone_c = str(row[2]).strip() if len(row) > 2 else ''

            if not name:
                continue  # No name → skip

            for raw_phone in [phone_b, phone_c]:
                if not raw_phone:
                    continue
                normalized = normalize_phone_iphone(raw_phone)
                if normalized:
                    iphone_lookup[normalized] = name

        print(f"✅ iPhone: Loaded {len(iphone_lookup)} phone entries from IPHONE_RECORDS")
    except Exception as e:
        print(f"❌ Error loading IPHONE_RECORDS: {e}")
        import traceback
        traceback.print_exc()

    return iphone_lookup


def extract_phone_for_iphone(details):
    """
    Extract the customer phone number from an iPhone transaction description.

    Uses the same token-based approach as _extract_phone_from_clean_text to
    avoid false positives and the space-strip merge bug:
      - Pre-scrubs known false-positive patterns (agency IDs, Ter IDs, REF numbers)
      - Then delegates to _extract_phone_from_clean_text which tokenises by
        whitespace so adjacent numbers never bleed into each other.

    Returns the raw matched phone string or None.
    """
    if not details or pd.isna(details):
        return None

    cleaned = str(details)

    # ── Scrub known non-phone patterns ────────────────────────────────────────
    # Remove "agency @XXXXXXX@" style agency numbers
    cleaned = re.sub(r'AGENCY\s*@\d+@', '', cleaned, flags=re.IGNORECASE)
    # Remove "Ter ID XXXXXXXX" (long numeric IDs that can look like phones)
    cleaned = re.sub(r'TER\s+ID\s+\d+', '', cleaned, flags=re.IGNORECASE)
    # Remove "Trx ID XXXXXXX"
    cleaned = re.sub(r'TRX\s+ID\s+\w+', '', cleaned, flags=re.IGNORECASE)
    # Remove REF: XXXXXXX (hex ref numbers often start with digits)
    cleaned = re.sub(r'REF:\s*\S+', '', cleaned, flags=re.IGNORECASE)

    # ── Delegate to the shared token-based extractor ───────────────────────────
    return _extract_phone_from_clean_text(cleaned)


def lookup_iphone_customer(details, iphone_lookup):
    """
    Given a transaction description, extract the phone number and look it up
    in the iphone_lookup dict (keyed by 9-digit normalized phone).

    Returns (customer_name, raw_phone_found) or (None, None).
    """
    raw_phone = extract_phone_for_iphone(details)
    if not raw_phone:
        print(f"  📵 iPhone: No phone found in: {details[:80]}")
        return None, None

    normalized = normalize_phone_iphone(raw_phone)
    if not normalized:
        print(f"  📵 iPhone: Could not normalize phone '{raw_phone}'")
        return None, None

    customer_name = iphone_lookup.get(normalized)
    if customer_name:
        print(f"  ✅ iPhone match: {normalized} → {customer_name}")
        return customer_name, raw_phone
    else:
        print(f"  ❌ iPhone: No match for normalized phone '{normalized}' (raw: {raw_phone})")
        return None, raw_phone


# ═══════════════════════════════════════════════════════════════════════════════
# End iPhone Channel Functions
# ═══════════════════════════════════════════════════════════════════════════════


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
    """🔥 UPDATED: Load all customers from pikipiki records2 sheet (for PASSED_SAV) - includes customer IDs"""
    try:
        sheet = service.spreadsheets()
        result = sheet.values().get(
            spreadsheetId=PIKIPIKI_SHEET_ID,
            range='pikipiki records2!A:E'
        ).execute()
        
        values = result.get('values', [])
        if not values:
            print("⚠️ No data found in pikipiki records2")
            return {}, {}, {}
        
        phone_lookup_sav = {}
        plate_lookup_sav = {}
        id_lookup_sav = {}  # 🔥 NEW: Maps phone/plate to customer ID
        
        for row in values[1:]:
            plate_col = row[1] if len(row) > 1 else ''
            phone_col = row[3] if len(row) > 3 else ''
            name_col = row[2] if len(row) > 2 else ''
            customer_id_col = row[4] if len(row) > 4 else ''  # 🔥 NEW: Customer ID from column E (index 4)
            
            if not plate_col and not phone_col:
                continue
            
            if plate_col:
                plate_clean = str(plate_col).replace(' ', '').upper()
                if plate_clean:
                    plate_lookup_sav[plate_clean] = name_col
                    id_lookup_sav[plate_clean] = str(customer_id_col).strip()  # 🔥 NEW: Store customer ID
            
            if phone_col:
                phone_clean = str(phone_col).replace(' ', '').replace('-', '')
                if phone_clean:
                    phone_lookup_sav[phone_clean] = name_col
                    id_lookup_sav[phone_clean] = str(customer_id_col).strip()  # 🔥 NEW: Store customer ID
        
        print(f"✅ Loaded {len(phone_lookup_sav)} phone numbers and {len(plate_lookup_sav)} plates from pikipiki records2 (SAV)")
        print(f"✅ Loaded {len(id_lookup_sav)} customer IDs from pikipiki records2")
        return phone_lookup_sav, plate_lookup_sav, id_lookup_sav
        
    except Exception as e:
        print(f"⚠️ Error loading pikipiki records2 (SAV): {e}")
        return {}, {}, {}

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

def lookup_customer_id_from_cache(identifier, lookup_type, id_lookup_sav):
    """🔥 NEW: Look up customer ID from cached SAV data"""
    if lookup_type == 'phone':
        customer_id = id_lookup_sav.get(identifier)
        if customer_id:
            return customer_id
        
        # Try alternative phone formats
        if identifier.startswith('255'):
            alt_format = '0' + identifier[3:]
            customer_id = id_lookup_sav.get(alt_format)
            if customer_id:
                return customer_id
        
        elif identifier.startswith('07') or identifier.startswith('06'):
            alt_format = '255' + identifier[1:]
            customer_id = id_lookup_sav.get(alt_format)
            if customer_id:
                return customer_id
        
        return ''
        
    elif lookup_type == 'plate':
        return id_lookup_sav.get(identifier, '')
    
    return ''

def get_existing_refs(service, sheet_name='PASSED', refs_only=False):
    """
    Get existing reference numbers AND messages for duplicate detection.
    refs_only=True: skip loading message column entirely (saves memory for large sheets).
    """
    try:
        sheet = service.spreadsheets()
        
        if sheet_name == 'FAILED':
            ref_column = 'I'  # CRDB FAILED has 9 cols, ref in col I
        elif sheet_name in ('FAILED_NMB', 'FAILED_NMB_OLD'):
            ref_column = 'H'  # NMB FAILED has 8 cols, ref in col H
        else:
            ref_column = 'H'
        
        target_sheet_id, actual_tab = _resolve_sheet(sheet_name)
        mode = 'REFS ONLY' if refs_only else 'MESSAGE+REF'
        print(f"📖 Reading {sheet_name} (tab:{actual_tab}): {mode}, REFNUMBER from column {ref_column}")
        
        if refs_only:
            # Only fetch the ref column — skip messages to save memory
            result = service.spreadsheets().values().get(
                spreadsheetId=target_sheet_id,
                range=f'{actual_tab}!{ref_column}:{ref_column}'
            ).execute()
            refs = set()
            messages = set()
            for row in result.get('values', [])[1:]:
                if row and row[0]:
                    ref = str(row[0]).strip()
                    if ref and ref.lower() != 'refnumber':
                        refs.add(ref)
            print(f"✅ {sheet_name}: Found {len(refs)} unique REFs (refs_only mode)")
            return refs, messages

        result = service.spreadsheets().values().batchGet(
            spreadsheetId=target_sheet_id,
            ranges=[
                f'{actual_tab}!D:D',
                f'{actual_tab}!{ref_column}:{ref_column}'
            ]
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
                    
                    pattern = r'REF[:\s]\s*([A-Fa-f0-9]{10,})'
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
        target_sheet_id, actual_tab = _resolve_sheet(sheet_name)
        sheet = service.spreadsheets()
        result = sheet.values().get(
            spreadsheetId=target_sheet_id,
            range=f'{actual_tab}!A:A'
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
        target_sheet_id, actual_tab = _resolve_sheet(sheet_name)
        result = service.spreadsheets().values().get(
            spreadsheetId=target_sheet_id,
            range=f'{actual_tab}!A:A'
        ).execute()
        
        values = result.get('values', [])
        return len(values)
    except Exception as e:
        print(f"Error getting last row: {e}")
        return 0

def append_to_sheet(service, sheet_name, data):
    """Append data to Google Sheet - WORKS WITH FILTERS"""
    try:
        target_sheet_id, actual_tab = _resolve_sheet(sheet_name)
        last_row = get_last_row_number(service, sheet_name)
        start_row = last_row + 1
        range_name = f'{actual_tab}!A{start_row}'
        
        print(f"Attempting to append to {sheet_name} (tab:{actual_tab}) starting at row {start_row}")
        print(f"Adding {len(data)} rows")
        
        result = service.spreadsheets().values().update(
            spreadsheetId=target_sheet_id,
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
        bank_type = request.form.get('bank_type', 'CRDB')  # 🔥 NEW: Get bank type
        
        print(f"📁 File received: {file.filename}, Bank: {bank_type}")
        
        if file.filename == '':
            print("❌ Empty filename")
            return jsonify({'error': 'No file selected'}), 400
        
        # 🔥 UPDATED: Accept both .xlsx and .pdf files (case-insensitive)
        filename_lower = file.filename.lower()
        if not (filename_lower.endswith('.xlsx') or filename_lower.endswith('.xls') or filename_lower.endswith('.pdf')):
            print(f"❌ Invalid file type: {file.filename}")
            return jsonify({'error': f'Please upload an Excel file (.xlsx/.xls) or PDF file (.pdf). Got: {file.filename}'}), 400
        
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        
        print(f"💾 Saving to: {filepath}")
        file.save(filepath)
        
        # Check if file was saved
        if not os.path.exists(filepath):
            print(f"❌ File not saved: {filepath}")
            return jsonify({'error': 'Failed to save file'}), 500
        
        file_size = os.path.getsize(filepath)
        print(f"✅ File saved successfully: {filename} ({file_size} bytes)")
        
        session['filepath'] = filepath
        session['bank_type'] = bank_type  # 🔥 NEW: Store bank type
        
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
        bank_type = session.get('bank_type', 'CRDB')  # 🔥 NEW: Get bank type
        
        if not filepath or not os.path.exists(filepath):
            return jsonify({'error': 'No file uploaded'}), 400
        
        print(f"🏦 Processing {bank_type} statement...")
        
        # 🔥 NEW: Route to appropriate processing function
        if bank_type == 'NMB':
            return process_nmb_transactions(filepath)
        else:
            return process_crdb_transactions(filepath)
    
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


def process_crdb_transactions(filepath):
    """Process CRDB bank statement (existing logic + iPhone channel)"""
    try:
        # Determine file type and read accordingly
        if filepath.endswith('.pdf'):
            print("📄 Processing CRDB PDF file...")
            credit_df = extract_data_from_pdf(filepath)
            
            if credit_df is None or credit_df.empty:
                return jsonify({'error': 'Failed to extract data from PDF or no credit transactions found'}), 400
            
            print(f"✅ PDF: Found {len(credit_df)} credit transactions")
        
        elif filepath.endswith('.xlsx') or filepath.endswith('.xls'):
            print("📊 Processing CRDB Excel file...")
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
            
            # Filter only CREDIT transactions — use .loc to avoid pandas FutureWarning
            df.loc[:, 'Credit'] = pd.to_numeric(df['Credit'].astype(str).str.replace(',', ''), errors='coerce')
            df.loc[:, 'Debit']  = pd.to_numeric(df['Debit'].astype(str).str.replace(',', ''), errors='coerce')

            credit_df = df[(df['Credit'].notna()) & (df['Credit'] > 0) &
                           ((df['Debit'].isna()) | (df['Debit'] == 0))].copy()
            
            # 🔥 Free full df immediately
            del df
            gc.collect()
            
            print(f"✅ Excel: Found {len(credit_df)} credit transactions")
        
        else:
            return jsonify({'error': 'Unsupported file format'}), 400
        
        # Convert to list of dicts so pandas DataFrame can be freed early
        transactions_list = credit_df.to_dict('records')
        del credit_df
        gc.collect()
        print(f"✅ Converted {len(transactions_list)} transactions to list, freed DataFrame")

        # Initialize Google Sheets service
        service = get_google_service()
        
        # ── Load customer lookups ──────────────────────────────────────────────
        print("Loading customer database from pikipiki records...")
        phone_lookup, plate_lookup = load_all_customers(service)
        
        print("\nLoading customer database from pikipiki records2 (SAV)...")
        phone_lookup_sav, plate_lookup_sav, id_lookup_sav = load_all_customers_sav(service)

        # 🔥 NEW: Load iPhone customer lookup from separate sheet
        print("\nLoading iPhone customer database from IPHONE_RECORDS...")
        iphone_lookup = load_iphone_customers(service)
        
        # ── Load existing refs (duplicate guard) ──────────────────────────────
        print("Loading existing references from PASSED sheet...")
        existing_passed_refs, existing_passed_messages = get_existing_refs(service, 'PASSED')
        
        print("Loading existing references from PASSED_SAV sheet...")
        existing_passed_sav_refs, existing_passed_sav_messages = get_existing_refs(service, 'PASSED_SAV')
        
        print("Loading existing references from FAILED sheet...")
        existing_failed_refs, existing_failed_messages = get_existing_refs(service, 'FAILED')

        # 🔥 NEW: Load existing refs for iPhone sheets
        print("Loading existing references from BANK_PASSED sheet...")
        existing_bank_passed_refs, existing_bank_passed_messages = get_existing_refs(service, 'BANK_PASSED')

        print("Loading existing references from BANK_FAILED sheet...")
        existing_bank_failed_refs, existing_bank_failed_messages = get_existing_refs(service, 'BANK_FAILED')
        
        # 🔥 iPhone duplicate sets
        all_iphone_existing_refs = existing_bank_passed_refs.union(existing_bank_failed_refs)
        all_iphone_existing_messages = existing_bank_passed_messages.union(existing_bank_failed_messages)

        # 🔥 CRITICAL FIX: include BANK_PASSED + BANK_FAILED in the main dup check
        # so transactions already written to iPhone sheets are caught at the top
        # of the loop and never fall through to pikipiki lookup → FAILED
        all_existing_refs = (
            existing_passed_refs
            .union(existing_passed_sav_refs)
            .union(existing_failed_refs)
            .union(all_iphone_existing_refs)
        )
        all_existing_messages = (
            existing_passed_messages
            .union(existing_passed_sav_messages)
            .union(existing_failed_messages)
            .union(all_iphone_existing_messages)
        )

        print(f"Total unique refs in system (normal): {len(all_existing_refs)}")
        print(f"Total unique refs in system (iPhone): {len(all_iphone_existing_refs)}")

        # 🔥 Free individual sets — merged sets are all we need
        del existing_passed_refs, existing_passed_messages
        del existing_passed_sav_refs, existing_passed_sav_messages
        del existing_failed_refs, existing_failed_messages
        del existing_bank_passed_refs, existing_bank_passed_messages
        del existing_bank_failed_refs, existing_bank_failed_messages
        # all_iphone_existing_messages kept — still needed in the loop (small, ~few hundred entries)
        gc.collect()
        
        # ── Get last IDs ───────────────────────────────────────────────────────
        last_passed_id     = get_last_id(service, 'PASSED')
        last_passed_sav_id = get_last_id(service, 'PASSED_SAV')
        last_failed_id     = get_last_id(service, 'FAILED')

        # 🔥 NEW: Last IDs for iPhone sheets
        last_bank_passed_id = get_last_id(service, 'BANK_PASSED')
        last_bank_failed_id = get_last_id(service, 'BANK_FAILED')
        
        # ── Row buckets ────────────────────────────────────────────────────────
        passed_data      = []
        passed_sav_data  = []
        failed_data      = []
        needs_review_data = []

        # 🔥 NEW: iPhone buckets
        bank_passed_data = []
        bank_failed_data = []

        # 🔥 NEW: Fuzzy-rescued rows — written to PASSED with green highlight
        fuzzy_passed_data = []

        stats = {
            'total': len(transactions_list),
            'passed': 0,
            'passed_sav': 0,
            'failed': 0,
            'needs_review': 0,
            'skipped': 0,
            'skipped_from_passed': 0,
            'skipped_from_passed_sav': 0,
            'skipped_from_failed': 0,
            # 🔥 NEW: iPhone stats
            'iphone_passed': 0,
            'iphone_failed': 0,
            'iphone_skipped': 0,
            # 🔥 NEW: Fuzzy stats
            'fuzzy_rescued': 0,
        }
        
        for row in transactions_list:
            posting_date  = str(row.get('Posting Date', ''))
            details       = str(row.get('Details', ''))
            credit_amount = row.get('Credit', 0)
            ref_number    = extract_ref_number(details)

            # ══════════════════════════════════════════════════════════════════
            # 🔥 NEW: iPhone Channel — intercept BEFORE normal processing
            # ══════════════════════════════════════════════════════════════════
            if is_iphone_transaction(details):
                print(f"\n📱 iPhone transaction detected: {details[:80]}")

                # Duplicate check within iPhone sheets
                iphone_is_dup = False
                if ref_number and ref_number in all_iphone_existing_refs:
                    iphone_is_dup = True
                elif details in all_iphone_existing_messages:
                    iphone_is_dup = True

                if iphone_is_dup:
                    stats['iphone_skipped'] += 1
                    stats['skipped'] += 1
                    print(f"  ⏭️ iPhone duplicate — skipped")
                    continue  # Do NOT fall through to normal flow

                # Look up customer in IPHONE_RECORDS
                customer_name, raw_phone = lookup_iphone_customer(details, iphone_lookup)

                # Determine display identifier (prefer 255-prefix format)
                if raw_phone:
                    norm = normalize_phone_iphone(raw_phone)
                    display_phone = f"255{norm}" if norm else raw_phone
                else:
                    display_phone = 'No phone'

                if customer_name:
                    # ✅ Match found → BANK_PASSED
                    last_bank_passed_id += 1
                    bank_passed_row = [
                        last_bank_passed_id,
                        posting_date,
                        'CRDB',
                        details,
                        credit_amount,
                        display_phone,
                        customer_name,
                        ref_number or '',
                        ''          # No separate customer_id in IPHONE_RECORDS
                    ]
                    bank_passed_data.append(bank_passed_row)
                    stats['iphone_passed'] += 1
                    print(f"  ✅ BANK_PASSED: {customer_name} — {display_phone} — {credit_amount}")
                else:
                    # ❌ No match → BANK_FAILED
                    last_bank_failed_id += 1
                    reason = f"PHONE({display_phone}) not found in IPHONE_RECORDS"
                    bank_failed_row = [
                        last_bank_failed_id,
                        posting_date,
                        'CRDB',
                        details,
                        credit_amount,
                        display_phone,
                        reason,
                        ref_number or ''
                    ]
                    bank_failed_data.append(bank_failed_row)
                    stats['iphone_failed'] += 1
                    print(f"  ❌ BANK_FAILED: {reason}")

                # ⚠️ CRITICAL: continue — do NOT run normal pikipiki logic
                continue
            # ══════════════════════════════════════════════════════════════════
            # End iPhone Channel
            # ══════════════════════════════════════════════════════════════════

            # ── Normal duplicate check ─────────────────────────────────────────
            is_duplicate = False

            if ref_number and ref_number in all_existing_refs:
                is_duplicate = True
                stats['skipped'] += 1
            elif details in all_existing_messages:
                is_duplicate = True
                stats['skipped'] += 1

            if is_duplicate:
                continue
            
            # ── Extract phone and plate ────────────────────────────────────────
            phone = extract_phone_number(details)
            plate = extract_plate_number(details)
            
            identifier  = None
            lookup_type = None
            
            if phone:
                identifier  = phone
                lookup_type = 'phone'
                print(f"Found phone: {phone} in: {details[:80]}")
            elif plate:
                identifier  = plate
                lookup_type = 'plate'
                print(f"Found plate: {plate} in: {details[:80]}")
            
            if identifier and lookup_type:
                # Check pikipiki records first
                customer_name = lookup_customer_from_cache(identifier, lookup_type, phone_lookup, plate_lookup)
                
                if customer_name:
                    # Add to PASSED
                    last_passed_id += 1
                    passed_row = [
                        last_passed_id,
                        posting_date,
                        'CRDB',
                        details,
                        credit_amount,
                        identifier,
                        customer_name,
                        ref_number or '',
                        ''  # Empty customer_id for PASSED
                    ]
                    passed_data.append(passed_row)
                    stats['passed'] += 1
                    print(f"✅ PASSED: {customer_name} - {identifier} - {credit_amount}")
                else:
                    # Check pikipiki records2 (SAV)
                    customer_name_sav = lookup_customer_from_cache(identifier, lookup_type, phone_lookup_sav, plate_lookup_sav)
                    
                    if customer_name_sav:
                        # Get customer ID for PASSED_SAV records
                        customer_id = lookup_customer_id_from_cache(identifier, lookup_type, id_lookup_sav)
                        
                        last_passed_sav_id += 1
                        passed_sav_row = [
                            last_passed_sav_id,
                            posting_date,
                            'CRDB',
                            details,
                            credit_amount,
                            identifier,
                            customer_name_sav,
                            ref_number or '',
                            customer_id
                        ]
                        passed_sav_data.append(passed_sav_row)
                        stats['passed_sav'] += 1
                        print(f"✅ PASSED_SAV: {customer_name_sav} - {identifier} - {credit_amount} - ID: {customer_id}")
                    else:
                        # 🔥 Tier 3: Not in pikipiki records1 or records2.
                        # If we have a phone, try IPHONE_RECORDS before giving up.
                        iphone_matched = False
                        if lookup_type == 'phone':
                            norm = normalize_phone_iphone(identifier)
                            iphone_customer = iphone_lookup.get(norm) if norm else None
                            if iphone_customer:
                                # Found in IPHONE_RECORDS → BANK_PASSED
                                # Duplicate check first
                                iphone_is_dup = (
                                    (ref_number and ref_number in all_iphone_existing_refs)
                                    or details in all_iphone_existing_messages
                                )
                                if not iphone_is_dup:
                                    norm_phone = identifier if identifier.startswith('255') else f"255{norm}"
                                    last_bank_passed_id += 1
                                    bank_passed_row = [
                                        last_bank_passed_id,
                                        posting_date,
                                        'CRDB',
                                        details,
                                        credit_amount,
                                        norm_phone,
                                        iphone_customer,
                                        ref_number or '',
                                        ''
                                    ]
                                    bank_passed_data.append(bank_passed_row)
                                    stats['iphone_passed'] += 1
                                    iphone_matched = True
                                    print(f"  ✅ BANK_PASSED (via phone fallback): {iphone_customer} — {norm_phone} — {credit_amount}")

                        if not iphone_matched:
                            # ── FUZZY RESCUE attempt before giving up ─────────
                            # Only for plate failures, not phone failures
                            fuzzy_cands = []
                            if lookup_type == 'plate':
                                fuzzy_cands = try_fuzzy_rescue(details, plate_lookup,
                                                               plate_lookup_sav, id_lookup_sav)

                            if fuzzy_cands:
                                last_passed_id += 1
                                fuzzy_row = fuzzy_rescue_to_passed_row(
                                    last_passed_id, posting_date, 'CRDB', details,
                                    credit_amount, ref_number, fuzzy_cands
                                )
                                fuzzy_passed_data.append(fuzzy_row)
                                stats['fuzzy_rescued'] += 1
                                print(f"  🟢 FUZZY→PASSED: {len(fuzzy_cands)} candidate(s) written green")
                                continue  # move to next transaction

                            # Truly not found anywhere — add to FAILED
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
                # Check for plate suggestions (original logic)
                plate_suggestions = extract_plate_suggestions(details)
                
                if plate_suggestions:
                    for suggestion in plate_suggestions:
                        suggested_plate = suggestion['suggested']
                        
                        customer_name = lookup_customer_from_cache(suggested_plate, 'plate', phone_lookup, plate_lookup)
                        customer_name_sav = None
                        customer_id = ''
                        
                        if not customer_name:
                            customer_name_sav = lookup_customer_from_cache(suggested_plate, 'plate', phone_lookup_sav, plate_lookup_sav)
                            if customer_name_sav:
                                customer_id = lookup_customer_id_from_cache(suggested_plate, 'plate', id_lookup_sav)
                        
                        if customer_name or customer_name_sav:
                            needs_review_data.append({
                                'posting_date': posting_date,
                                'details': details,
                                'credit_amount': credit_amount,
                                'ref_number': ref_number or '',
                                'original_text': suggestion['original'],
                                'suggested_plate': suggested_plate,
                                'customer_name': customer_name or customer_name_sav,
                                'customer_id': customer_id,
                                'target_sheet': 'PASSED' if customer_name else 'PASSED_SAV',
                                'confidence': suggestion['confidence'],
                                'reason': suggestion['reason'],
                                'bank': 'CRDB'
                            })
                            stats['needs_review'] += 1
                            print(f"🔍 NEEDS REVIEW: {suggestion['original']} -> {suggested_plate} -> {customer_name or customer_name_sav}")
                            break
                    
                    if not needs_review_data or needs_review_data[-1]['details'] != details:
                        # ── RESCUE before FAILED ──────────────────────────────
                        rescue_plates = _rescue_find_plates(details)
                        if rescue_plates:
                            candidate_details = []
                            for rp in rescue_plates:
                                cn = lookup_customer_from_cache(rp, 'plate', phone_lookup, plate_lookup)
                                cn_sav, cid = None, ''
                                if not cn:
                                    cn_sav = lookup_customer_from_cache(rp, 'plate', phone_lookup_sav, plate_lookup_sav)
                                    if cn_sav:
                                        cid = lookup_customer_id_from_cache(rp, 'plate', id_lookup_sav)
                                candidate_details.append({'plate': rp, 'customer_name': cn or cn_sav or '', 'customer_id': cid, 'target_sheet': 'PASSED' if cn else ('PASSED_SAV' if cn_sav else None)})
                            needs_review_data.append({'posting_date': posting_date, 'details': details, 'credit_amount': credit_amount, 'ref_number': ref_number or '', 'review_type': 'choose_plate', 'candidates': candidate_details, 'bank': 'CRDB'})
                            stats['needs_review'] += 1
                            print(f"🔍 RESCUE REVIEW (CRDB): {[c['plate'] for c in candidate_details]}")
                        else:
                            # ── FUZZY RESCUE before FAILED ─────────────────────
                            fuzzy_cands = try_fuzzy_rescue(details, plate_lookup,
                                                           plate_lookup_sav, id_lookup_sav)
                            if fuzzy_cands:
                                last_passed_id += 1
                                fuzzy_row = fuzzy_rescue_to_passed_row(
                                    last_passed_id, posting_date, 'CRDB', details,
                                    credit_amount, ref_number, fuzzy_cands
                                )
                                fuzzy_passed_data.append(fuzzy_row)
                                stats['fuzzy_rescued'] += 1
                                print(f"  🟢 FUZZY→PASSED: {len(fuzzy_cands)} candidate(s) written green")
                            else:
                                last_failed_id += 1
                                failed_data.append([last_failed_id, posting_date, 'CRDB', details, credit_amount, 'No phone/plate', 'No identifier', ref_number or ''])
                                stats['failed'] += 1
                else:
                    # ── RESCUE before FAILED ──────────────────────────────────
                    rescue_plates = _rescue_find_plates(details)
                    if rescue_plates:
                        candidate_details = []
                        for rp in rescue_plates:
                            cn = lookup_customer_from_cache(rp, 'plate', phone_lookup, plate_lookup)
                            cn_sav, cid = None, ''
                            if not cn:
                                cn_sav = lookup_customer_from_cache(rp, 'plate', phone_lookup_sav, plate_lookup_sav)
                                if cn_sav:
                                    cid = lookup_customer_id_from_cache(rp, 'plate', id_lookup_sav)
                            candidate_details.append({'plate': rp, 'customer_name': cn or cn_sav or '', 'customer_id': cid, 'target_sheet': 'PASSED' if cn else ('PASSED_SAV' if cn_sav else None)})
                        needs_review_data.append({'posting_date': posting_date, 'details': details, 'credit_amount': credit_amount, 'ref_number': ref_number or '', 'review_type': 'choose_plate', 'candidates': candidate_details, 'bank': 'CRDB'})
                        stats['needs_review'] += 1
                        print(f"🔍 RESCUE REVIEW (CRDB): {[c['plate'] for c in candidate_details]}")
                    else:
                        # ── FUZZY RESCUE before FAILED ─────────────────────────
                        fuzzy_cands = try_fuzzy_rescue(details, plate_lookup,
                                                       plate_lookup_sav, id_lookup_sav)
                        if fuzzy_cands:
                            last_passed_id += 1
                            fuzzy_row = fuzzy_rescue_to_passed_row(
                                last_passed_id, posting_date, 'CRDB', details,
                                credit_amount, ref_number, fuzzy_cands
                            )
                            fuzzy_passed_data.append(fuzzy_row)
                            stats['fuzzy_rescued'] += 1
                            print(f"  🟢 FUZZY→PASSED: {len(fuzzy_cands)} candidate(s) written green")
                        else:
                            last_failed_id += 1
                            failed_data.append([last_failed_id, posting_date, 'CRDB', details, credit_amount, 'No phone/plate', 'No identifier', ref_number or ''])
                            stats['failed'] += 1
                            print(f"❌ FAILED: No phone/plate found in: {details[:80]} (REF: {ref_number})")
        
        # ── Flush iPhone buckets immediately (no review flow needed) ──────────
        if bank_passed_data:
            print(f"\n📱 Writing {len(bank_passed_data)} rows to BANK_PASSED...")
            append_to_sheet(service, 'BANK_PASSED', bank_passed_data)

        if bank_failed_data:
            print(f"\n📱 Writing {len(bank_failed_data)} rows to BANK_FAILED...")
            append_to_sheet(service, 'BANK_FAILED', bank_failed_data)

        # ── Flush fuzzy-rescued bucket → PASSED + green highlight ─────────────
        if fuzzy_passed_data:
            print(f"\n🟢 Writing {len(fuzzy_passed_data)} fuzzy-rescued rows to PASSED...")
            # Capture next row number BEFORE appending so we can compute which rows to highlight
            start_row = get_last_row_number(service, 'PASSED') + 1
            if append_to_sheet(service, 'PASSED', fuzzy_passed_data):
                highlight_rows = list(range(start_row, start_row + len(fuzzy_passed_data)))
                apply_green_highlight(service, 'PASSED', highlight_rows)

        # ── Store review data in file instead of session ───────────────────────
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
                    },
                    'bank': 'CRDB'
                }, f)
            
            session['review_file'] = review_file
            
            return jsonify({
                'needs_review': True,
                'review_data': needs_review_data,
                'stats': stats,
                'message': f"Found {len(needs_review_data)} records that need your review before processing"
            })
        
        # ── No reviews needed — append directly ───────────────────────────────
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
            'message': (
                f"Processed {stats['total']} transactions: "
                f"{stats['passed']} passed, "
                f"{stats['passed_sav']} passed (SAV), "
                f"{stats['failed']} failed, "
                f"{stats['iphone_passed']} iPhone passed, "
                f"{stats['iphone_failed']} iPhone failed, "
                f"{stats['fuzzy_rescued']} fuzzy rescued 🟢"
            )
        })
    
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


def process_nmb_transactions(filepath):
    """
    🔥 UPDATED: Process NMB bank statement with 3-tier routing:
    1. Found in pikipiki records (sheet 1)  → PASSED       (shared with CRDB, shared ID)
    2. Found in pikipiki records2 (SAV)     → PASSED_SAV_NMB
    3. Not found                            → FAILED_NMB
    Also includes needs_review / plate suggestion flow (same as CRDB).
    """
    try:
        print("📊 Processing NMB Excel file...")

        # ── Detect engine: xlrd for .xls, openpyxl for .xlsx ──────────────────
        filepath_lower = filepath.lower()
        if filepath_lower.endswith('.xls'):
            engine = 'xlrd'
        else:
            engine = 'openpyxl'

        print(f"📂 Reading with engine: {engine}")

        # ── Auto-detect header row ─────────────────────────────────────────────
        # NMB statements have variable metadata before the data table.
        # We scan each row for the word 'Description' which always appears
        # in the column header row. This works for both .xls and .xlsx.
        HEADER_ROW = None
        try:
            scan_df = pd.read_excel(filepath, engine=engine, header=None, nrows=60)
            for idx, row in scan_df.iterrows():
                row_vals = [str(v).strip() for v in row if v is not None and str(v).strip() != 'nan']
                if 'Description' in row_vals or 'DESCRIPTION' in row_vals:
                    HEADER_ROW = idx
                    print(f"✅ NMB header row auto-detected at row {idx} (0-based)")
                    break
            del scan_df
            gc.collect()
        except Exception as scan_err:
            print(f"⚠️ Header scan failed: {scan_err} — falling back to row 23")

        if HEADER_ROW is None:
            HEADER_ROW = 23
            print(f"⚠️ Header not found in first 60 rows, using default row {HEADER_ROW}")

        # ── Read only the data we need ─────────────────────────────────────────
        # skiprows skips the metadata; header=0 uses the first row after skipping
        # as column names. We keep only the 4 columns the processing loop reads.
        try:
            df = pd.read_excel(
                filepath,
                engine=engine,
                skiprows=HEADER_ROW,
                header=0,
                dtype=str
            )
        except Exception as read_err:
            return jsonify({'error': f'Failed to read NMB file: {str(read_err)}'}), 400

        print(f"Columns found: {list(df.columns)}")

        # NMB columns: Date, Value Date, Cheque Number/Control Number,
        #              Description, Reference Number, Credit, Debit, Balance
        required_columns = ['Date', 'Description', 'Credit']
        missing = [col for col in required_columns if col not in df.columns]

        if missing:
            return jsonify({
                'error': f'Missing required columns: {missing}. Found: {list(df.columns)}'
            }), 400

        # ── Convert Credit/Debit in-place, filter to credit rows only ─────────
        df.loc[:, 'Credit'] = pd.to_numeric(
            df['Credit'].str.replace(',', '', regex=False)
                        .str.replace('TZS', '', regex=False)
                        .str.strip(),
            errors='coerce'
        )

        if 'Debit' in df.columns:
            df.loc[:, 'Debit'] = pd.to_numeric(
                df['Debit'].str.replace(',', '', regex=False)
                           .str.replace('TZS', '', regex=False)
                           .str.strip(),
                errors='coerce'
            )
            mask = ((df['Credit'].notna()) & (df['Credit'] > 0) &
                    ((df['Debit'].isna()) | (df['Debit'] == 0)))
        else:
            mask = (df['Credit'].notna()) & (df['Credit'] > 0)

        # Keep only the 4 columns the loop actually uses before converting to dicts
        keep_cols = [c for c in ['Date', 'Description', 'Reference Number', 'Credit'] if c in df.columns]
        transactions_list = df.loc[mask, keep_cols].to_dict('records')

        # 🔥 Free full df and mask immediately — list-of-dicts is all we need
        del df, mask
        gc.collect()

        print(f"✅ NMB Excel: Found {len(transactions_list)} credit transactions, DataFrame freed")
        
        # Initialize Google Sheets service
        service = get_google_service()
        
        # ── Load BOTH customer sources separately ──────────────────────────────
        print("Loading customer database from pikipiki records (sheet 1)...")
        phone_lookup, plate_lookup = load_all_customers(service)

        print("Loading customer database from pikipiki records2 (SAV)...")
        phone_lookup_sav, plate_lookup_sav, id_lookup_sav = load_all_customers_sav(service)

        # 🔥 NEW: Load iPhone lookup for NMB iPhone channel
        print("\nLoading iPhone customer database from IPHONE_RECORDS...")
        iphone_lookup = load_iphone_customers(service)

        # ── Duplicate-check refs across ALL relevant tabs ──────────────────────
        # Check BOTH old sheet (PASSED_SHEET_ID) AND new NMB sheet to cover
        # all existing records — old data stays on old sheet.

        # 🔥 For NMB: load PASSED refs only (not messages) — PASSED has 30k+ CRDB rows
        # that would OOM the server. NMB has its own ref number column so message
        # matching against PASSED is not needed.
        print("Loading existing references from old PASSED sheet (refs only)...")
        existing_passed_refs, existing_passed_messages = get_existing_refs(service, 'PASSED', refs_only=True)

        print("Loading existing references from new NMB PASSED sheet (refs only)...")
        existing_nmb_passed_refs, existing_nmb_passed_messages = get_existing_refs(service, 'PASSED_NMB', refs_only=True)

        print("Loading existing references from old PASSED_SAV_NMB sheet (refs only)...")
        existing_passed_nmb_old_refs, existing_passed_nmb_old_messages = get_existing_refs(service, 'PASSED_SAV_NMB_OLD', refs_only=True)

        print("Loading existing references from new PASSED_SAV_NMB sheet (refs only)...")
        existing_passed_nmb_refs, existing_passed_nmb_messages = get_existing_refs(service, 'PASSED_SAV_NMB', refs_only=True)

        print("Loading existing references from old FAILED_NMB sheet...")
        existing_failed_nmb_old_refs, existing_failed_nmb_old_messages = get_existing_refs(service, 'FAILED_NMB_OLD')

        print("Loading existing references from new FAILED_NMB sheet...")
        existing_failed_nmb_refs, existing_failed_nmb_messages = get_existing_refs(service, 'FAILED_NMB')

        # 🔥 Load BANK_PASSED/BANK_FAILED refs_only to save memory
        print("Loading existing references from BANK_PASSED sheet (refs only)...")
        existing_bank_passed_refs, existing_bank_passed_messages = get_existing_refs(service, 'BANK_PASSED', refs_only=True)
        print("Loading existing references from BANK_FAILED sheet (refs only)...")
        existing_bank_failed_refs, existing_bank_failed_messages = get_existing_refs(service, 'BANK_FAILED', refs_only=True)

        all_iphone_existing_refs     = existing_bank_passed_refs.union(existing_bank_failed_refs)
        all_iphone_existing_messages = existing_bank_passed_messages.union(existing_bank_failed_messages)

        all_existing_refs = (
            existing_passed_refs
            .union(existing_nmb_passed_refs)
            .union(existing_passed_nmb_old_refs)
            .union(existing_passed_nmb_refs)
            .union(existing_failed_nmb_old_refs)
            .union(existing_failed_nmb_refs)
            .union(all_iphone_existing_refs)       # 🔥 iPhone sheets included
        )
        all_existing_messages = (
            existing_passed_messages
            .union(existing_nmb_passed_messages)
            .union(existing_passed_nmb_old_messages)
            .union(existing_passed_nmb_messages)
            .union(existing_failed_nmb_old_messages)
            .union(existing_failed_nmb_messages)
            .union(all_iphone_existing_messages)   # 🔥 iPhone sheets included
        )
        print(f"Total unique NMB refs in system (old+new): {len(all_existing_refs)}")

        # 🔥 Free individual sets — keep all_iphone_existing_messages (still needed in loop)
        del existing_passed_refs, existing_passed_messages
        del existing_nmb_passed_refs, existing_nmb_passed_messages
        del existing_passed_nmb_old_refs, existing_passed_nmb_old_messages
        del existing_passed_nmb_refs, existing_passed_nmb_messages
        del existing_failed_nmb_old_refs, existing_failed_nmb_old_messages
        del existing_failed_nmb_refs, existing_failed_nmb_messages
        del existing_bank_passed_refs, existing_bank_passed_messages
        del existing_bank_failed_refs, existing_bank_failed_messages
        gc.collect()

        # ── Get last IDs — take max of old + new sheets ────────────────────────
        last_passed_id     = max(get_last_id(service, 'PASSED'), get_last_id(service, 'PASSED_NMB'))
        last_passed_nmb_id = max(get_last_id(service, 'PASSED_SAV_NMB_OLD'), get_last_id(service, 'PASSED_SAV_NMB'))
        last_failed_nmb_id = max(get_last_id(service, 'FAILED_NMB_OLD'), get_last_id(service, 'FAILED_NMB'))
        print(f"Continuing from IDs — PASSED:{last_passed_id}, PASSED_SAV_NMB:{last_passed_nmb_id}, FAILED_NMB:{last_failed_nmb_id}")

        # 🔥 NEW: Last IDs for iPhone sheets (shared with CRDB iPhone)
        last_bank_passed_id = get_last_id(service, 'BANK_PASSED')
        last_bank_failed_id = get_last_id(service, 'BANK_FAILED')

        passed_data      = []          # → shared PASSED tab
        passed_nmb_data  = []          # → PASSED_SAV_NMB
        failed_nmb_data  = []          # → FAILED_NMB
        bank_passed_data = []          # 🔥 NEW → BANK_PASSED (iPhone)
        bank_failed_data = []          # 🔥 NEW → BANK_FAILED (iPhone)
        needs_review_data = []         # → review modal

        # 🔥 NEW: Fuzzy-rescued rows → PASSED with green highlight
        fuzzy_passed_data = []

        stats = {
            'total': len(transactions_list),
            'passed': 0,           # went to PASSED (pikipiki records match)
            'passed_sav_nmb': 0,   # went to PASSED_SAV_NMB (records2 match)
            'failed_nmb': 0,
            'needs_review': 0,
            'skipped': 0,
            'iphone_passed': 0,    # 🔥 NEW
            'iphone_failed': 0,    # 🔥 NEW
            'iphone_skipped': 0,   # 🔥 NEW
            'fuzzy_rescued': 0,    # 🔥 NEW
        }

        for row in transactions_list:
            date_col    = str(row.get('Date', ''))
            description = str(row.get('Description', ''))
            credit_amount = row.get('Credit', 0)

            # 🔥 Extract date+time from within the description message.
            # Fallback to the Date column (date only, no time) if not found.
            extracted_dt = extract_nmb_datetime(description, date_col)
            date = extracted_dt if extracted_dt else date_col

            # NMB has a dedicated Reference Number column
            ref_number = (
                str(row.get('Reference Number', '')).strip()
                if 'Reference Number' in row and pd.notna(row.get('Reference Number'))
                else ''
            )

            # ── Duplicate check ────────────────────────────────────────────────
            is_duplicate = False
            if ref_number and ref_number in all_existing_refs:
                is_duplicate = True
                stats['skipped'] += 1
            elif description in all_existing_messages:
                is_duplicate = True
                stats['skipped'] += 1

            if is_duplicate:
                continue

            # ══════════════════════════════════════════════════════════════════
            # 🔥 NEW: NMB iPhone Channel — intercept BEFORE normal processing
            # Same logic as CRDB iPhone but with 'NMB' in bank column
            # ══════════════════════════════════════════════════════════════════
            if is_iphone_transaction(description):
                print(f"\n📱 NMB iPhone transaction detected: {description[:80]}")

                # Duplicate check within iPhone sheets
                iphone_is_dup = False
                if ref_number and ref_number in all_iphone_existing_refs:
                    iphone_is_dup = True
                elif description in all_iphone_existing_messages:
                    iphone_is_dup = True

                if iphone_is_dup:
                    stats['iphone_skipped'] += 1
                    stats['skipped'] += 1
                    print(f"  ⏭️ NMB iPhone duplicate — skipped")
                    continue  # Do NOT fall through to normal flow

                # Look up customer in IPHONE_RECORDS
                customer_name, raw_phone = lookup_iphone_customer(description, iphone_lookup)

                # Determine display identifier (0XX format, matching IPHONE_RECORDS)
                if raw_phone:
                    norm = normalize_phone_iphone(raw_phone)
                    display_phone = f"0{norm}" if norm else raw_phone
                else:
                    display_phone = 'No phone'

                if customer_name:
                    # ✅ Match found → BANK_PASSED
                    last_bank_passed_id += 1
                    bank_passed_row = [
                        last_bank_passed_id,
                        date,
                        'NMB',          # 🔥 NMB not CRDB
                        description,
                        credit_amount,
                        display_phone,
                        customer_name,
                        ref_number or '',
                        ''
                    ]
                    bank_passed_data.append(bank_passed_row)
                    stats['iphone_passed'] += 1
                    print(f"  ✅ BANK_PASSED (NMB): {customer_name} — {display_phone} — {credit_amount}")
                else:
                    # ❌ No match → BANK_FAILED
                    last_bank_failed_id += 1
                    reason = f"PHONE({display_phone}) not found in IPHONE_RECORDS"
                    bank_failed_row = [
                        last_bank_failed_id,
                        date,
                        'NMB',          # 🔥 NMB not CRDB
                        description,
                        credit_amount,
                        display_phone,
                        reason,
                        ref_number or ''
                    ]
                    bank_failed_data.append(bank_failed_row)
                    stats['iphone_failed'] += 1
                    print(f"  ❌ BANK_FAILED (NMB): {reason}")

                # ⚠️ CRITICAL: continue — do NOT run normal pikipiki logic
                continue
            # ══════════════════════════════════════════════════════════════════
            # End NMB iPhone Channel
            # ══════════════════════════════════════════════════════════════════

            # ── Extract identifiers ────────────────────────────────────────────
            phone = extract_phone_number(description)
            plate = extract_plate_number(description)

            identifier  = None
            lookup_type = None

            if phone:
                identifier  = phone
                lookup_type = 'phone'
                print(f"Found phone: {phone} in: {description[:80]}")
            elif plate:
                identifier  = plate
                lookup_type = 'plate'
                print(f"Found plate: {plate} in: {description[:80]}")

            if identifier and lookup_type:
                # ── Tier 1: pikipiki records → PASSED ─────────────────────────
                customer_name = lookup_customer_from_cache(
                    identifier, lookup_type, phone_lookup, plate_lookup
                )

                if customer_name:
                    last_passed_id += 1
                    passed_row = [
                        last_passed_id,
                        date,
                        'NMB',          # bank column
                        description,
                        credit_amount,
                        identifier,
                        customer_name,
                        ref_number,
                        ''              # no customer_id for records-1 customers
                    ]
                    passed_data.append(passed_row)
                    stats['passed'] += 1
                    print(f"✅ PASSED (NMB): {customer_name} - {identifier} - {credit_amount}")

                else:
                    # ── Tier 2: pikipiki records2 → PASSED_SAV_NMB ────────────
                    customer_name_sav = lookup_customer_from_cache(
                        identifier, lookup_type, phone_lookup_sav, plate_lookup_sav
                    )

                    if customer_name_sav:
                        customer_id = lookup_customer_id_from_cache(
                            identifier, lookup_type, id_lookup_sav
                        )
                        last_passed_nmb_id += 1
                        passed_nmb_row = [
                            last_passed_nmb_id,
                            date,
                            'NMB',
                            description,
                            credit_amount,
                            identifier,
                            customer_name_sav,
                            ref_number,
                            customer_id
                        ]
                        passed_nmb_data.append(passed_nmb_row)
                        stats['passed_sav_nmb'] += 1
                        print(f"✅ PASSED_SAV_NMB: {customer_name_sav} - {identifier} - {credit_amount} - ID: {customer_id}")

                    else:
                        # ── Tier 3: not in pikipiki records1 or records2 ──────
                        # If we have a phone, try IPHONE_RECORDS before giving up
                        iphone_matched = False
                        if lookup_type == 'phone':
                            norm = normalize_phone_iphone(identifier)
                            iphone_customer = iphone_lookup.get(norm) if norm else None
                            if iphone_customer:
                                iphone_is_dup = (
                                    (ref_number and ref_number in all_iphone_existing_refs)
                                    or description in all_iphone_existing_messages
                                )
                                if not iphone_is_dup:
                                    display_phone = f"0{norm}"
                                    last_bank_passed_id += 1
                                    bank_passed_row = [
                                        last_bank_passed_id,
                                        date,
                                        'NMB',
                                        description,
                                        credit_amount,
                                        display_phone,
                                        iphone_customer,
                                        ref_number or '',
                                        ''
                                    ]
                                    bank_passed_data.append(bank_passed_row)
                                    stats['iphone_passed'] += 1
                                    iphone_matched = True
                                    print(f"  ✅ BANK_PASSED (NMB phone fallback): {iphone_customer} — {display_phone} — {credit_amount}")

                        if not iphone_matched:
                            # ── FUZZY RESCUE attempt before giving up ─────────
                            # Only for plate failures, not phone failures
                            fuzzy_cands = []
                            if lookup_type == 'plate':
                                fuzzy_cands = try_fuzzy_rescue(description, plate_lookup,
                                                               plate_lookup_sav, id_lookup_sav)

                            if fuzzy_cands:
                                last_passed_id += 1
                                fuzzy_row = fuzzy_rescue_to_passed_row(
                                    last_passed_id, date, 'NMB', description,
                                    credit_amount, ref_number, fuzzy_cands
                                )
                                fuzzy_passed_data.append(fuzzy_row)
                                stats['fuzzy_rescued'] += 1
                                print(f"  🟢 FUZZY→PASSED (NMB): {len(fuzzy_cands)} candidate(s) written green")
                                continue  # move to next transaction

                            # Truly not found anywhere → FAILED_NMB
                            last_failed_nmb_id += 1
                            reason = f"{lookup_type.upper()}({identifier}) not found"

                            final_identifier = identifier
                            if lookup_type == 'phone' and not identifier.startswith('255'):
                                if identifier.startswith('0'):
                                    final_identifier = '255' + identifier[1:]
                                else:
                                    final_identifier = '255' + identifier

                            failed_nmb_row = [
                                last_failed_nmb_id,
                                date,
                                'NMB',
                                description,
                                credit_amount,
                                final_identifier,
                                reason,
                                ref_number
                            ]
                            failed_nmb_data.append(failed_nmb_row)
                            stats['failed_nmb'] += 1
                            print(f"❌ FAILED_NMB: Customer not found for {final_identifier} (REF: {ref_number})")

            else:
                # ── No clean identifier — try plate suggestions (review flow) ──
                plate_suggestions = extract_plate_suggestions(description)

                if plate_suggestions:
                    added_to_review = False
                    for suggestion in plate_suggestions:
                        suggested_plate = suggestion['suggested']
                        customer_name = lookup_customer_from_cache(suggested_plate, 'plate', phone_lookup, plate_lookup)
                        customer_name_sav = None
                        customer_id = ''
                        if not customer_name:
                            customer_name_sav = lookup_customer_from_cache(suggested_plate, 'plate', phone_lookup_sav, plate_lookup_sav)
                            if customer_name_sav:
                                customer_id = lookup_customer_id_from_cache(suggested_plate, 'plate', id_lookup_sav)
                        if customer_name or customer_name_sav:
                            target_sheet = 'PASSED' if customer_name else 'PASSED_SAV_NMB'
                            needs_review_data.append({'posting_date': date, 'details': description, 'credit_amount': credit_amount, 'ref_number': ref_number, 'original_text': suggestion['original'], 'suggested_plate': suggested_plate, 'customer_name': customer_name or customer_name_sav, 'customer_id': customer_id, 'target_sheet': target_sheet, 'confidence': suggestion['confidence'], 'reason': suggestion['reason'], 'bank': 'NMB'})
                            stats['needs_review'] += 1
                            added_to_review = True
                            print(f"🔍 NMB NEEDS REVIEW: {suggestion['original']} -> {suggested_plate} -> {customer_name or customer_name_sav}")
                            break

                    if not added_to_review:
                        # ── RESCUE before FAILED ──────────────────────────────
                        rescue_plates = _rescue_find_plates(description)
                        if rescue_plates:
                            candidate_details = []
                            for rp in rescue_plates:
                                cn = lookup_customer_from_cache(rp, 'plate', phone_lookup, plate_lookup)
                                cn_sav, cid = None, ''
                                if not cn:
                                    cn_sav = lookup_customer_from_cache(rp, 'plate', phone_lookup_sav, plate_lookup_sav)
                                    if cn_sav:
                                        cid = lookup_customer_id_from_cache(rp, 'plate', id_lookup_sav)
                                candidate_details.append({'plate': rp, 'customer_name': cn or cn_sav or '', 'customer_id': cid, 'target_sheet': 'PASSED' if cn else ('PASSED_SAV_NMB' if cn_sav else None)})
                            needs_review_data.append({'posting_date': date, 'details': description, 'credit_amount': credit_amount, 'ref_number': ref_number, 'review_type': 'choose_plate', 'candidates': candidate_details, 'bank': 'NMB'})
                            stats['needs_review'] += 1
                            print(f"🔍 RESCUE REVIEW (NMB): {[c['plate'] for c in candidate_details]}")
                        else:
                            # ── FUZZY RESCUE before FAILED ─────────────────────
                            fuzzy_cands = try_fuzzy_rescue(description, plate_lookup,
                                                           plate_lookup_sav, id_lookup_sav)
                            if fuzzy_cands:
                                last_passed_id += 1
                                fuzzy_row = fuzzy_rescue_to_passed_row(
                                    last_passed_id, date, 'NMB', description,
                                    credit_amount, ref_number, fuzzy_cands
                                )
                                fuzzy_passed_data.append(fuzzy_row)
                                stats['fuzzy_rescued'] += 1
                                print(f"  🟢 FUZZY→PASSED (NMB): {len(fuzzy_cands)} candidate(s) written green")
                            else:
                                last_failed_nmb_id += 1
                                failed_nmb_data.append([last_failed_nmb_id, date, 'NMB', description, credit_amount, 'No phone/plate', 'No identifier', ref_number])
                                stats['failed_nmb'] += 1
                else:
                    # ── RESCUE before FAILED ──────────────────────────────────
                    rescue_plates = _rescue_find_plates(description)
                    if rescue_plates:
                        candidate_details = []
                        for rp in rescue_plates:
                            cn = lookup_customer_from_cache(rp, 'plate', phone_lookup, plate_lookup)
                            cn_sav, cid = None, ''
                            if not cn:
                                cn_sav = lookup_customer_from_cache(rp, 'plate', phone_lookup_sav, plate_lookup_sav)
                                if cn_sav:
                                    cid = lookup_customer_id_from_cache(rp, 'plate', id_lookup_sav)
                            candidate_details.append({'plate': rp, 'customer_name': cn or cn_sav or '', 'customer_id': cid, 'target_sheet': 'PASSED' if cn else ('PASSED_SAV_NMB' if cn_sav else None)})
                        needs_review_data.append({'posting_date': date, 'details': description, 'credit_amount': credit_amount, 'ref_number': ref_number, 'review_type': 'choose_plate', 'candidates': candidate_details, 'bank': 'NMB'})
                        stats['needs_review'] += 1
                        print(f"🔍 RESCUE REVIEW (NMB): {[c['plate'] for c in candidate_details]}")
                    else:
                        # ── FUZZY RESCUE before FAILED ─────────────────────────
                        fuzzy_cands = try_fuzzy_rescue(description, plate_lookup,
                                                       plate_lookup_sav, id_lookup_sav)
                        if fuzzy_cands:
                            last_passed_id += 1
                            fuzzy_row = fuzzy_rescue_to_passed_row(
                                last_passed_id, date, 'NMB', description,
                                credit_amount, ref_number, fuzzy_cands
                            )
                            fuzzy_passed_data.append(fuzzy_row)
                            stats['fuzzy_rescued'] += 1
                            print(f"  🟢 FUZZY→PASSED (NMB): {len(fuzzy_cands)} candidate(s) written green")
                        else:
                            last_failed_nmb_id += 1
                            failed_nmb_data.append([last_failed_nmb_id, date, 'NMB', description, credit_amount, 'No phone/plate', 'No identifier', ref_number])
                            stats['failed_nmb'] += 1
                            print(f"❌ FAILED_NMB: No phone/plate found in: {description[:80]} (REF: {ref_number})")

        # ── If review needed, save state and return to frontend ────────────────
        if needs_review_data:
            review_file = os.path.join(
                app.config['TEMP_FOLDER'],
                f'review_{datetime.now().timestamp()}.pkl'
            )
            with open(review_file, 'wb') as f:
                pickle.dump({
                    'needs_review': needs_review_data,
                    'passed_data': passed_data,
                    'passed_nmb_data': passed_nmb_data,
                    'failed_nmb_data': failed_nmb_data,
                    'stats': stats,
                    'last_ids': {
                        'passed': last_passed_id,
                        'passed_nmb': last_passed_nmb_id,
                        'failed_nmb': last_failed_nmb_id
                    },
                    'bank': 'NMB',
                    'use_passed_nmb': True  # 🔥 flag so confirm-reviews writes to NMB sheet
                }, f)

            session['review_file'] = review_file

            # 🔥 NEW: Flush NMB iPhone buckets immediately even if review needed
            if bank_passed_data:
                print(f"\n📱 Writing {len(bank_passed_data)} NMB iPhone rows to BANK_PASSED...")
                append_to_sheet(service, 'BANK_PASSED', bank_passed_data)
            if bank_failed_data:
                print(f"\n📱 Writing {len(bank_failed_data)} NMB iPhone rows to BANK_FAILED...")
                append_to_sheet(service, 'BANK_FAILED', bank_failed_data)

            # 🔥 NEW: Flush fuzzy-rescued bucket → PASSED_NMB + green highlight
            if fuzzy_passed_data:
                print(f"\n🟢 Writing {len(fuzzy_passed_data)} NMB fuzzy-rescued rows to PASSED_NMB...")
                start_row = get_last_row_number(service, 'PASSED_NMB') + 1
                if append_to_sheet(service, 'PASSED_NMB', fuzzy_passed_data):
                    highlight_rows = list(range(start_row, start_row + len(fuzzy_passed_data)))
                    apply_green_highlight(service, 'PASSED_NMB', highlight_rows)

            return jsonify({
                'needs_review': True,
                'review_data': needs_review_data,
                'stats': stats,
                'message': f"Found {len(needs_review_data)} NMB records that need your review before processing"
            })

        # ── No reviews needed — write directly ─────────────────────────────

        # 🔥 NEW: Flush iPhone buckets first (same sheets as CRDB)
        if bank_passed_data:
            print(f"\n📱 Writing {len(bank_passed_data)} NMB iPhone rows to BANK_PASSED...")
            append_to_sheet(service, 'BANK_PASSED', bank_passed_data)

        if bank_failed_data:
            print(f"\n📱 Writing {len(bank_failed_data)} NMB iPhone rows to BANK_FAILED...")
            append_to_sheet(service, 'BANK_FAILED', bank_failed_data)

        # 🔥 NEW: Flush fuzzy-rescued bucket → PASSED_NMB + green highlight
        if fuzzy_passed_data:
            print(f"\n🟢 Writing {len(fuzzy_passed_data)} NMB fuzzy-rescued rows to PASSED_NMB...")
            start_row = get_last_row_number(service, 'PASSED_NMB') + 1
            if append_to_sheet(service, 'PASSED_NMB', fuzzy_passed_data):
                highlight_rows = list(range(start_row, start_row + len(fuzzy_passed_data)))
                apply_green_highlight(service, 'PASSED_NMB', highlight_rows)

        if passed_data:
            append_to_sheet(service, 'PASSED_NMB', passed_data)

        if passed_nmb_data:
            append_to_sheet(service, 'PASSED_SAV_NMB', passed_nmb_data)

        if failed_nmb_data:
            append_to_sheet(service, 'FAILED_NMB', failed_nmb_data)

        # Clean up uploaded file
        if os.path.exists(filepath):
            os.remove(filepath)

        return jsonify({
            'success': True,
            'stats': stats,
            'message': (
                f"Processed {stats['total']} NMB transactions: "
                f"{stats['passed']} passed (PASSED), "
                f"{stats['passed_sav_nmb']} passed (PASSED_SAV_NMB), "
                f"{stats['failed_nmb']} failed, "
                f"{stats['iphone_passed']} iPhone passed, "
                f"{stats['iphone_failed']} iPhone failed, "
                f"{stats['fuzzy_rescued']} fuzzy rescued 🟢"
            )
        })

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/confirm-reviews', methods=['POST'])
def confirm_reviews():
    """🔥 UPDATED: Handle review confirmations for both CRDB and NMB banks"""
    try:
        data = request.get_json()
        confirmations = data.get('confirmations', [])

        review_file = session.get('review_file')
        if not review_file or not os.path.exists(review_file):
            return jsonify({'error': 'Review data not found'}), 400

        with open(review_file, 'rb') as f:
            review_data = pickle.load(f)

        needs_review = review_data['needs_review']
        stats        = review_data['stats']
        last_ids     = review_data['last_ids']
        bank         = review_data.get('bank', 'CRDB')

        service = get_google_service()

        if bank == 'NMB':
            # ── NMB review processing ──────────────────────────────────────────
            passed_data     = review_data.get('passed_data', [])
            passed_nmb_data = review_data.get('passed_nmb_data', [])
            failed_nmb_data = review_data.get('failed_nmb_data', [])

            for confirmation in confirmations:
                idx    = confirmation['index']
                accept = confirmation['accept']

                if idx >= len(needs_review):
                    continue

                review_item = needs_review[idx]

                # ── Rescue choose_plate items ──────────────────────────────────
                if review_item.get('review_type') == 'choose_plate':
                    chosen_plate = confirmation.get('chosen_plate')
                    if accept and chosen_plate:
                        candidate = next((c for c in review_item['candidates'] if c['plate'] == chosen_plate), None)
                        if candidate and candidate['target_sheet'] == 'PASSED':
                            last_ids['passed'] += 1
                            passed_data.append([last_ids['passed'], review_item['posting_date'], 'NMB', review_item['details'], review_item['credit_amount'], chosen_plate, candidate['customer_name'], review_item['ref_number'], ''])
                            stats['passed'] = stats.get('passed', 0) + 1
                        elif candidate and candidate['target_sheet'] == 'PASSED_SAV_NMB':
                            last_ids['passed_nmb'] += 1
                            passed_nmb_data.append([last_ids['passed_nmb'], review_item['posting_date'], 'NMB', review_item['details'], review_item['credit_amount'], chosen_plate, candidate['customer_name'], review_item['ref_number'], candidate.get('customer_id', '')])
                            stats['passed_sav_nmb'] = stats.get('passed_sav_nmb', 0) + 1
                        else:
                            last_ids['failed_nmb'] += 1
                            failed_nmb_data.append([last_ids['failed_nmb'], review_item['posting_date'], 'NMB', review_item['details'], review_item['credit_amount'], chosen_plate or 'No plate', 'Not found in records', review_item['ref_number']])
                            stats['failed_nmb'] = stats.get('failed_nmb', 0) + 1
                    else:
                        last_ids['failed_nmb'] += 1
                        failed_nmb_data.append([last_ids['failed_nmb'], review_item['posting_date'], 'NMB', review_item['details'], review_item['credit_amount'], 'No plate chosen', 'Skipped by user', review_item['ref_number']])
                        stats['failed_nmb'] = stats.get('failed_nmb', 0) + 1
                    continue

                if accept:
                    if review_item['target_sheet'] == 'PASSED':
                        # Goes to shared PASSED tab
                        last_ids['passed'] += 1
                        row = [
                            last_ids['passed'],
                            review_item['posting_date'],
                            'NMB',
                            review_item['details'],
                            review_item['credit_amount'],
                            review_item['suggested_plate'],
                            review_item['customer_name'],
                            review_item['ref_number'],
                            ''
                        ]
                        passed_data.append(row)
                        stats['passed'] = stats.get('passed', 0) + 1
                    else:
                        # Goes to PASSED_SAV_NMB
                        last_ids['passed_nmb'] += 1
                        row = [
                            last_ids['passed_nmb'],
                            review_item['posting_date'],
                            'NMB',
                            review_item['details'],
                            review_item['credit_amount'],
                            review_item['suggested_plate'],
                            review_item['customer_name'],
                            review_item['ref_number'],
                            review_item.get('customer_id', '')
                        ]
                        passed_nmb_data.append(row)
                        stats['passed_sav_nmb'] = stats.get('passed_sav_nmb', 0) + 1
                else:
                    # Rejected → FAILED_NMB
                    last_ids['failed_nmb'] += 1
                    row = [
                        last_ids['failed_nmb'],
                        review_item['posting_date'],
                        'NMB',
                        review_item['details'],
                        review_item['credit_amount'],
                        review_item['suggested_plate'],
                        'Rejected by user',
                        review_item['ref_number']
                    ]
                    failed_nmb_data.append(row)
                    stats['failed_nmb'] = stats.get('failed_nmb', 0) + 1

            passed_tab = 'PASSED_NMB' if review_data.get('use_passed_nmb') else 'PASSED'
            if passed_data:
                append_to_sheet(service, passed_tab, passed_data)
            if passed_nmb_data:
                append_to_sheet(service, 'PASSED_SAV_NMB', passed_nmb_data)
            if failed_nmb_data:
                append_to_sheet(service, 'FAILED_NMB', failed_nmb_data)

            message = (
                f"NMB processing complete: "
                f"{stats.get('passed', 0)} passed (PASSED), "
                f"{stats.get('passed_sav_nmb', 0)} passed (PASSED_SAV_NMB), "
                f"{stats.get('failed_nmb', 0)} failed"
            )

        else:
            # ── CRDB review processing (original logic) ────────────────────────
            passed_data     = review_data['passed_data']
            passed_sav_data = review_data['passed_sav_data']
            failed_data     = review_data['failed_data']

            for confirmation in confirmations:
                idx    = confirmation['index']
                accept = confirmation['accept']

                if idx >= len(needs_review):
                    continue

                review_item = needs_review[idx]

                # ── Rescue choose_plate items ──────────────────────────────────
                if review_item.get('review_type') == 'choose_plate':
                    chosen_plate = confirmation.get('chosen_plate')
                    if accept and chosen_plate:
                        candidate = next((c for c in review_item['candidates'] if c['plate'] == chosen_plate), None)
                        if candidate and candidate['target_sheet'] == 'PASSED':
                            last_ids['passed'] += 1
                            passed_data.append([last_ids['passed'], review_item['posting_date'], 'CRDB', review_item['details'], review_item['credit_amount'], chosen_plate, candidate['customer_name'], review_item['ref_number'], ''])
                            stats['passed'] += 1
                        elif candidate and candidate['target_sheet'] == 'PASSED_SAV':
                            last_ids['passed_sav'] += 1
                            passed_sav_data.append([last_ids['passed_sav'], review_item['posting_date'], 'CRDB', review_item['details'], review_item['credit_amount'], chosen_plate, candidate['customer_name'], review_item['ref_number'], candidate.get('customer_id', '')])
                            stats['passed_sav'] += 1
                        else:
                            last_ids['failed'] += 1
                            failed_data.append([last_ids['failed'], review_item['posting_date'], 'CRDB', review_item['details'], review_item['credit_amount'], chosen_plate or 'No plate', 'Not found in records', review_item['ref_number']])
                            stats['failed'] += 1
                    else:
                        last_ids['failed'] += 1
                        failed_data.append([last_ids['failed'], review_item['posting_date'], 'CRDB', review_item['details'], review_item['credit_amount'], 'No plate chosen', 'Skipped by user', review_item['ref_number']])
                        stats['failed'] += 1
                    continue

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
                            review_item['ref_number'],
                            ''
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
                            review_item['ref_number'],
                            review_item.get('customer_id', '')
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

            if passed_data:
                append_to_sheet(service, 'PASSED', passed_data)
            if passed_sav_data:
                append_to_sheet(service, 'PASSED_SAV', passed_sav_data)
            if failed_data:
                append_to_sheet(service, 'FAILED', failed_data)

            message = (
                f"Processing and update complete: "
                f"{stats.get('passed', 0)} passed, "
                f"{stats.get('passed_sav', 0)} passed (SAV), "
                f"{stats.get('failed', 0)} failed"
            )

        # ── Clean up ───────────────────────────────────────────────────────────
        filepath = session.get('filepath')
        if filepath and os.path.exists(filepath):
            os.remove(filepath)

        if os.path.exists(review_file):
            os.remove(review_file)

        session.pop('review_file', None)

        return jsonify({
            'success': True,
            'stats': stats,
            'message': message
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
