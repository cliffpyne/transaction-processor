#!/usr/bin/env python3
"""
Test script to verify phone and plate number extraction
"""

import re

def extract_phone_number(text):
    """Extract phone number from text in formats: 255XXXXXXXXX, 07XXXXXXXX, 06XXXXXXXX"""
    if not text:
        return None
    
    text = str(text).replace(' ', '').replace('-', '')
    
    # Pattern for 255 followed by 9 digits
    pattern_255 = r'255(\d{9})'
    match = re.search(pattern_255, text)
    if match:
        return f"255{match.group(1)}"
    
    # Pattern for 07 or 06 followed by 8 digits
    pattern_07_06 = r'0([67])(\d{8})(?!\d)'
    match = re.search(pattern_07_06, text)
    if match:
        return f"0{match.group(1)}{match.group(2)}"
    
    return None

def extract_plate_number(text):
    """Extract plate number in format: MC###XXX (MC followed by 3 numbers then 3 letters)"""
    if not text:
        return None
    
    text = str(text).replace(' ', '').upper()
    
    # Pattern for MC followed by 3 digits then 3 letters
    pattern = r'MC(\d{3})([A-Z]{3})'
    match = re.search(pattern, text)
    if match:
        return f"MC{match.group(1)}{match.group(2)}"
    
    return None

def extract_ref_number(text):
    """Extract reference number from message (format: REF:XXXXX)"""
    if not text:
        return None
    
    text = str(text)
    pattern = r'REF:\s*(\S+)'
    match = re.search(pattern, text, re.IGNORECASE)
    if match:
        return match.group(1)
    
    return None

# Test cases
test_cases = [
    # Phone numbers
    "Transaction from 255752900450",
    "Payment 0752900450",
    "From 0652900450",
    "Number: 255 752 900 450",
    "Call 0752-900-450",
    
    # Plate numbers
    "Vehicle MC765EFV",
    "Plate: MC 765 EFV",
    "MC123ABC payment",
    
    # Reference numbers
    "Payment REF:12345ABC",
    "REF: ABC123XYZ",
    "Transaction ref:TEST999",
    
    # Combined
    "255752900450 MC765EFV REF:PAY123",
]

print("=" * 60)
print("TESTING EXTRACTION FUNCTIONS")
print("=" * 60)

for i, test in enumerate(test_cases, 1):
    print(f"\n{i}. Test: {test}")
    phone = extract_phone_number(test)
    plate = extract_plate_number(test)
    ref = extract_ref_number(test)
    
    if phone:
        print(f"   ✅ Phone: {phone}")
    if plate:
        print(f"   ✅ Plate: {plate}")
    if ref:
        print(f"   ✅ Ref: {ref}")
    if not (phone or plate or ref):
        print(f"   ❌ No match found")

print("\n" + "=" * 60)
print("Testing complete!")
print("=" * 60)
