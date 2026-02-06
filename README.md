# Transaction Processor Web App

A Flask web application that processes CRDB bank statement debit transactions and automatically updates Google Sheets.

## Features

- 📤 Upload Excel bank statements (.xlsx)
- 🔍 Extract phone numbers and plate numbers from transaction details
- 👤 Automatic customer lookup from pikipiki records
- ✅ Add matched transactions to "PASSED" sheet
- ❌ Add unmatched transactions to "FAILED" sheet
- 🔄 Duplicate detection using reference numbers
- 📊 Real-time processing statistics

## Phone Number Formats Supported

- `255XXXXXXXXX` (255 followed by 9 digits)
- `07XXXXXXXX` (07 followed by 8 digits)
- `06XXXXXXXX` (06 followed by 8 digits)

The app automatically removes spaces and handles formatting issues.

## Plate Number Format

- `MC###XXX` (MC followed by 3 digits then 3 letters)
- Example: MC123ABC, MC765EFV

## Setup Instructions

### 1. Prerequisites

- Ubuntu/Linux system
- Python 3.8 or higher
- Google Cloud Project with Sheets API enabled
- `google.json` credentials file

### 2. Install Dependencies

```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate

# Install requirements
pip install -r requirements.txt
```

### 3. Configure Google Cloud

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select existing one
3. Enable Google Sheets API
4. Create OAuth 2.0 credentials:
   - Application type: Web application
   - Authorized redirect URIs: Add `http://localhost:5000/auth/callback`
5. Download credentials as `google.json`
6. Place `google.json` in the project root directory

### 4. Run Locally (Testing on Ubuntu)

```bash
# Make sure virtual environment is activated
source venv/bin/activate

# Run the app
python app.py
```

The app will be available at: `http://localhost:5000`

### 5. Test the Application

1. Open browser to `http://localhost:5000`
2. Click "Connect Google Sheets" and authenticate
3. Upload your Excel bank statement
4. Click "Process Transactions"
5. Check the results in your Google Sheets

## Deploy to Render

### 1. Prepare for Deployment

Create a file called `Procfile` (no extension):

```
web: gunicorn app:app
```

Add `gunicorn` to requirements.txt:

```bash
echo "gunicorn==21.2.0" >> requirements.txt
```

### 2. Update Redirect URI

1. Go to Google Cloud Console
2. Update OAuth 2.0 credentials
3. Add authorized redirect URI: `https://your-app-name.onrender.com/auth/callback`

### 3. Deploy on Render

1. Push code to GitHub (without `google.json`)
2. Go to [Render Dashboard](https://dashboard.render.com/)
3. Click "New +" → "Web Service"
4. Connect your GitHub repository
5. Configure:
   - **Name**: transaction-processor
   - **Environment**: Python 3
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `gunicorn app:app`
6. Add Environment Variables:
   - `SECRET_KEY`: Generate a random secret key
   - `GOOGLE_CREDENTIALS`: Paste entire contents of `google.json` file

### 4. Update app.py for Render

Add this near the top of `app.py` to read credentials from environment:

```python
import os
import json

# For Render deployment - read credentials from environment
GOOGLE_CREDS = os.environ.get('GOOGLE_CREDENTIALS')
if GOOGLE_CREDS:
    with open('google.json', 'w') as f:
        f.write(GOOGLE_CREDS)
```

### 5. Test Production

1. Visit your Render URL
2. Authenticate with Google
3. Upload and process transactions

## Project Structure

```
.
├── app.py                 # Main Flask application
├── templates/
│   └── index.html        # Frontend interface
├── requirements.txt       # Python dependencies
├── google.json           # Google OAuth credentials (not in git)
├── uploads/              # Temporary file uploads (not in git)
├── .gitignore           # Git ignore rules
└── README.md            # This file
```

## How It Works

### Processing Flow

1. **Upload**: User uploads Excel bank statement
2. **Filter**: Extract only debit transactions (Debit > 0, Credit = 0)
3. **Extract**: Find phone numbers or plate numbers in transaction details
4. **Lookup**: Search for customer in "pikipiki records" sheet
5. **Match**: 
   - If found → Add to "PASSED" with customer details
   - If not found → Add to "FAILED"
6. **Duplicate Check**: Skip transactions with existing REF numbers
7. **Auto-increment IDs**: Continue from last ID in each sheet

### Column Mapping

**PASSED Sheet:**
- ID: Auto-incremented
- Date: From "Posting Date" column
- Channel: "CRDB" (fixed)
- Message: Full transaction details
- Amount: From "Debit" column
- Plate/Phone: Extracted identifier
- Name: Customer name from lookup
- Ref Number: Extracted from message (REF:XXXXX)

**FAILED Sheet:**
- ID: Auto-incremented
- Date: From "Posting Date" column
- Channel: "CRDB" (fixed)
- Message: Full transaction details

## Troubleshooting

### Authentication Issues

- Make sure `google.json` is in the project root
- Check that redirect URI matches in Google Cloud Console
- Clear browser cookies and try again

### Processing Errors

- Ensure Excel file has correct columns: "Posting Date", "Details", "Debit", "Credit"
- Check that Google Sheets IDs are correct in `app.py`
- Verify sheet names: "PASSED", "FAILED", "pikipiki records"

### Phone/Plate Not Found

- Check format in transaction details
- Verify pikipiki records sheet has correct data
- Ensure no extra spaces in phone numbers or plates

## Support

For issues or questions, check the code comments or contact the developer.

## Security Notes

- Never commit `google.json` to Git
- Use environment variables for production
- Keep SECRET_KEY secure
- Limit Google Sheets API permissions as needed
