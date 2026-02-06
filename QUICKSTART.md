# 🚀 Quick Start Guide

## For Testing on Ubuntu (Local Development)

### Step 1: Setup

```bash
# Navigate to project directory
cd transaction-processor

# Run setup script
bash setup.sh

# This will:
# - Create virtual environment
# - Install all dependencies
# - Create uploads folder
```

### Step 2: Add Google Credentials

1. Place your `google.json` file in the project root directory
2. Make sure it contains your Google OAuth credentials from Google Cloud Console

### Step 3: Test Extraction Logic (Optional)

```bash
# Activate virtual environment
source venv/bin/activate

# Run test
python test_extraction.py
```

This will test the phone number, plate number, and reference extraction logic.

### Step 4: Run the Application

```bash
# Make sure virtual environment is activated
source venv/bin/activate

# Start the Flask server
python app.py
```

The app will start on: **http://localhost:5000**

### Step 5: Use the Application

1. Open browser to `http://localhost:5000`
2. Click "Connect Google Sheets" and authenticate
3. Upload your Excel bank statement (.xlsx)
4. Click "Process Transactions"
5. Check results in your Google Sheets

---

## For Production (Render Deployment)

See **RENDER_DEPLOYMENT.md** for detailed deployment instructions.

### Quick Deploy Steps:

1. **Push to GitHub**
   ```bash
   git init
   git add .
   git commit -m "Initial commit"
   git remote add origin YOUR_GITHUB_URL
   git push -u origin main
   ```

2. **Create Render Web Service**
   - Connect GitHub repository
   - Set environment variables:
     - `SECRET_KEY`: Random secret key
     - `GOOGLE_CREDENTIALS`: Your google.json content

3. **Update OAuth Redirect URI**
   - Add `https://your-app.onrender.com/auth/callback` to Google Cloud Console

4. **Deploy and Test**

---

## Project Structure

```
transaction-processor/
├── app.py                    # Main Flask application
├── templates/
│   └── index.html           # Web interface
├── requirements.txt          # Python dependencies
├── Procfile                 # Render deployment config
├── setup.sh                 # Local setup script
├── test_extraction.py       # Test extraction functions
├── .gitignore              # Git ignore rules
├── .env.example            # Environment variables template
├── README.md               # Full documentation
├── RENDER_DEPLOYMENT.md    # Deployment guide
└── google.json             # Your OAuth credentials (DO NOT COMMIT!)
```

---

## Troubleshooting

### "google.json not found"
- Make sure you placed your Google OAuth credentials file as `google.json` in the project root
- Download it from Google Cloud Console > APIs & Services > Credentials

### "Module not found" errors
- Make sure virtual environment is activated: `source venv/bin/activate`
- Reinstall dependencies: `pip install -r requirements.txt`

### OAuth not working
- Check redirect URI in Google Cloud Console matches your URL
- For local: `http://localhost:5000/auth/callback`
- For Render: `https://your-app.onrender.com/auth/callback`

### No transactions processed
- Check Excel file has correct columns: "Posting Date", "Details", "Debit", "Credit"
- Verify phone/plate formats in transaction details
- Check Google Sheets IDs in app.py are correct

---

## Need Help?

1. Check the full **README.md** for detailed documentation
2. Review **RENDER_DEPLOYMENT.md** for deployment issues
3. Run `python test_extraction.py` to test extraction logic
4. Check application logs for error messages

---

## What This App Does

✅ Reads Excel bank statements
✅ Extracts DEBIT transactions only
✅ Finds phone numbers (255XXXXXXXXX, 07XXXXXXXX, 06XXXXXXXX)
✅ Finds plate numbers (MC###XXX format)
✅ Looks up customer names from "pikipiki records" sheet
✅ Adds matched transactions to "PASSED" sheet
✅ Adds unmatched transactions to "FAILED" sheet
✅ Skips duplicate transactions (checks REF numbers)
✅ Auto-increments IDs

---

## First Time Setup Checklist

- [ ] Clone/download project files
- [ ] Run `bash setup.sh`
- [ ] Add `google.json` file
- [ ] Test locally: `python app.py`
- [ ] Access http://localhost:5000
- [ ] Authenticate with Google
- [ ] Upload test file
- [ ] Verify results in Google Sheets
- [ ] Ready to deploy to Render!

---

**Happy Processing! 🎉**
