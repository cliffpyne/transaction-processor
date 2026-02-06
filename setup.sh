#!/bin/bash

echo "======================================"
echo "Transaction Processor Setup"
echo "======================================"
echo ""

# Check Python version
echo "Checking Python version..."
python3 --version

if [ $? -ne 0 ]; then
    echo "❌ Python 3 is not installed. Please install Python 3.8 or higher."
    exit 1
fi

echo "✅ Python 3 is installed"
echo ""

# Create virtual environment
echo "Creating virtual environment..."
python3 -m venv venv

if [ $? -ne 0 ]; then
    echo "❌ Failed to create virtual environment"
    exit 1
fi

echo "✅ Virtual environment created"
echo ""

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Upgrade pip
echo "Upgrading pip..."
pip install --upgrade pip

# Install requirements
echo ""
echo "Installing dependencies..."
pip install -r requirements.txt

if [ $? -ne 0 ]; then
    echo "❌ Failed to install dependencies"
    exit 1
fi

echo "✅ Dependencies installed"
echo ""

# Create uploads directory
mkdir -p uploads

# Check for google.json
if [ ! -f "google.json" ]; then
    echo "⚠️  WARNING: google.json file not found!"
    echo "   Please add your Google OAuth credentials file as 'google.json'"
    echo "   in the project root directory."
    echo ""
fi

echo "======================================"
echo "Setup Complete!"
echo "======================================"
echo ""
echo "Next steps:"
echo "1. Make sure google.json is in the project directory"
echo "2. Run: source venv/bin/activate"
echo "3. Run: python test_extraction.py (to test extraction logic)"
echo "4. Run: python app.py (to start the server)"
echo "5. Open: http://localhost:5000"
echo ""
