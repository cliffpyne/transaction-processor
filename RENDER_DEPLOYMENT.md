# Deploying to Render

Follow these steps to deploy your Transaction Processor to Render.

## Prerequisites

1. GitHub account
2. Render account (free tier available at https://render.com)
3. Your `google.json` file content

## Step 1: Prepare Your Repository

1. **Create a new GitHub repository** (or use existing one)

2. **Initialize Git** (if not already done):
```bash
git init
git add .
git commit -m "Initial commit"
git branch -M main
git remote add origin YOUR_GITHUB_REPO_URL
git push -u origin main
```

3. **Important**: Make sure `.gitignore` is in place (already created) to prevent committing sensitive files.

## Step 2: Update Google Cloud OAuth Settings

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Navigate to "APIs & Services" > "Credentials"
3. Click on your OAuth 2.0 Client ID
4. Under "Authorized redirect URIs", add:
   ```
   https://YOUR-APP-NAME.onrender.com/auth/callback
   ```
   (Replace YOUR-APP-NAME with your chosen Render app name)
5. Save changes

## Step 3: Deploy on Render

### 3.1 Create Web Service

1. Log in to [Render Dashboard](https://dashboard.render.com/)
2. Click "New +" button → Select "Web Service"
3. Connect your GitHub account (if not already connected)
4. Select your repository
5. Click "Connect"

### 3.2 Configure Build Settings

**Basic Settings:**
- **Name**: `transaction-processor` (or your choice)
- **Region**: Choose closest to your location
- **Branch**: `main`
- **Root Directory**: Leave empty (or set if app is in subdirectory)
- **Runtime**: `Python 3`

**Build & Deploy:**
- **Build Command**: `pip install -r requirements.txt`
- **Start Command**: `gunicorn app:app`

### 3.3 Add Environment Variables

Click "Advanced" → "Add Environment Variable" and add:

1. **SECRET_KEY**
   - Generate a random key: `python3 -c "import secrets; print(secrets.token_hex(32))"`
   - Paste the generated key

2. **GOOGLE_CREDENTIALS**
   - Open your `google.json` file
   - Copy the ENTIRE content (it should be one long JSON string)
   - Paste it as the value
   - Example format:
   ```json
   {"web":{"client_id":"YOUR-CLIENT-ID.apps.googleusercontent.com","project_id":"your-project","auth_uri":"https://accounts.google.com/o/oauth2/auth","token_uri":"https://oauth2.googleapis.com/token","auth_provider_x509_cert_url":"https://www.googleapis.com/oauth2/v1/certs","client_secret":"YOUR-SECRET","redirect_uris":["https://your-app.onrender.com/auth/callback"]}}
   ```

### 3.4 Set Plan

- Select **Free** plan (sufficient for testing)
- Note: Free tier instances spin down after 15 minutes of inactivity

### 3.5 Create Web Service

Click "Create Web Service" button

## Step 4: Wait for Deployment

Render will:
1. Clone your repository
2. Install dependencies
3. Start your application
4. Provide you with a URL like `https://YOUR-APP-NAME.onrender.com`

This usually takes 2-5 minutes.

## Step 5: Test Your Deployment

1. Visit your Render URL
2. Click "Connect Google Sheets"
3. Complete OAuth authentication
4. Upload a test Excel file
5. Process transactions

## Troubleshooting

### Build Failed

**Check logs in Render dashboard:**
- Look for missing dependencies
- Verify `requirements.txt` is correct
- Ensure Python version compatibility

### OAuth Not Working

1. **Verify redirect URI** in Google Cloud Console matches your Render URL exactly
2. **Check GOOGLE_CREDENTIALS** environment variable is set correctly
3. **Clear browser cookies** and try authenticating again

### Application Crashes

1. **Check Render logs** for error messages
2. **Verify environment variables** are set correctly
3. **Test locally first** to ensure code works

### Sheets Access Denied

1. **Check Google Sheets IDs** in `app.py` are correct
2. **Verify Google account** used for OAuth has access to the sheets
3. **Enable Google Sheets API** in Google Cloud Console

## Monitoring

### View Logs
- Go to Render Dashboard
- Click on your service
- Click "Logs" tab
- Monitor real-time application logs

### Check Metrics
- View CPU and memory usage
- Monitor response times
- Track deployment history

## Updating Your App

### To Deploy Changes:

1. **Make changes locally**
2. **Commit and push to GitHub**:
   ```bash
   git add .
   git commit -m "Description of changes"
   git push origin main
   ```
3. **Render auto-deploys** when you push to main branch

### Manual Deploy:
- Go to Render Dashboard
- Click "Manual Deploy" → "Deploy latest commit"

## Free Tier Limitations

Render Free Tier includes:
- ✅ 750 hours/month
- ✅ Auto-sleep after 15 min inactivity
- ✅ SSL certificate included
- ⚠️ Slower cold starts (15-30 seconds)
- ⚠️ Limited CPU/memory

For production use, consider upgrading to paid tier.

## Cost Optimization

### Keep Free Tier Running:
- First deploy is free
- App sleeps after inactivity (saves hours)
- Wakes up on first request

### Upgrade When Needed:
- If you need 24/7 uptime
- If you need faster performance
- If processing large files regularly

## Security Checklist

- ✅ Never commit `google.json` to Git
- ✅ Use environment variables for secrets
- ✅ Keep SECRET_KEY secure and random
- ✅ Limit Google Sheets API scopes
- ✅ Monitor access logs regularly
- ✅ Use HTTPS (automatic on Render)

## Support

- **Render Docs**: https://render.com/docs
- **Render Community**: https://community.render.com
- **Your App Logs**: Check Render dashboard for issues

## Quick Commands Reference

```bash
# Generate secret key
python3 -c "import secrets; print(secrets.token_hex(32))"

# Test locally before deploying
source venv/bin/activate
python app.py

# Check Python version
python3 --version

# View git status
git status

# Push changes
git add .
git commit -m "Your message"
git push origin main
```

---

## Success Checklist

- [ ] Repository pushed to GitHub
- [ ] Google Cloud OAuth redirect URI updated
- [ ] Render web service created
- [ ] Environment variables set (SECRET_KEY, GOOGLE_CREDENTIALS)
- [ ] Deployment successful (check logs)
- [ ] Can access app URL
- [ ] OAuth authentication works
- [ ] File upload works
- [ ] Transaction processing works
- [ ] Data appears in Google Sheets

If all checkboxes are ticked, you're good to go! 🎉
