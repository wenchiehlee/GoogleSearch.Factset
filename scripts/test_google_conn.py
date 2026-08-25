
import os
import json
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

load_dotenv()

def test_connection():
    sheet_id = os.getenv('GOOGLE_SHEET_ID')
    creds_json = os.getenv('GOOGLE_SHEETS_CREDENTIALS')
    
    if not sheet_id or not creds_json:
        print("Missing env vars")
        return

    try:
        creds_info = json.loads(creds_json)
        scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        credentials = Credentials.from_service_account_info(creds_info, scopes=scopes)
        client = gspread.authorize(credentials)
        spreadsheet = client.open_by_key(sheet_id)
        print(f"✅ Success! Connected to: {spreadsheet.title}")
    except Exception as e:
        print(f"❌ Connection failed: {e}")

if __name__ == "__main__":
    test_connection()
