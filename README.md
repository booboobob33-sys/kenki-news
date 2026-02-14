# Kenki News Collector

Automated news collection system for the construction machinery industry. Fetches news from RSS feeds, summarizes/translates using Gemini AI, and saves to Notion.

## Features
- **Multi-source RSS**: English (Global) and Japanese (Local) support.
- **AI-Powered**: Summarizes articles and translates English news to Japanese.
- **Notion Integration**: Saves structured data (Summary, Content, URL) to a database.
- **Automation**: Runs daily via GitHub Actions.
- **Duplicate Prevention**: Checks existing entries in Notion.

## Setup

### 1. Prerequisites
- Python 3.10+
- Notion API Integration Token and Database ID
- Google Gemini API Key

### 2. Local Installation
```bash
pip install -r requirements.txt
```

### 3. Environment Variables
Set the following environment variables:
- `NOTION_TOKEN`: Your Notion integration token.
- `DATABASE_ID`: Your Notion database ID.
- `GEMINI_API_KEY`: Your Google Gemini API key.

## GitHub Actions
The system is configured to run daily at 5:00 AM JST. To enable this:
1. Push this repository to GitHub.
2. Go to **Settings > Secrets and variables > Actions**.
3. Add the secrets listed in the section above.
