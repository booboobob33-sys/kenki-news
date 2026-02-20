import requests
import feedparser
import google.generativeai as genai
from notion_client import Client
import notion_client
from datetime import datetime
import time
import json
import os
import re
import sys
import traceback

# --- Configuration ---
NOTION_TOKEN = os.getenv("NOTION_TOKEN", "")
DATABASE_ID = os.getenv("DATABASE_ID", "")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")

def safe_print(text):
    """Safely print text handling potential encoding errors."""
    try:
        print(text, flush=True)
    except Exception:
        try:
            print(str(text).encode('utf-8', errors='replace').decode('utf-8'), flush=True)
        except:
             pass

# Diagnostic Log for environment
def check_env_and_exit_if_empty():
    safe_print(f"--- Environment Diagnostics ---")
    safe_print(f"Python Executable: {sys.executable}")
    safe_print(f"SDK Version (Gemini): {getattr(genai, '__version__', 'Unknown')}")
    safe_print(f"SDK Version (Notion): {getattr(notion_client, '__version__', 'Obsolete (<0.5.0)')}")
    
    mask = lambda s: s[:4] + "***" if (s and len(s) > 4) else ("EMPTY" if not s else "SHORT")
    safe_print(f"NOTION_TOKEN: {mask(NOTION_TOKEN)}")
    safe_print(f"DATABASE_ID: {mask(DATABASE_ID)}")
    safe_print(f"GEMINI_API_KEY: {mask(GEMINI_API_KEY)}")
    
    missing = []
    if not NOTION_TOKEN: missing.append("NOTION_TOKEN")
    if not DATABASE_ID: missing.append("DATABASE_ID")
    if not GEMINI_API_KEY: missing.append("GEMINI_API_KEY")
    
    if missing:
        safe_print(f"\n[CRITICAL ERROR] The following Secrets are missing in GitHub Settings: {', '.join(missing)}")
        safe_print("Please check: Settings -> Secrets and variables -> Actions")
        sys.exit(1)
    safe_print(f"-------------------------------\n")

# Run check immediately
check_env_and_exit_if_empty()

# AI Configuration (v1 REST for Paid Tier stability)
genai.configure(api_key=GEMINI_API_KEY, transport='rest')

def get_best_model():
    """Discover available models to avoid 404 errors."""
    try:
        models = [m.name for m in genai.list_models()]
        preferred = [
            "models/gemini-1.5-flash",
            "models/gemini-1.5-flash-latest",
            "models/gemini-1.5-flash-001",
            "models/gemini-1.5-flash-002",
            "models/gemini-1.0-pro"
        ]
        for p in preferred:
            if p in models:
                safe_print(f"  [AI INFO] Using model: {p}")
                return genai.GenerativeModel(model_name=p)
        
        # Fallback to defaults
        for m in models:
            if "flash" in m or "pro" in m:
                safe_print(f"  [AI INFO] Falling back to available model: {m}")
                return genai.GenerativeModel(model_name=m)
    except Exception as e:
        safe_print(f"  [AI WARN] Could not list models: {e}")
    return genai.GenerativeModel(model_name="gemini-1.5-flash")

model = get_best_model()

# Notion Client
notion = Client(auth=NOTION_TOKEN)

# ニュースソース設定 (Googleニュース検索)
RSS_FEEDS = [
    {"name": "Googleニュース (建設機械)", "url": "https://news.google.com/rss/search?q=%E5%BB%BA%E8%A8%AD%E6%A9%9F%E6%A2%B0&hl=ja&gl=JP&ceid=JP:ja"},
    {"name": "Googleニュース (鉱山機械)", "url": "https://news.google.com/rss/search?q=%E9%89%B1%E5%B1%B1%E6%A9%9F%E6%A2%B0&hl=ja&gl=JP&ceid=JP:ja"},
    {"name": "Googleニュース (コマツ/Komatsu)", "url": "https://news.google.com/rss/search?q=Komatsu+OR+%E3%82%B3%E3%83%9E%E3%83%84&hl=ja&gl=JP&ceid=JP:ja"},
    {"name": "Googleニュース (日立建機/Hitachi CM)", "url": "https://news.google.com/rss/search?q=Hitachi+Construction+Machinery+OR+%E6%97%A5%E7%AB%8B%E5%BB%BA%E6%A9%9F&hl=ja&gl=JP&ceid=JP:ja"},
    {"name": "KHL Construction News", "url": "https://news.google.com/rss/search?q=site:khl.com+International+Construction&hl=en-US&gl=US&ceid=US:en"}
]

def fetch_latest_article(feed_config):
    safe_print(f"[FETCH] {feed_config['name']}...")
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36'}
        response = requests.get(feed_config['url'], headers=headers, timeout=20)
        if response.status_code != 200: return None
        feed = feedparser.parse(response.content)
        if feed.entries:
            entry = feed.entries[0]
            return {"title": entry.title, "link": entry.link, "summary": entry.get("summary", entry.get("description", ""))}
    except Exception as e:
        safe_print(f"  [ERROR] Fetch: {e}")
    return None

def analyze_article_with_gemini(article_data):
    safe_print(f"  [AI] Analyzing: {article_data['title'][:40]}...")
    prompt = f"""建設機械業界の判定: ニュース（{article_data['title']} / {article_data['summary']}）が建設・鉱山・農林機械に関連するか判定。
【出力形式】関連あればJSON項目（brand, segment, summary_ja）のみ。なければ 'null'。"""
    try:
        response = model.generate_content(prompt)
        text = response.text.strip()
        if "null" in text.lower() and "{" not in text: return None
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match: return json.loads(match.group())
    except Exception as e:
        safe_print(f"  [ERROR] AI: {e}"); time.sleep(10)
    return None

def clean_multi_select(val):
    if not val: return [{"name": "Other"}]
    parts = [p.strip() for p in str(val).replace("、", ",").split(",")]
    return [{"name": p} for p in parts if p]

def save_to_notion(result, article_data):
    safe_print(f"  [NOTION] Attempting save...")
    try:
        # 重複チェック (機能を分離して安全に実行)
        try:
            if hasattr(notion.databases, "query"):
                q = notion.databases.query(database_id=DATABASE_ID, filter={"property": "Source URL", "url": {"equals": article_data['link']}})
                if q["results"]:
                    safe_print("  [SKIP] Duplicate article.")
                    return False
        except Exception as qe:
            safe_print(f"  [WARN] Duplication check failed (skipping): {qe}")

        notion.pages.create(
            parent={"database_id": DATABASE_ID},
            properties={
                "Title": {"title": [{"text": {"content": article_data['title'][:100]}}]},
                "Source Name": {"select": {"name": "RSS Search Collector"}},
                "Source URL": {"url": article_data['link']},
                "Summary": {"rich_text": [{"text": {"content": result.get("summary_ja", "No summary")[:2000]}}]},
                "Brand": {"multi_select": clean_multi_select(result.get("brand", "Other"))},
                "Segment": {"multi_select": clean_multi_select(result.get("segment", "Other"))},
                "Date": {"date": {"start": datetime.now().isoformat()}}
            }
        )
        safe_print("  [SUCCESS] Saved article.")
        return True
    except Exception as e:
        safe_print(f"  [ERROR] Notion Save: {e}")
        return False

def main():
    processed_count = 0
    for feed in RSS_FEEDS:
        data = fetch_latest_article(feed)
        if data:
            res = analyze_article_with_gemini(data)
            if res:
                if save_to_notion(res, data):
                    processed_count += 1
                    if processed_count % 3 == 0: time.sleep(60)
        time.sleep(10)
    safe_print(f"\n=== Finished. Saved {processed_count} items. ===")

if __name__ == "__main__":
    main()
