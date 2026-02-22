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

# Diagnostic Log
def check_env_and_exit_if_empty():
    safe_print(f"--- Environment Diagnostics ---")
    safe_print(f"Python: {sys.version}")
    
    # Check SDK Versions and Paths
    try:
        import google.generativeai as gai
        import notion_client as nc
        safe_print(f"Gemini SDK: {getattr(gai, '__version__', 'Unknown')} ({getattr(gai, '__file__', 'No Path')})")
        safe_print(f"Notion SDK: {getattr(nc, '__version__', 'Unknown')} ({getattr(nc, '__file__', 'No Path')})")
    except Exception as e:
        safe_print(f"Error checking SDKs: {e}")

    mask = lambda s: s[:4] + "***" if (s and len(s) > 4) else ("EMPTY" if not s else "SHORT")
    safe_print(f"DATABASE_ID: {mask(DATABASE_ID)}")
    safe_print(f"GEMINI_API_KEY: {mask(GEMINI_API_KEY)}")
    
    if not all([NOTION_TOKEN, DATABASE_ID, GEMINI_API_KEY]):
        safe_print("\n[CRITICAL ERROR] Missing Secrets in GitHub Settings.")
        sys.exit(1)
    safe_print(f"-------------------------------\n")

check_env_and_exit_if_empty()

# AI Configuration
genai.configure(api_key=GEMINI_API_KEY)

def get_best_model():
    """Select the most stable Gemini model available with fallback strategies."""
    safe_print("  [AI] Discovering available models...")
    try:
        available_models = []
        for m in genai.list_models():
            if 'generateContent' in m.supported_generation_methods:
                available_models.append(m.name)
        
        safe_print(f"  [AI] Found: {', '.join(available_models)}")
        
        # Priority list
        preferred = ["models/gemini-1.5-flash", "gemini-1.5-flash", "models/gemini-1.5-flash-latest"]
        for p in preferred:
            for am in available_models:
                if p == am or p.split('/')[-1] == am.split('/')[-1]:
                    safe_print(f"  [AI] Selected: {am}")
                    return genai.GenerativeModel(model_name=am)
        
        if available_models:
            safe_print(f"  [AI] No preferred model found. Using first available: {available_models[0]}")
            return genai.GenerativeModel(model_name=available_models[0])
            
    except Exception as e:
        safe_print(f"  [WARN] Model discovery failed: {e}")
    
    safe_print("  [AI] Using absolute fallback: models/gemini-1.5-flash")
    return genai.GenerativeModel(model_name="models/gemini-1.5-flash")

model = get_best_model()

# Notion Client and Schema Discovery
notion = Client(auth=NOTION_TOKEN)

def get_db_properties():
    """Fetch database properties with robust error handling for SDK variations."""
    try:
        # Debug methods
        if hasattr(notion, "databases"):
            safe_print(f"  [NOTION] Databases methods: {dir(notion.databases)}")
        
        # 1. Try retrieve database
        db = notion.databases.retrieve(database_id=DATABASE_ID)
        props = list(db.get("properties", {}).keys())
        if props:
            safe_print(f"  [NOTION] Available properties: {', '.join(props)}")
            return props
            
        # 2. Fallback to searching first page if retrieve is empty
        safe_print("  [NOTION] Retrieve returned no properties. Trying search fallback...")
        query_method = getattr(notion.databases, "query", None)
        if query_method:
            res = query_method(database_id=DATABASE_ID, page_size=1)
            if res.get("results"):
                props = list(res["results"][0].get("properties", {}).keys())
                safe_print(f"  [NOTION] Found properties via search: {', '.join(props)}")
                return props
        else:
            safe_print("  [WARN] notion.databases.query is NOT available in this SDK version.")
            
    except Exception as e:
        safe_print(f"  [WARN] Could not retrieve DB schema: {e}")
    return []

DB_PROPS = get_db_properties()

# ニュースソース設定
RSS_FEEDS = [
    {"name": "Googleニュース (建設機械)", "url": "https://news.google.com/rss/search?q=%E5%BB%BA%E8%A8%AD%E6%A9%9F%E6%A2%B0&hl=ja&gl=JP&ceid=JP:ja"},
    {"name": "Googleニュース (鉱山機械)", "url": "https://news.google.com/rss/search?q=%E9%89%B1%E5%B1%B1%E6%A9%9F%E6%A2%B0&hl=ja&gl=JP&ceid=JP:ja"},
    {"name": "Googleニュース (コマツ/Komatsu)", "url": "https://news.google.com/rss/search?q=Komatsu+OR+%E3%82%B3%E3%83%9E%E3%83%84&hl=ja&gl=JP&ceid=JP:ja"},
    {"name": "Googleニュース (日立建機/Hitachi CM)", "url": "https://news.google.com/rss/search?q=Hitachi+Construction+Machinery+OR+%E6%97%A5%E7%AB%8B%E5%BB%BA%E6%A9%9F&hl=ja&gl=JP&ceid=JP:ja"},
    {"name": "KHL Construction News", "url": "https://news.google.com/rss/search?q=site:khl.com+International+Construction&hl=en-US&gl=US&ceid=US:en"}
]

def analyze_article_with_gemini(article_data):
    safe_print(f"  [AI] Analyzing: {article_data['title'][:40]}...")
    prompt = f"""あなたは建設機械業界の専門家です。
以下のニュースが建設・鉱山・農林機械、またはそのメーカーに関連するか判定してください。

【タイトル】: {article_data['title']}
【概要】: {article_data['summary']}

【出力形式】関連あれば以下のJSON、なければ 'null' とだけ出力してください。
{{
  "brand": "メーカー名",
  "segment": "製品区分",
  "region": "地域（Japan, Global, China等）",
  "summary_ja": "日本語要約（200字）"
}}"""
    try:
        response = model.generate_content(prompt)
        text = response.text.strip()
        
        if "null" in text.lower() and "{" not in text:
            safe_print("  [SKIP] AI judged as NOT relevant.")
            return None
            
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match:
            res = json.loads(match.group())
            safe_print(f"  [MATCH] Brand: {res.get('brand')}, Region: {res.get('region')}")
            return res
        else:
            safe_print(f"  [WARN] AI returned no JSON: {text[:50]}")
    except Exception as e:
        safe_print(f"  [ERROR] AI analysis failed: {e}")
    return None

def clean_multi_select(val):
    if not val: return [{"name": "Other"}]
    parts = [p.strip() for p in str(val).replace("、", ",").split(",")]
    return [{"name": p} for p in parts if p]

def save_to_notion(result, article_data):
    safe_print(f"  [NOTION] Attempting save to database...")
    try:
        # 重複チェック
        if "Source URL" in DB_PROPS:
            try:
                q = notion.databases.query(database_id=DATABASE_ID, filter={"property": "Source URL", "url": {"equals": article_data['link']}})
                if q["results"]:
                    safe_print("  [SKIP] Duplicate article.")
                    return False
            except: pass

        props = {}
        # ユーザー指定のプロパティ名に合わせる
        title_val = article_data['title'][:100]
        if "Title" in DB_PROPS: props["Title"] = {"title": [{"text": {"content": title_val}}]}
        elif "Name" in DB_PROPS: props["Name"] = {"title": [{"text": {"content": title_val}}]}
            
        if "Source URL" in DB_PROPS: props["Source URL"] = {"url": article_data['link']}
        if "Source Name" in DB_PROPS: props["Source Name"] = {"select": {"name": "RSS Search Collector"}}
        if "Brand" in DB_PROPS: props["Brand"] = {"multi_select": clean_multi_select(result.get("brand", "Other"))}
        if "Segment" in DB_PROPS: props["Segment"] = {"multi_select": clean_multi_select(result.get("segment", "Other"))}
        if "Region" in DB_PROPS: props["Region"] = {"multi_select": clean_multi_select(result.get("region", "Global"))}
            
        now = datetime.now().isoformat()
        if "Published Date" in DB_PROPS: props["Published Date"] = {"date": {"start": now}}
        elif "Date" in DB_PROPS: props["Date"] = {"date": {"start": now}}

        if "Summary" in DB_PROPS:
            props["Summary"] = {"rich_text": [{"text": {"content": result.get("summary_ja", "")[:2000]}}]}

        notion.pages.create(parent={"database_id": DATABASE_ID}, properties=props)
        safe_print("  [SUCCESS] Saved article.")
        return True
    except Exception as e:
        safe_print(f"  [ERROR] Notion Save: {e}")
        return False

def main():
    processed_count = 0
    safe_print("=== Starting Collection ===")
    for feed in RSS_FEEDS:
        try:
            resp = requests.get(feed['url'], headers={'User-Agent': 'Mozilla/5.0'}, timeout=20)
            if resp.status_code != 200: continue
            entries = feedparser.parse(resp.content).entries
            if entries:
                data = {"title": entries[0].title, "link": entries[0].link, "summary": entries[0].get("summary", entries[0].get("description", ""))}
                res = analyze_article_with_gemini(data)
                if res and save_to_notion(res, data):
                    processed_count += 1
                    if processed_count % 3 == 0: time.sleep(60)
            time.sleep(10)
        except Exception as e:
            safe_print(f"  [ERROR] Loop error: {e}")
    safe_print(f"\n=== Finished. Saved {processed_count} items. ===")

if __name__ == "__main__":
    main()
