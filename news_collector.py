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
    """Fetch database properties with deep diagnostics and literal mapping."""
    try:
        # 1. Try retrieve database meta
        safe_print(f"  [NOTION] Fetching database metadata for ID: {str(DATABASE_ID)[:8]}...")
        db = notion.databases.retrieve(database_id=DATABASE_ID)
        
        # Log properties found for debugging (very helpful if mismatches occur)
        all_found = list(db.get("properties", {}).keys())
        if all_found:
            safe_print(f"  [NOTION] Found actual properties in DB: {', '.join(all_found)}")
            # Log full titles for Published Date candidates
            for p in all_found:
                if "Date" in p or "日付" in p:
                    safe_print(f"  [DEBUG] Found Date candidate: {p}")
            return all_found
        else:
            safe_print(f"  [WARN] Notion API returned no properties for this DB ID.")
            
    except Exception as e:
        safe_print(f"  [WARN] Could not retrieve DB schema via API: {e}")
    
    return []

# Fetch actual property list once at start
ACTUAL_DB_PROPS = get_db_properties()

# --- Zero-Trust Fallback ---
# If API failed to return any properties, we use a hardcoded list of "best guess" verified names.
if not ACTUAL_DB_PROPS:
    safe_print(f"  [NOTION] API returned no properties. Using hardcoded backup for column names.")
    ACTUAL_DB_PROPS = [
        "Title", "Source URL", "Published Date（記事日付）", 
        "Brand", "Region", "Segment", "Source Name", "Summary"
    ]

def get_prop_name(candidates, default_if_empty=None):
    """Find the best matching property name from the actual DB columns."""
    for c in candidates:
        if c in ACTUAL_DB_PROPS: return c
    
    # Try fuzzy match (case/space/bracket insensitive)
    def simplify(s): return re.sub(r'[^a-zA-Z0-9\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FFF]', '', str(s)).lower()
    sc_list = [simplify(c) for c in candidates]
    for act in ACTUAL_DB_PROPS:
        sa = simplify(act)
        for sc in sc_list:
            if sc in sa or sa in sc:
                return act
    return default_if_empty

# Mapping based on user provided exact names
P_MAP = {
    "title": get_prop_name(["Title"], default_if_empty="Title"),
    "url": get_prop_name(["Source URL"], default_if_empty="Source URL"),
    "date": get_prop_name(["Published Date（記事日付）", "Published Date"], default_if_empty="Published Date（記事日付）"),
    "brand": get_prop_name(["Brand"], default_if_empty="Brand"),
    "segment": get_prop_name(["Segment"], default_if_empty="Segment"),
    "region": get_prop_name(["Region"], default_if_empty="Region"),
    "summary": get_prop_name(["Summary"], default_if_empty="Summary"),
    "source_name": get_prop_name(["Source Name"], default_if_empty="Source Name")
}

safe_print(f"  [NOTION] Final Mapped columns: " + ", ".join([f"{k}->{v}" for k,v in P_MAP.items() if v]))

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
    """Clean and format multi-select values, returning empty list if unknown."""
    if not val or str(val).lower() in ["none", "不明", "other"]: 
        return []
    parts = [p.strip() for p in str(val).replace("、", ",").split(",")]
    return [{"name": p} for p in parts if p]

def save_to_notion(result, article_data):
    safe_print(f"  [NOTION] Attempting save to database...")
    
    # Construct base properties
    props = {}
    title_col = P_MAP["title"]
    url_col = P_MAP["url"]
    date_col = P_MAP["date"]
    brand_col = P_MAP["brand"]
    segment_col = P_MAP["segment"]
    region_col = P_MAP["region"]
    summary_col = P_MAP["summary"]
    source_name_col = P_MAP["source_name"]

    # Populate data
    if title_col: props[title_col] = {"title": [{"text": {"content": article_data['title'][:100]}}]}
    if url_col: props[url_col] = {"url": article_data['link']}
    if source_name_col: props[source_name_col] = {"select": {"name": "RSS Search Collector"}}
    
    # AI Results
    brand_tags = clean_multi_select(result.get("brand"))
    if brand_tags and brand_col: props[brand_col] = {"multi_select": brand_tags}
    
    segment_tags = clean_multi_select(result.get("segment"))
    if segment_tags and segment_col: props[segment_col] = {"multi_select": segment_tags}
    
    region_tags = clean_multi_select(result.get("region"))
    if region_tags and region_col: props[region_col] = {"multi_select": region_tags}
    
    if date_col: props[date_col] = {"date": {"start": datetime.now().isoformat()}}
    
    if summary_col:
        text = result.get("summary_ja") or article_data.get("summary", "")
        props[summary_col] = {"rich_text": [{"text": {"content": str(text)[:2000]}}]}

    # Attempt save with self-correction retry loop
    max_retries = 5
    for attempt in range(max_retries):
        try:
            # Duplicate check (only on first successful retry attempt if URL column exists)
            if url_col in props and attempt == 0:
                query_method = getattr(notion.databases, "query", None)
                if query_method:
                    q = query_method(database_id=DATABASE_ID, filter={"property": url_col, "url": {"equals": article_data['link']}})
                    if q["results"]:
                        safe_print("  [SKIP] Duplicate article.")
                        return False

            notion.pages.create(parent={"database_id": DATABASE_ID}, properties=props)
            safe_print("  [SUCCESS] Saved article.")
            return True

        except Exception as e:
            err_msg = str(e)
            # Check if error is about a missing property
            match = re.search(r"Property ['\"](.+?)['\"] is not a property", err_msg)
            if match:
                bad_prop = match.group(1)
                safe_print(f"  [FIX] Removing non-existent property and retrying: {bad_prop}")
                if bad_prop in props:
                    del props[bad_prop]
                    continue # Retry with remaining properties
            
            safe_print(f"  [ERROR] Notion Save Failed: {err_msg}")
            return False
    
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
    safe_print(f"\n=== Finished. Successfully saved {processed_count} news items to Notion. ===")

if __name__ == "__main__":
    main()
