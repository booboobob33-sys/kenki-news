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
    # --- Specialized Industry Media ---
    {"name": "Mining.com (Mining Machine)", "url": "https://www.mining.com/tag/mining-machinery/feed/"},
    {"name": "Construction Equipment Guide", "url": "https://www.constructionequipmentguide.com/rss/"},
    {"name": "KHL Construction News", "url": "https://news.google.com/rss/search?q=site:khl.com+International+Construction&hl=en-US&gl=US&ceid=US:en"},
    
    # --- Major 14 Manufacturers (Targeted Searches) ---
    {"name": "CAT (Caterpillar)", "url": "https://news.google.com/rss/search?q=Caterpillar+Construction+Mining+News&hl=en-US&gl=US&ceid=US:en"},
    {"name": "Komatsu (Global)", "url": "https://news.google.com/rss/search?q=Komatsu+Mining+Construction+News&hl=en-US&gl=US&ceid=US:en"},
    {"name": "John Deere (Construction)", "url": "https://news.google.com/rss/search?q=John+Deere+Construction+News&hl=en-US&gl=US&ceid=US:en"},
    {"name": "XCMG / Sany News", "url": "https://news.google.com/rss/search?q=%22XCMG%22+OR+%22Sany+Group%22+Construction&hl=en-US&gl=US&ceid=US:en"},
    {"name": "Volvo CE / Liebherr", "url": "https://news.google.com/rss/search?q=%22Volvo+CE%22+OR+%22Liebherr%22+Machinery&hl=en-US&gl=US&ceid=US:en"},
    {"name": "Hitachi CM / Bobcat", "url": "https://news.google.com/rss/search?q=%22Hitachi+Construction+Machinery%22+OR+%22Bobcat%22+News&hl=en-US&gl=US&ceid=US:en"},
    {"name": "Zoomlion / Kubota News", "url": "https://news.google.com/rss/search?q=%22Zoomlion%22+OR+%22Kubota%22+Construction&hl=en-US&gl=US&ceid=US:en"},
    {"name": "JCB / Kobelco / Sumitomo", "url": "https://news.google.com/rss/search?q=%22JCB%22+OR+%22Kobelco%22+OR+%22Sumitomo+Construction+Machinery%22&hl=en-US&gl=US&ceid=US:en"},

    # --- Japanese Media & Specialized Keywords ---
    {"name": "Googleニュース (建機/鉱山機械)", "url": "https://news.google.com/rss/search?q=%E5%BB%BA%E8%A8%AD%E6%A9%9F%E6%A2%B0+OR+%E9%89%B1%E5%B1%B1%E6%A9%9F%E6%A2%B0&hl=ja&gl=JP&ceid=JP:ja"},
    {"name": "日経ニュース (重機/自動化)", "url": "https://news.google.com/rss/search?q=site:nikkei.com+%E5%BB%BA%E8%A8%AD%E6%A9%9F%E6%A2%B0+OR+%E8%87%AA%E5%8B%95%E5%8C%96&hl=ja&gl=JP&ceid=JP:ja"},
    {"name": "Googleニュース (脱炭素/電動建機)", "url": "https://news.google.com/rss/search?q=%E9%9B%BB%E5%8B%95%E5%BB%BA%E8%A8%AD%E6%A9%9F%E6%A2%B0+OR+%E8%84%B1%E7%82%AD%E7%B4%A0+%E5%BB%BA%E6%A9%9F&hl=ja&gl=JP&ceid=JP:ja"}
]

def analyze_article_with_gemini(article_data, page_text=""):
    safe_print(f"  [AI] Analyzing: {article_data['title'][:40]}...")
    
    # 徹底的にタグ除去したテキストを作成
    raw_content = page_text if len(page_text) > 200 else article_data['summary']
    clean_content = re.sub(r'<[^>]+>', '', raw_content) # 念押し
    clean_content = clean_content.replace("&nbsp;", " ").replace("&quot;", "\"").strip()

    prompt = f"""あなたは建設・鉱山機械業界の専門家です。
以下の記事内容を分析し、指定のJSON形式で日本語で出力してください。

【重要ルール】
・HTMLタグ（<a>等）、生のURL、[URL]のようなブラケットメタデータは絶対に出力しないでください。
・きれいな日本語のみで出力してください。

【タイトル】: {article_data['title']}
【記事内容】: {clean_content[:5000]}

【出力形式】
関連がない場合は 'null' とだけ出力。
関連がある場合は以下のJSONのみを出力：
{{
  "brand": "メーカー名",
  "segment": "製品区分",
  "region": "地域（Japan, Global等）",
  "bullet_summary": "3行以内の簡潔な箇条書き日本語要約",
  "full_body": "本文の転記または翻訳（HTML/URLを一切含まない日本語）"
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
            # AIがURLなどを混ぜてきた場合の保険
            if "full_body" in res:
                res["full_body"] = re.sub(r'<[^>]+>', '', str(res["full_body"]))
            safe_print(f"  [MATCH] Clean content generated.")
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
    
    # Remove summary from properties (we will write it to the page body instead)
    if summary_col in props:
        del props[summary_col]

    # Construct Page Body (Children)
    children = []
    
    # Summary Section
    bullets = result.get("bullet_summary", "").strip()
    if bullets:
        children.append({
            "object": "block",
            "type": "heading_2",
            "heading_2": {"rich_text": [{"type": "text", "text": {"content": "【要約】"}}]}
        })
        bullet_list = [b.strip("- •*") for b in bullets.split("\n") if b.strip()]
        for b in bullet_list[:3]:
            children.append({
                "object": "block",
                "type": "bulleted_list_item",
                "bulleted_list_item": {"rich_text": [{"type": "text", "text": {"content": b[:2000]}}]}
            })

    # Body Section
    body_text = result.get("full_body", "").strip()
    if body_text:
        children.append({
            "object": "block",
            "type": "heading_2",
            "heading_2": {"rich_text": [{"type": "text", "text": {"content": "【本文引用/翻訳】"}}]}
        })
        
        display_body = body_text
        needs_link = False
        if len(display_body) > 1900:
            display_body = display_body[:1800] + "..."
            needs_link = True
            
        children.append({
            "object": "block",
            "type": "paragraph",
            "paragraph": {"rich_text": [{"type": "text", "text": {"content": display_body}}]}
        })
        
        if needs_link:
            children.append({
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [
                        {"type": "text", "text": {"content": "（続きはサイトへ）\n"}},
                        {"type": "text", "text": {"content": article_data['link'], "link": {"url": article_data['link']}}}
                    ]
                }
            })

    # Attempt save with retry loop
    max_retries = 3
    for attempt in range(max_retries):
        try:
            if url_col in props and attempt == 0:
                query_method = getattr(notion.databases, "query", None)
                if query_method:
                    q = query_method(database_id=DATABASE_ID, filter={"property": url_col, "url": {"equals": article_data['link']}})
                    if q["results"]:
                        safe_print("  [SKIP] Duplicate article.")
                        return False

            notion.pages.create(parent={"database_id": DATABASE_ID}, properties=props, children=children)
            safe_print("  [SUCCESS] Saved article with clean content.")
            return True

        except Exception as e:
            err_msg = str(e)
            match = re.search(r"Property ['\"](.+?)['\"] is not a property", err_msg)
            if match:
                bad_prop = match.group(1)
                safe_print(f"  [FIX] Removing {bad_prop} and retrying...")
                if bad_prop in props:
                    del props[bad_prop]
                    continue 
            safe_print(f"  [ERROR] Notion Save Failed: {err_msg}")
            return False
    return False

def get_page_text(url):
    """Fetch and extract clean text from a URL."""
    try:
        from bs4 import BeautifulSoup
        resp = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=15)
        if resp.status_code != 200: return ""
        soup = BeautifulSoup(resp.content, 'html.parser')
        
        for s in soup(["script", "style", "nav", "header", "footer"]):
            s.decompose()
            
        text = soup.get_text()
        return " ".join(text.split())[:10000]
    except:
        return ""

def main():
    processed_count = 0
    safe_print("=== Starting Collection ===")
    for feed in RSS_FEEDS:
        try:
            resp = requests.get(feed['url'], headers={'User-Agent': 'Mozilla/5.0'}, timeout=20)
            if resp.status_code != 200: continue
            entries = feedparser.parse(resp.content).entries
            
            # Process up to 15 entries per feed to avoid overload
            for entry in entries[:15]:
                data = {
                    "title": entry.title, 
                    "link": entry.link, 
                    "summary": entry.get("summary", entry.get("description", ""))
                }
                
                # Fetch full text
                page_text = get_page_text(data['link'])
                
                res = analyze_article_with_gemini(data, page_text)
                if res and save_to_notion(res, data):
                    processed_count += 1
                    if processed_count % 3 == 0: time.sleep(60)
                
                time.sleep(15) # Between articles
            
            time.sleep(5) # Between feeds
        except Exception as e:
            safe_print(f"  [ERROR] Loop error: {e}")
    safe_print(f"\n=== Finished. Successfully saved {processed_count} news items to Notion. ===")

if __name__ == "__main__":
    main()
