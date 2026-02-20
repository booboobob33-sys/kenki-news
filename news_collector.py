import requests
import feedparser
import google.generativeai as genai
from notion_client import Client
from datetime import datetime
import time
import json
import os
import re
import sys
import traceback

def safe_print(text):
    """Safely print text handling potential encoding errors."""
    try:
        print(text, flush=True)
    except Exception:
        try:
            print(str(text).encode('utf-8', errors='replace').decode('utf-8'), flush=True)
        except:
             pass

# --- Configuration ---
NOTION_TOKEN = os.getenv("NOTION_TOKEN", "")
DATABASE_ID = os.getenv("DATABASE_ID", "")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")

# Diagnostic Log for environment
def check_env():
    mask = lambda s: s[:4] + "***" if s else "MISSING"
    print(f"--- Environment Check ---")
    print(f"NOTION_TOKEN: {mask(NOTION_TOKEN)}")
    print(f"DATABASE_ID: {mask(DATABASE_ID)}")
    print(f"GEMINI_API_KEY: {mask(GEMINI_API_KEY)}")
    print(f"-------------------------")

# AI Configuration (v1 REST for Paid Tier stability)
safe_print(f"SDK Version: {genai.__version__}")
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
        
        # Fallback to the first available flash or pro model
        for m in models:
            if "flash" in m or "pro" in m:
                safe_print(f"  [AI INFO] Falling back to available model: {m}")
                return genai.GenerativeModel(model_name=m)
    except Exception as e:
        safe_print(f"  [AI WARN] Could not list models: {e}")
    
    # Absolute fallback
    return genai.GenerativeModel(model_name="gemini-1.5-flash")

model = get_best_model()

# Notion Client
notion = Client(auth=NOTION_TOKEN)

# ニュースソース設定 (Googleニュース検索ベース。ロボット判定回避のためUser-Agentを厳重に指定)
# 検索ワード: 建設機械, 鉱山機械, コマツ, 日立建機, Caterpillar, Komatsu
RSS_FEEDS = [
    {"name": "Googleニュース (建設機械)", "url": "https://news.google.com/rss/search?q=%E5%BB%BA%E8%A8%AD%E6%A9%9F%E6%A2%B0&hl=ja&gl=JP&ceid=JP:ja"},
    {"name": "Googleニュース (鉱山機械)", "url": "https://news.google.com/rss/search?q=%E9%89%B1%E5%B1%B1%E6%A9%9F%E6%A2%B0&hl=ja&gl=JP&ceid=JP:ja"},
    {"name": "Googleニュース (コマツ/Komatsu)", "url": "https://news.google.com/rss/search?q=Komatsu+OR+%E3%82%B3%E3%83%9E%E3%83%84&hl=ja&gl=JP&ceid=JP:ja"},
    {"name": "Googleニュース (日立建機/Hitachi CM)", "url": "https://news.google.com/rss/search?q=Hitachi+Construction+Machinery+OR+%E6%97%A5%E7%AB%8B%E5%BB%BA%E6%A9%9F&hl=ja&gl=JP&ceid=JP:ja"},
    {"name": "KHL Construction News", "url": "https://news.google.com/rss/search?q=site:khl.com+International+Construction&hl=en-US&gl=US&ceid=US:en"},
    {"name": "Mining.com", "url": "https://www.mining.com/feed/"}
]


def fetch_latest_article(feed_config):
    safe_print(f"\n[FETCH] Processing: {feed_config['name']}")
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36',
            'Accept-Language': 'ja,en-US;q=0.9,en;q=0.8',
            'Referer': 'https://www.google.com/'
        }
        response = requests.get(feed_config['url'], headers=headers, timeout=20)
        
        if response.status_code != 200:
            safe_print(f"  [ERROR] HTTP {response.status_code} for {feed_config['name']}")
            return None

        feed = feedparser.parse(response.content)
        if feed.entries:
            # 取得できた記事の数をログ
            safe_print(f"  [INFO] Found {len(feed.entries)} entries. Analyzing latest article...")
            entry = feed.entries[0]
            safe_print(f"  [INFO] Title: {entry.title}")
            
            # 概要（summary/description）が空でないか確認
            summary = entry.get("summary", entry.get("description", ""))
            if not summary or len(summary) < 10:
                safe_print("  [WARN] Summary is too short/empty. Will rely on title.")
                
            return {
                "title": entry.title,
                "link": entry.link,
                "summary": summary
            }
        else:
            safe_print(f"  [WARN] No entries found for {feed_config['name']}.")
    except Exception as e:
        safe_print(f"  [ERROR] Fetch error: {e}")
    return None

def analyze_article_with_gemini(article_data):
    safe_print(f"  [AI] Analyzing relevance via Gemini...")
    
    prompt = f"""
あなたは建設機械業界の専門アナリストです。
以下のニュース（タイトルと概要）を分析し、**建設機械、鉱山機械、農業機械、林業機械**のいずれかの製品、メーカー、または業界動向に直接関連する「重要またはポジティブ」な記事か判定してください。

【タイトル】: {article_data['title']}
【概要】: {article_data['summary']}

判定基準:
- 関連がある場合のみ、指定のJSON形式で出力してください。
- 関連がない（一般ニュース、関係ない株式ニュース等）場合は、理由を添えずに 'null' とだけ出力してください。

出力フォーマット（関連がある場合のみ）:
{{
  "brand": "主要メーカー名（Komatsu, Caterpillar, Hitachi, John Deere, Volvo, Liebherr, Sany, XCMG, Kobelco, Sumitomo, Kubota, JCB 等。不明な場合は 'Other'）",
  "segment": "製品区分（Excavator, Loader, Mining, Forestry, Utility 等）",
  "summary_ja": "内容の簡潔な日本語要約（200文字以内）",
  "confidence": "判定の自信（0.0-1.0）"
}}
"""
    try:
        response = model.generate_content(prompt)
        raw_text = response.text.strip()
        
        # 診断用: AIの生の回答をログ
        # safe_print(f"  [AI DEBUG] Raw response: {raw_text[:100]}...")
        
        if "null" in raw_text.lower() and "{" not in raw_text:
            safe_print("  [SKIP] AI judged as NOT relevant.")
            return None
        
        json_match = re.search(r"\{.*\}", raw_text, re.DOTALL)
        if json_match:
            result = json.loads(json_match.group())
            safe_print(f"  [MATCH] Brand: {result.get('brand')}, Segment: {result.get('segment')}")
            return result
        else:
            safe_print(f"  [ERROR] AI returned non-JSON text: {raw_text[:100]}")
            return None
            
    except Exception as e:
        if "429" in str(e):
            raise e
        safe_print(f"  [ERROR] AI Error: {e}")
    return None

def clean_multi_select(val):
    if not val: return []
    if isinstance(val, list):
        parts = [str(p).strip() for p in val]
    else:
        parts = [p.strip() for p in str(val).replace("、", ",").split(",")]
    return [{"name": p} for p in parts if p]

def save_to_notion(result, article_data):
    safe_print(f"  [NOTION] Saving: {article_data['title'][:40]}...")
    try:
        # 重複チェック (Source URL プロパティ)
        query = notion.databases.query(
            database_id=DATABASE_ID,
            filter={"property": "Source URL", "url": {"equals": article_data['link']}}
        )
        if query["results"]:
            safe_print("  [SKIP] Duplicate URL found in Notion.")
            return False

        notion.pages.create(
            parent={"database_id": DATABASE_ID},
            properties={
                "Title": {"title": [{"text": {"content": article_data['title'][:100]}}]},
                "Source Name": {"select": {"name": "RSS Search Collector"}},
                "Source URL": {"url": article_data['link']},
                "Summary": {"rich_text": [{"text": {"content": result.get("summary_ja", "")[:2000]}}]},
                "Brand": {"multi_select": clean_multi_select(result.get("brand", "Other"))},
                "Segment": {"multi_select": clean_multi_select(result.get("segment", "Other"))},
                "Date": {"date": {"start": datetime.now().isoformat()}}
            }
        )
        safe_print("  [SUCCESS] Saved to Notion.")
        return True
    except Exception as e:
        safe_print(f"  [ERROR] Notion Save Error: {e}")
        # 診断用: トレースバックを出力
        # traceback.print_exc()
        return False

def main():
    safe_print("=== News Collection (Diagnostic Mode) ===")
    check_env()
    processed_count = 0
    
    for feed_config in RSS_FEEDS:
        article_data = fetch_latest_article(feed_config)
        
        if article_data:
            try:
                result = analyze_article_with_gemini(article_data)
                
                if result:
                    if save_to_notion(result, article_data):
                        processed_count += 1
                        # Paid Tier Quota Management
                        if processed_count % 3 == 0:
                            safe_print("  [WAIT] Sleeping 60s for quota...")
                            time.sleep(60)
                
            except Exception as e:
                if "429" in str(e):
                    safe_print("  [WAIT] 429 Hit. Waiting 70s...")
                    time.sleep(70)
                    try:
                        result = analyze_article_with_gemini(article_data)
                        if result: save_to_notion(result, article_data)
                    except: pass
        
        # サイト負荷に配慮して1件ごとに待機
        time.sleep(10)

    safe_print(f"\n=== Process Finished. Saved {processed_count} items. ===")

if __name__ == "__main__":
    main()
