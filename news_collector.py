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

# --- Configuration ---
NOTION_TOKEN = os.getenv("NOTION_TOKEN", "")
DATABASE_ID = os.getenv("DATABASE_ID", "")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")

# AI Configuration (v1 REST for Paid Tier stability)
genai.configure(api_key=GEMINI_API_KEY, transport='rest')
model = genai.GenerativeModel(model_name="gemini-1.5-flash")

# Notion Client
notion = Client(auth=NOTION_TOKEN)

# ニュースソース設定 (RSSフィードに一本化)
RSS_FEEDS = [
    {"name": "Yahooニュース (主要)", "url": "https://news.yahoo.co.jp/rss/topics/top-pickups.xml", "category": "General"},
    {"name": "Yahooニュース (IT・科学)", "url": "https://news.yahoo.co.jp/rss/topics/it.xml", "category": "General"},
    {"name": "Yahooニュース (経済)", "url": "https://news.yahoo.co.jp/rss/topics/business.xml", "category": "General"},
    {"name": "KHL Construction News", "url": "https://news.google.com/rss/search?q=site:khl.com+International+Construction&hl=en-US&gl=US&ceid=US:en", "category": "Construction"},
    {"name": "Mining.com", "url": "https://www.mining.com/feed/", "category": "Mining"}
]

def safe_print(text):
    """Safely print text handling potential encoding errors (Windows/Linux)."""
    try:
        print(text, flush=True)
    except Exception:
        try:
            print(str(text).encode('utf-8', errors='replace').decode('utf-8'), flush=True)
        except:
             pass

def fetch_latest_article(feed_config):
    safe_print(f"Fetching news for {feed_config['name']}...")
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(feed_config['url'], headers=headers, timeout=20)
        safe_print(f"  HTTP Status: {response.status_code}")
        
        if response.status_code != 200:
            return None

        feed = feedparser.parse(response.content)
        if feed.entries:
            entry = feed.entries[0]
            safe_print(f"  Found article: {entry.title}")
            return {
                "title": entry.title,
                "link": entry.link,
                "summary": entry.get("summary", entry.get("description", ""))
            }
    except Exception as e:
        safe_print(f"Error fetching RSS: {e}")
    return None

def analyze_article_with_gemini(article_data):
    # 本文スクレイピングを廃止し、RSSのタイトルと概要からAI判定
    prompt = f"""
以下のニュース（タイトルと概要）を分析し、建設機械、鉱山機械、農業機械、林業機械に関連するポジティブまたは重要な記事か判定してください。
関連がある場合のみ、指定のJSON形式で出力してください。

【タイトル】: {article_data['title']}
【概要】: {article_data['summary']}

出力フォーマット（関連がない、または重要でない場合は null とだけ出力）:
{{
  "brand": "メーカー名（Komatsu, Caterpillar, Hitachi 等。不明な場合は 'Other'）",
  "segment": "製品区分（Excavator, Loader, Mining, Forestry 等）",
  "summary_ja": "内容の簡潔な日本語要約（200文字以内）"
}}
"""
    try:
        response = model.generate_content(prompt)
        text = response.text.strip()
        if "null" in text.lower() and "{" not in text:
            return None
        
        json_match = re.search(r"\{.*\}", text, re.DOTALL)
        if json_match:
            return json.loads(json_match.group())
    except Exception as e:
        if "429" in str(e):
            raise e
        safe_print(f"AI Analysis Error: {e}")
    return None

def clean_multi_select(val):
    if not val: return []
    if isinstance(val, list):
        parts = [str(p).strip() for p in val]
    else:
        parts = [p.strip() for p in str(val).replace("、", ",").split(",")]
    return [{"name": p} for p in parts if p]

def save_to_notion(result, article_data):
    try:
        # 重複チェック (Source URL プロパティ)
        query = notion.databases.query(
            database_id=DATABASE_ID,
            filter={"property": "Source URL", "url": {"equals": article_data['link']}}
        )
        if query["results"]:
            safe_print("  Article already exists in Notion. Skipping.")
            return False

        notion.pages.create(
            parent={"database_id": DATABASE_ID},
            properties={
                "Title": {"title": [{"text": {"content": article_data['title'][:100]}}]},
                "Source Name": {"select": {"name": "RSS Collector"}},
                "Source URL": {"url": article_data['link']},
                "Summary": {"rich_text": [{"text": {"content": result.get("summary_ja", "")[:2000]}}]},
                "Brand": {"multi_select": clean_multi_select(result.get("brand", "Other"))},
                "Segment": {"multi_select": clean_multi_select(result.get("segment", "Other"))},
                "Date": {"date": {"start": datetime.now().isoformat()}}
            }
        )
        safe_print(f"  Successfully saved to Notion: {article_data['title'][:50]}...")
        return True
    except Exception as e:
        safe_print(f"Error saving to Notion: {e}")
        return False

def main():
    safe_print("=== News Collection Started (RSS Comprehensive Mode) ===")
    processed_count = 0
    
    for feed_config in RSS_FEEDS:
        article_data = fetch_latest_article(feed_config)
        
        if article_data:
            try:
                result = analyze_article_with_gemini(article_data)
                
                if result:
                    if save_to_notion(result, article_data):
                        processed_count += 1
                        # 3件ごとに60秒休憩 (Quota対策)
                        if processed_count % 3 == 0:
                            safe_print("Taking a 60s break to respect API quota...")
                            time.sleep(60)
                else:
                    safe_print("  Not relevant to machinery. Skipping.")
            
            except Exception as e:
                if "429" in str(e):
                    safe_print("Quota exceeded. Waiting 70 seconds before retry...")
                    time.sleep(70)
                    try:
                        result = analyze_article_with_gemini(article_data)
                        if result:
                            save_to_notion(result, article_data)
                    except:
                        pass
        
        time.sleep(5)

    safe_print(f"=== Process Completed. Saved {processed_count} news items. ===")

if __name__ == "__main__":
    main()
