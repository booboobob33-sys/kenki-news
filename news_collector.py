
import requests
import feedparser
import google.generativeai as genai
from notion_client import Client
from datetime import datetime
import time
import json
import os
from bs4 import BeautifulSoup
import re
import sys
import traceback

# Selenium imports
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# --- Configuration ---
# Priority: Environment Variables > Hardcoded Values
NOTION_TOKEN = os.getenv("NOTION_TOKEN", "")
DATABASE_ID = os.getenv("DATABASE_ID", "")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")

# AI Configuration
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel('gemini-2.0-flash') # Updated to latest stable available for generic tasks if needed

# Notion Client
notion = Client(auth=NOTION_TOKEN)

# News Sources
# Prioritizing: 1. Specialized Media, 2. Corporate Newsrooms (14 Brands), 3. Japanese Media, 4. General
RSS_FEEDS = [
    # --- Priority Specialized Industry Media ---
    {"priority": 1, "name": "International Construction (KHL)", "lang": "en", "url": "https://news.google.com/rss/search?q=site:khl.com+International+Construction&hl=en-US&gl=US&ceid=US:en"},
    {"priority": 1, "name": "Mining.com", "lang": "en", "url": "https://www.mining.com/feed/"},
    {"priority": 1, "name": "Construction Equipment Guide", "lang": "en", "url": "https://www.constructionequipmentguide.com/rss/news"},
    
    # --- Corporate Newsrooms (Global 14 Companies) ---
    {"priority": 2, "name": "Caterpillar (USA)", "lang": "en", "url": "https://news.google.com/rss/search?q=site:caterpillar.com/en/news&hl=en-US&gl=US&ceid=US:en"},
    {"priority": 2, "name": "Komatsu (Global)", "lang": "en", "url": "https://news.google.com/rss/search?q=site:komatsu.jp/en/newsroom+OR+site:komatsu.com/en/newsroom&hl=en-US&gl=US&ceid=US:en"},
    {"priority": 2, "name": "John Deere (USA)", "lang": "en", "url": "https://news.google.com/rss/search?q=site:deere.com/en/news&hl=en-US&gl=US&ceid=US:en"},
    {"priority": 2, "name": "Volvo CE (Sweden)", "lang": "en", "url": "https://news.google.com/rss/search?q=site:volvoce.com/global/en/news-and-events/news&hl=en-US&gl=US&ceid=US:en"},
    {"priority": 2, "name": "Hitachi CM (Japan)", "lang": "en", "url": "https://news.google.com/rss/search?q=site:hitachicm.com/global/en/news-and-media&hl=en-US&gl=US&ceid=US:en"},
    {"priority": 2, "name": "Liebherr (Germany)", "lang": "en", "url": "https://news.google.com/rss/search?q=site:liebherr.com/en/int/about-liebherr/news-and-press-releases&hl=en-US&gl=US&ceid=US:en"},
    {"priority": 2, "name": "Sany (China)", "lang": "en", "url": "https://news.google.com/rss/search?q=site:sanyglobal.com/news&hl=en-US&gl=US&ceid=US:en"},
    {"priority": 2, "name": "XCMG (China)", "lang": "en", "url": "https://news.google.com/rss/search?q=site:xcmgglobal.com/news&hl=en-US&gl=US&ceid=US:en"},
    {"priority": 2, "name": "Zoomlion (China)", "lang": "en", "url": "https://news.google.com/rss/search?q=site:zoomlion.com/news&hl=en-US&gl=US&ceid=US:en"},
    {"priority": 2, "name": "Kobelco (Japan)", "lang": "en", "url": "https://news.google.com/rss/search?q=site:kobelcocm-global.com/news&hl=en-US&gl=US&ceid=US:en"},
    {"priority": 2, "name": "Sumitomo CM (Japan)", "lang": "en", "url": "https://news.google.com/rss/search?q=site:sumitomokenki.com/news&hl=en-US&gl=US&ceid=US:en"},
    {"priority": 2, "name": "Doosan Bobcat (Korea)", "lang": "en", "url": "https://news.google.com/rss/search?q=site:bobcat.com/na/en/news-and-media+OR+site:doosanbobcat.com/en/media&hl=en-US&gl=US&ceid=US:en"},
    {"priority": 2, "name": "Kubota (Japan)", "lang": "en", "url": "https://news.google.com/rss/search?q=site:kubota.com/news&hl=en-US&gl=US&ceid=US:en"},
    {"priority": 2, "name": "JCB (UK)", "lang": "en", "url": "https://news.google.com/rss/search?q=site:jcb.com/en-gb/about/news&hl=en-US&gl=US&ceid=US:en"},
    
    # --- Specialized Japan Media ---
    {"priority": 3, "name": "Kensetsu News", "lang": "ja", "url": "https://news.google.com/rss/search?q=%E5%BB%BA%E8%A8%AD%E9%80%9A%E4%BF%A1%E6%96%B0%E8%81%9E&hl=ja&gl=JP&ceid=JP:ja"},
    {"priority": 3, "name": "Nikkei (Construction Machinery)", "lang": "ja", "url": "https://news.google.com/rss/search?q=site:nikkei.com+%E5%BB%BA%E8%A8%AD%E6%A9%9F%E6%A2%B0&hl=ja&gl=JP&ceid=JP:ja"},
    {"priority": 3, "name": "Nikkan Kogyo (Construction Machinery)", "lang": "ja", "url": "https://news.google.com/rss/search?q=site:nikkan.co.jp+%E5%BB%BA%E8%A8%AD%E6%A9%9F%E6%A2%B0&hl=ja&gl=JP&ceid=JP:ja"},
    
    # --- General Industry Search (Safety Net) ---
    {"priority": 4, "name": "Google News (Construction Machinery)", "lang": "ja", "url": "https://news.google.com/rss/search?q=%E5%BB%BA%E8%A8%AD%E6%A9%9F%E6%A2%B0&hl=ja&gl=JP&ceid=JP:ja"},
]

def safe_print(text):
    """Safely print text handling potential encoding errors on Windows."""
    try:
        print(text, flush=True)
    except UnicodeEncodeError:
        try:
            print(text.encode(sys.stdout.encoding, errors='replace').decode(sys.stdout.encoding), flush=True)
        except:
             print(text.encode('utf-8', errors='ignore'), flush=True)

def fetch_latest_article(feed_config):
    safe_print(f"Fetching news for {feed_config['name']}...")
    try:
        feed = feedparser.parse(feed_config['url'])
        if feed.entries:
            return feed.entries[0]
    except Exception as e:
        safe_print(f"Error fetching RSS: {e}")
    return None

def resolve_and_extract_content(url):
    """
    Resolve Google News redirect AND extract article text using Selenium.
    Returns: (resolved_url, extracted_text)
    """
    safe_print(f"Resolving & Extracting: {url}")
    
    if "news.google.com" not in url:
        return url, "Could not extract content (Direct Link)"

    text_content = ""
    resolved_url = url
    driver = None
    
    try:
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--log-level=3") 
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.set_page_load_timeout(30)
        
        driver.get(url)
        time.sleep(3) 
        resolved_url = driver.current_url
        if "google.com" in resolved_url:
             time.sleep(2)
             resolved_url = driver.current_url
             
        safe_print(f"Resolved URL: {resolved_url}")

        # Extract text from p tags
        try:
            paragraphs = driver.find_elements(By.TAG_NAME, "p")
            full_text = "\n".join([p.text for p in paragraphs if len(p.text) > 20])
            text_content = full_text
            safe_print(f"Extracted {len(text_content)} characters.")
        except Exception as e:
            text_content = "Failed to extract content (Element not found)."
            
    except Exception as e:
        safe_print(f"Extraction failed: {e}")
        text_content = "Failed to extract content."
        try:
             response = requests.get(url, allow_redirects=True, timeout=5)
             resolved_url = response.url
        except:
             pass
    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass
            
    return resolved_url, text_content

def analyze_article(title, body_text, lang):
    """Geminiを使用して記事を分析する。API制限対策のため入力テキストを2000文字に制限。"""
    # 3. AIへの送信データの最適化 (トークン節約と安定化)
    truncated_body = body_text[:2000] + "..." if len(body_text) > 2000 else body_text
    
    prompt = f"""
    You are an expert industry analyst in the construction machinery sector.
    Process this news article for a professional database.

    Title: {title}
    Original Language: {lang}
    Body Text: 
    {truncated_body}
    
    Tasks:
    1. **Translate Title**: Natural and professional Japanese title.
    2. **Relevance Check**: TRUE if it's significant industry news (Product launch, Partnership, Market trend, Policy, ESG). FALSE if it's just raw stock data, simple earnings summary without context, or irrelevant content.
    3. **Categorize Region**: Pick BEST from [Africa, North America, India, China, Japan, Southeast Asia, Europe, Global].
    4. **Categorize Segment**: Pick ALL relevant from [Utility, Forklift, Agriculture, Construction, Mining].
    5. **Categorize Brand**: List relevant manufacturers mentioned. Focus on the major 14: [Komatsu, Caterpillar, Hitachi, Volvo, Liebherr, John Deere, Sany, XCMG, Zoomlion, Kobelco, Sumitomo, Doosan, Bobcat, Kubota, JCB].
    6. **3-Line Summary**: Summarize the key points in Japanese in exactly 3 concise bullet points. Maintain professional tone.
    7. **Full Translation**: If the Original Language is NOT Japanese, provide a comprehensive natural Japanese translation of the Body Text. If it IS Japanese, return "Original is Japanese".
    
    Response Format (JSON Only):
    {{
        "translated_title": "...",
        "is_relevant_news": true/false,
        "region": "...",
        "segment": "...",
        "brand": "...",
        "summary": "...", 
        "full_translation": "..." 
    }}
    """
    
    try:
        response = model.generate_content(prompt)
        text = response.text.strip()
        if text.startswith("```json"):
            text = text[7:]
        if text.endswith("```"):
            text = text[:-3]
        text = text.strip()
        return json.loads(text)
    except Exception as e:
        if "429" in str(e) or "Quota Exceeded" in str(e) or "ResourceExhausted" in str(e):
            safe_print("CRITICAL: Gemini API Rate Limit (429) hit.")
            raise e
        safe_print(f"AI Analysis failed: {e}")
        return None

def get_published_date_property_name():
    try:
        results = notion.search(filter={"property": "object", "value": "page"}, page_size=1)
        if results["results"]:
            props = results["results"][0]["properties"]
            for key in props.keys():
                if key.lower().startswith("published date"):
                    return key
        return "Published Date"
    except:
        return "Published Date"

def get_existing_urls():
    """Fetch all unique 'Source URL' values from Notion."""
    safe_print("Fetching existing URLs from Notion to prevent duplicates...")
    existing_urls = set()
    try:
        cursor = None
        has_more = True
        while has_more:
            response = notion.search(
                start_cursor=cursor,
                page_size=100,
                filter={"property": "object", "value": "page"}
            )
            for page in response.get("results", []):
                url = page["properties"].get("Source URL", {}).get("url")
                if url:
                    existing_urls.add(url)
            
            has_more = response.get("has_more", False)
            cursor = response.get("next_cursor")
        
        safe_print(f"Found {len(existing_urls)} existing URLs.")
    except Exception as e:
        safe_print(f"Warning: Could not fetch existing URLs: {e}")
    return existing_urls

def save_to_notion(source_name, article_data, ai_data, resolved_url, original_text):
    """Notionにページを保存する。既存の不備修正（Database ID, Brand処理）を維持。"""
    safe_print(f"Saving to Notion: {ai_data['translated_title']}")
    
    try:
        if hasattr(article_data, 'published_parsed') and article_data.published_parsed:
            dt = datetime.fromtimestamp(time.mktime(article_data.published_parsed))
        else:
            dt = datetime.now()
        iso_date = dt.isoformat()
    except Exception as e:
        iso_date = datetime.now().isoformat()

    published_date_prop = get_published_date_property_name()

    summary_data = ai_data.get("summary", "")
    summary_text = "\n".join(summary_data) if isinstance(summary_data, list) else str(summary_data)

    children = [
        {"object": "block", "type": "heading_2", "heading_2": {"rich_text": [{"type": "text", "text": {"content": "要約"}}]}},
        {"object": "block", "type": "callout", "callout": {"rich_text": [{"type": "text", "text": {"content": summary_text}}], "icon": {"emoji": "💡"}}}
    ]
    
    # Translation
    translation = ai_data.get("full_translation", "")
    if translation and "Original is Japanese" not in translation:
        children.append({
            "object": "block",
            "type": "heading_2",
            "heading_2": {
                "rich_text": [{"type": "text", "text": {"content": "日本語訳"}}]
            }
        })
        for i in range(0, len(translation), 2000):
            chunk = translation[i:i+2000]
            children.append({
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [{"type": "text", "text": {"content": chunk}}]
                }
            })

    # 2. 2000文字制限とリンク対応 (Notion本文)
    children.append({"object": "block", "type": "heading_2", "heading_2": {"rich_text": [{"type": "text", "text": {"content": "原文"}}]}})
    if original_text:
        display_text = original_text
        if len(display_text) > 2000:
            display_text = display_text[:2000] + f"\n\n...（2000文字制限のため中略。全文は以下のリンクから確認してください）\n{resolved_url}"
        
        children.append({
            "object": "block", 
            "type": "paragraph", 
            "paragraph": {
                "rich_text": [{"type": "text", "text": {"content": display_text}, "annotations": {"italic": True}}]
            }
        })
    
    # Clean multi-select helper - Fixed for List or String input
    def clean_multi_select(val):
        if not val: return []
        if isinstance(val, list):
            # If AI returns a list of strings
            parts = [str(p).strip() for p in val]
        else:
            # If AI returns a single string with commas/delimiters
            parts = [p.strip() for p in str(val).replace("、", ",").split(",")]
        return [{"name": p} for p in parts if p]

    # Properties metadata
    properties = {
        "Title": {"title": [{"text": {"content": ai_data["translated_title"]}}]},
        "Source URL": {"url": resolved_url},
        "Source Name": {"select": {"name": source_name}},
        published_date_prop: {"date": {"start": iso_date}},
        "Region": {"multi_select": clean_multi_select(ai_data.get("region"))},
        "Segment": {"multi_select": clean_multi_select(ai_data.get("segment"))},
        "Brand": {"multi_select": clean_multi_select(ai_data.get("brand", ""))}
    }
    
    try:
        # Check DATABASE_ID
        if not DATABASE_ID:
            raise ValueError("DATABASE_ID is empty. Please check your environment variables.")
            
        new_page = notion.pages.create(
            parent={"database_id": DATABASE_ID}, # Fixed validation error by ensuring ID is used correctly
            properties=properties,
            children=children[:100] # Notion limit: up to 100 children in one request
        )
        safe_print(f"Successfully saved to Notion! URL: {new_page['url']}")
        return True
    except Exception as e:
        safe_print(f"Failed to save to Notion: {e}")
        if hasattr(e, 'body'):
             safe_print(f"Error detail: {e.body}")
        return False

def main():
    # Setup UTF-8 for Windows console
    try:
        if sys.stdout.encoding.lower() != 'utf-8':
            sys.stdout.reconfigure(encoding='utf-8')
    except:
        pass

    # 1. ループ処理と待機
    CHUNK_SIZE = 3
    LONG_SLEEP_SECONDS = 60
    
    saved_total = 0
    existing_urls = get_existing_urls()
    
    # 5. 優先巡回
    sorted_feeds = sorted(RSS_FEEDS, key=lambda x: x.get("priority", 99))
    
    try:
        for feed in sorted_feeds:
            lang = feed.get("lang", "en")
            safe_print(f"\nChecking source ({feed.get('priority')}): {feed['name']} ({lang})")
            article = fetch_latest_article(feed)
            
            if article:
                # 重複チェック
                if article.link in existing_urls:
                    safe_print(f"Article exists: Skip.")
                    continue
                
                resolved_url, body_text = resolve_and_extract_content(article.link)
                
                if resolved_url in existing_urls:
                    safe_print(f"Article exists: Skip.")
                    continue
                
                if len(body_text) < 100:
                     safe_print("Content too short, skipping.")
                     continue
                
                try:
                    ai_result = analyze_article(article.title, body_text, lang)
                    
                    if ai_result:
                        if ai_result.get("is_relevant_news", False):
                            success = save_to_notion(feed['name'], article, ai_result, resolved_url, body_text)
                            if success:
                                saved_total += 1
                                # 1. 3件処理するごとに1分間スリープ
                                if saved_total % CHUNK_SIZE == 0:
                                    safe_print(f"\nSaved {saved_total} items. Sleeping {LONG_SLEEP_SECONDS} seconds for API cooling...")
                                    time.sleep(LONG_SLEEP_SECONDS)
                                else:
                                    # 短い待機
                                    time.sleep(5)
                        else:
                            safe_print(f"Filtered (Irrelevant): {ai_result.get('translated_title')}")
                    else:
                         safe_print("AI Analysis Failed.")
                         
                except Exception as api_error:
                    # 429エラー等の要因で中断が必要な場合
                    if "429" in str(api_error) or "ResourceExhausted" in str(api_error):
                        safe_print("API Quota limit reached. Stopping and keeping successful saves.")
                        break
                    else:
                        safe_print(f"Unhandled error: {api_error}")
                        continue
            else:
                 safe_print("No feeds found.")
                
        safe_print(f"\nCompleted! Total Saved: {saved_total}")
    except Exception as e:
        safe_print(f"Main Loop Error: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main()
