
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
# Using gemini-2.5-flash which had working quota
model = genai.GenerativeModel('gemini-2.5-flash')

# Notion Client
notion = Client(auth=NOTION_TOKEN)

# News Sources
# Mixing Global (EN) and Japan (JP) sources
RSS_FEEDS = [
    # --- Specialized Industry Media (Deep Professional Info) ---
    {"name": "International Construction (KHL)", "lang": "en", "url": "https://news.google.com/rss/search?q=site:khl.com+International+Construction&hl=en-US&gl=US&ceid=US:en"},
    {"name": "Mining.com", "lang": "en", "url": "https://www.mining.com/feed/"},
    {"name": "Construction Equipment Guide", "lang": "en", "url": "https://www.constructionequipmentguide.com/rss/news"},
    
    # --- Corporate Newsrooms (Global 14 Companies) ---
    {"name": "Caterpillar (USA)", "lang": "en", "url": "https://news.google.com/rss/search?q=site:caterpillar.com/en/news&hl=en-US&gl=US&ceid=US:en"},
    {"name": "Komatsu (Global)", "lang": "en", "url": "https://news.google.com/rss/search?q=site:komatsu.jp/en/newsroom+OR+site:komatsu.com/en/newsroom&hl=en-US&gl=US&ceid=US:en"},
    {"name": "John Deere (USA)", "lang": "en", "url": "https://news.google.com/rss/search?q=site:deere.com/en/news&hl=en-US&gl=US&ceid=US:en"},
    {"name": "Volvo CE (Sweden)", "lang": "en", "url": "https://news.google.com/rss/search?q=site:volvoce.com/global/en/news-and-events/news&hl=en-US&gl=US&ceid=US:en"},
    {"name": "Hitachi CM (Japan)", "lang": "en", "url": "https://news.google.com/rss/search?q=site:hitachicm.com/global/en/news-and-media&hl=en-US&gl=US&ceid=US:en"},
    {"name": "Liebherr (Germany)", "lang": "en", "url": "https://news.google.com/rss/search?q=site:liebherr.com/en/int/about-liebherr/news-and-press-releases&hl=en-US&gl=US&ceid=US:en"},
    {"name": "Sany (China)", "lang": "en", "url": "https://news.google.com/rss/search?q=site:sanyglobal.com/news&hl=en-US&gl=US&ceid=US:en"},
    {"name": "XCMG (China)", "lang": "en", "url": "https://news.google.com/rss/search?q=site:xcmgglobal.com/news&hl=en-US&gl=US&ceid=US:en"},
    {"name": "Zoomlion (China)", "lang": "en", "url": "https://news.google.com/rss/search?q=site:zoomlion.com/news&hl=en-US&gl=US&ceid=US:en"},
    {"name": "Kobelco (Japan)", "lang": "en", "url": "https://news.google.com/rss/search?q=site:kobelcocm-global.com/news&hl=en-US&gl=US&ceid=US:en"},
    {"name": "Sumitomo CM (Japan)", "lang": "en", "url": "https://news.google.com/rss/search?q=site:sumitomokenki.com/news&hl=en-US&gl=US&ceid=US:en"},
    {"name": "Doosan Bobcat (Korea)", "lang": "en", "url": "https://news.google.com/rss/search?q=site:bobcat.com/na/en/news-and-media+OR+site:doosanbobcat.com/en/media&hl=en-US&gl=US&ceid=US:en"},
    {"name": "Kubota (Japan)", "lang": "en", "url": "https://news.google.com/rss/search?q=site:kubota.com/news&hl=en-US&gl=US&ceid=US:en"},
    {"name": "JCB (UK)", "lang": "en", "url": "https://news.google.com/rss/search?q=site:jcb.com/en-gb/about/news&hl=en-US&gl=US&ceid=US:en"},
    
    # --- Specialized Japan Media ---
    {"name": "Kensetsu News", "lang": "ja", "url": "https://news.google.com/rss/search?q=%E5%BB%BA%E8%A8%AD%E9%80%9A%E4%BF%A1%E6%96%B0%E8%81%9E&hl=ja&gl=JP&ceid=JP:ja"},
    {"name": "Nikkei (Construction Machinery)", "lang": "ja", "url": "https://news.google.com/rss/search?q=site:nikkei.com+%E5%BB%BA%E8%A8%AD%E6%A9%9F%E6%A2%B0&hl=ja&gl=JP&ceid=JP:ja"},
    {"name": "Nikkan Kogyo (Construction Machinery)", "lang": "ja", "url": "https://news.google.com/rss/search?q=site:nikkan.co.jp+%E5%BB%BA%E8%A8%AD%E6%A9%9F%E6%A2%B0&hl=ja&gl=JP&ceid=JP:ja"},
    
    # --- General Industry Search (Safety Net) ---
    {"name": "Google News (Construction Machinery)", "lang": "ja", "url": "https://news.google.com/rss/search?q=%E5%BB%BA%E8%A8%AD%E6%A9%9F%E6%A2%B0&hl=ja&gl=JP&ceid=JP:ja"},
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
        # Add user agent to avoid blocking
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.set_page_load_timeout(30) # 30 seconds timeout
        
        driver.get(url)
        
        # Wait for potential redirect
        time.sleep(3) 
        resolved_url = driver.current_url
        if "google.com" in resolved_url:
             # Wait a bit longer if still on google
             time.sleep(2)
             resolved_url = driver.current_url
             
        safe_print(f"Resolved URL: {resolved_url}")

        # Extract text from p tags
        try:
            paragraphs = driver.find_elements(By.TAG_NAME, "p")
            text_content = "\n".join([p.text for p in paragraphs if len(p.text) > 20])
            
            # Limit extracted text to ~500 chars to avoid token limits during test
            if len(text_content) > 500:
                text_content = text_content[:500] + "..."
        except Exception as e:
            text_content = "Failed to extract content (Element not found)."
            
    except Exception as e:
        safe_print(f"Extraction failed: {e}")
        text_content = "Failed to extract content."
        try:
             # Fallback resolving
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
    """
    Analyze article using Gemini.
    - Translate Title
    - Categorize
    - Generate 3-line Summary
    - Full Translation (if extraction successful)
    """
    
    prompt = f"""
    You are an expert analyst. Process this news article.
    
    Title: {title}
    Original Language: {lang}
    Body Text (Excerpt): 
    {body_text}
    
    Tasks:
    1. **Translate Title**: Natural Japanese title.
    2. **Relevance Check**: TRUE if it's significant news (Product launch, Partnership, Market trend, Policy). FALSE if it's just raw stock data, simple earnings table, or irrelevant.
    3. **Categorize Region**: [Africa, North America, India, China, Japan, Southeast Asia, Europe, Global].
    4. **Categorize Segment**: [Utility, Forklift, Agriculture, Construction, Mining].
    5. **Categorize Brand**: List relevant manufacturers mentioned [Komatsu, Caterpillar, Hitachi, Volvo, Liebherr, John Deere, Sany, XCMG, Zoomlion, Kobelco, Sumitomo, etc.].
    6. **3-Line Summary**: Summarize the key points in Japanese in exactly 3 bullet points.
    7. **Full Translation**: If the Original Language is NOT Japanese, provide a comprehensive natural Japanese translation of the Body Text. If it IS Japanese, just return "Original is Japanese".
    
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
        safe_print(f"AI Analysis failed: {e}")
        return None

def get_published_date_property_name():
    try:
        results = notion.search(filter={"property": "object", "value": "page"}, page_size=1)
        if results["results"]:
            props = results["results"][0]["properties"]
            for key in props.keys():
                if key.startswith("Published Date"):
                    return key
        return "Published Date"
    except:
        return "Published Date"

def get_existing_urls():
    """Fetch all unique 'Source URL' values from Notion using search."""
    safe_print("Fetching existing URLs from Notion to prevent duplicates...")
    existing_urls = set()
    try:
        cursor = None
        has_more = True
        while has_more:
            # Search is more robust in this version of the library
            response = notion.search(
                start_cursor=cursor,
                page_size=100,
                filter={"property": "object", "value": "page"}
            )
            for page in response.get("results", []):
                # Only collect from pages that have the Source URL property
                url = page["properties"].get("Source URL", {}).get("url")
                if url:
                    existing_urls.add(url)
            
            has_more = response.get("has_more", False)
            cursor = response.get("next_cursor")
        
        safe_print(f"Found {len(existing_urls)} existing URLs.")
    except Exception as e:
        safe_print(f"Warning: Could not fetch existing URLs via search: {e}")
    return existing_urls

def save_to_notion(source_name, article_data, ai_data, resolved_url, original_text):
    safe_print(f"Saving to Notion: {ai_data['translated_title']}")
    
    try:
        dt = datetime.fromtimestamp(time.mktime(article_data.published_parsed))
        iso_date = dt.isoformat()
    except:
        iso_date = datetime.now().isoformat()

    published_date_prop = get_published_date_property_name()

    # Handle summary as string or list from AI
    summary_data = ai_data.get("summary", "")
    if isinstance(summary_data, list):
        summary_text = "\n".join(summary_data)
    else:
        summary_text = str(summary_data)

    # Content Blocks
    children = [
        {
            "object": "block",
            "type": "heading_2",
            "heading_2": {
                "rich_text": [{"type": "text", "text": {"content": "要約"}}]
            }
        },
        {
            "object": "block",
            "type": "callout",
            "callout": {
                "rich_text": [{"type": "text", "text": {"content": summary_text}}],
                "icon": {"emoji": "💡"}
            }
        }
    ]
    
    # Add Translation if available
    if ai_data.get("full_translation") and "Original is Japanese" not in ai_data["full_translation"]:
        children.append({
            "object": "block",
            "type": "heading_2",
            "heading_2": {
                "rich_text": [{"type": "text", "text": {"content": "日本語訳"}}]
            }
        })
        # Split translation into chunks (Notion limit 2000 chars per text block)
        translation = ai_data["full_translation"]
        for i in range(0, len(translation), 2000):
            chunk = translation[i:i+2000]
            children.append({
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [{"type": "text", "text": {"content": chunk}}]
                }
            })

    # Add Original Text
    children.append({
        "object": "block",
        "type": "heading_2",
        "heading_2": {
            "rich_text": [{"type": "text", "text": {"content": "原文"}}]
        }
    })
    # Split original text
    if original_text:
        text_to_save = original_text
        for i in range(0, len(text_to_save), 2000):
            chunk = text_to_save[i:i+2000]
            children.append({
                "object": "block",
                "type": "paragraph", # Or quote
                "paragraph": {
                    "rich_text": [{"type": "text", "text": {"content": chunk}}]
                }
            })
    
    # Help AI errors: split by comma if multiple were returned as one string
    def clean_multi_select(val):
        if not val: return []
        # Split by comma and strip
        parts = [p.strip() for p in val.replace("、", ",").split(",")]
        return [{"name": p} for p in parts if p]

    properties = {
        "Title": {"title": [{"text": {"content": ai_data["translated_title"]}}]},
        "Source URL": {"url": resolved_url},
        "Source Name": {"select": {"name": source_name}},
        published_date_prop: {"date": {"start": iso_date}},
        "Region": {"multi_select": clean_multi_select(ai_data["region"])},
        "Segment": {"multi_select": clean_multi_select(ai_data["segment"])},
        "Brand": {"multi_select": clean_multi_select(ai_data.get("brand", ""))}
    }
    
    try:
        new_page = notion.pages.create(
            parent={"database_id": DATABASE_ID},
            properties=properties,
            children=children
        )
        safe_print("Successfully saved to Notion!")
        safe_print(f"Page URL: {new_page['url']}")
        return True # Success
    except Exception as e:
        safe_print(f"Failed to save to Notion: {e}")
        if hasattr(e, 'body'):
             safe_print(f"Error body: {e.body}")
        return False # Failed

def main():
    try:
        if sys.stdout.encoding.lower() != 'utf-8':
             try:
                 sys.stdout.reconfigure(encoding='utf-8')
             except:
                 pass
    except:
        pass

    target_counts = {"ja": 5, "en": 15}
    saved_counts = {"ja": 0, "en": 0}
    
    # Pre-fetch existing URLs
    existing_urls = get_existing_urls()
    
    try:
        for feed in RSS_FEEDS:
            lang = feed.get("lang", "en")
            if saved_counts[lang] >= target_counts[lang]:
                continue
                
            safe_print(f"\nChecking source: {feed['name']} ({lang})")
            article = fetch_latest_article(feed)
            
            if article:
                safe_print(f"Found: {article.title}")
                
                # Check for duplicates before expensive processing
                if article.link in existing_urls:
                    safe_print(f"Article already exists in Notion: Skip.")
                    continue
                
                resolved_url, body_text = resolve_and_extract_content(article.link)
                
                # Check resolved URL too (in case of different shortlinks)
                if resolved_url in existing_urls:
                    safe_print(f"Resolved URL already exists in Notion: Skip.")
                    continue
                
                if len(body_text) < 50:
                     safe_print("Body text too short, skipping.")
                     continue
                     
                ai_result = analyze_article(article.title, body_text, lang)
                
                if ai_result:
                    if ai_result.get("is_relevant_news", False):
                        safe_print("Article is RELEVANT. Saving...")
                        success = save_to_notion(feed['name'], article, ai_result, resolved_url, body_text)
                        if success:
                            saved_counts[lang] += 1
                    else:
                        safe_print(f"Filtered out: {ai_result.get('translated_title')}")
                else:
                     safe_print("AI Failed.")
            else:
                 safe_print("No articles found.")
            
            if all(c >= t for c, t in zip(saved_counts.values(), target_counts.values())):
                break
                
        safe_print(f"\nProcessing complete. Saved: {saved_counts}")
    except Exception as e:
        safe_print(f"Critical Error: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main()
