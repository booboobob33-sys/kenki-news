import requests
import feedparser
import google.generativeai as genai
from notion_client import Client
import notion_client
from datetime import datetime, timedelta, timezone
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
ARCHIVE_DATABASE_ID = os.getenv("ARCHIVE_DATABASE_ID", "")  # 非関連アーカイブDB

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
    safe_print(f"ARCHIVE_DATABASE_ID: {mask(ARCHIVE_DATABASE_ID) if ARCHIVE_DATABASE_ID else 'NOT SET (フィードバック機能無効)'}")
    
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

def get_excluded_articles():
    """
    非関連アーカイブDBから過去に除外された記事タイトルを取得する。
    ARCHIVE_DATABASE_ID が未設定の場合は空リストを返す。
    notion-clientのSDKバージョン差異を回避するためrequestsで直接API呼び出し。
    戻り値: ["タイトル1", "タイトル2", ...] (最大50件)
    """
    if not ARCHIVE_DATABASE_ID:
        safe_print("  [ARCHIVE] ARCHIVE_DATABASE_ID未設定、フィードバック機能をスキップ")
        return []
    if not NOTION_TOKEN:
        return []
    try:
        safe_print("  [ARCHIVE] 非関連アーカイブDBから除外記事を取得中...")
        results = []
        has_more = True
        next_cursor = None
        headers = {
            "Authorization": f"Bearer {NOTION_TOKEN}",
            "Content-Type": "application/json",
            "Notion-Version": "2022-06-28"
        }
        while has_more and len(results) < 50:
            body = {
                "page_size": 50,
                "sorts": [{"timestamp": "created_time", "direction": "descending"}]
            }
            if next_cursor:
                body["start_cursor"] = next_cursor

            resp = requests.post(
                f"https://api.notion.com/v1/databases/{ARCHIVE_DATABASE_ID}/query",
                headers=headers,
                json=body,
                timeout=15
            )
            if resp.status_code != 200:
                safe_print(f"  [WARN] アーカイブDB取得エラー HTTP {resp.status_code}: {resp.text[:200]}")
                return []

            data = resp.json()
            for page in data.get("results", []):
                props = page.get("properties", {})
                title_text = ""
                # "Title(JP)", "Title" など順番に探す
                for key in ["Title(JP)", "Title", "タイトル"]:
                    if key in props:
                        rich = props[key].get("title", [])
                        if rich:
                            title_text = rich[0].get("plain_text", "")
                            break
                # 見つからなければtitleタイプのプロパティを検索
                if not title_text:
                    for key, val in props.items():
                        if isinstance(val, dict) and val.get("type") == "title":
                            rich = val.get("title", [])
                            if rich:
                                title_text = rich[0].get("plain_text", "")
                                break
                if title_text:
                    results.append(title_text)

            has_more = data.get("has_more", False)
            next_cursor = data.get("next_cursor")

        safe_print(f"  [ARCHIVE] {len(results)}件の除外記事を取得")
        return results[:50]
    except Exception as e:
        safe_print(f"  [WARN] 非関連アーカイブDB取得失敗: {e}")
        return []


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
        "Title(JP)", "Title(EN)", "Source URL", "Published Date（記事日付）",
        "Brand", "Region", "Segment", "Source", "Summary"
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
    "title_jp": get_prop_name(["Title(JP)"], default_if_empty="Title(JP)"),
    "title_en": get_prop_name(["Title(EN)"], default_if_empty="Title(EN)"),
    "url": get_prop_name(["Source URL"], default_if_empty="Source URL"),
    "date": get_prop_name(["Published Date（記事日付）", "Published Date"], default_if_empty="Published Date（記事日付）"),
    "brand": get_prop_name(["Brand"], default_if_empty="Brand"),
    "segment": get_prop_name(["Segment"], default_if_empty="Segment"),
    "region": get_prop_name(["Region"], default_if_empty="Region"),
    "summary": get_prop_name(["Summary"], default_if_empty="Summary"),
    "source": get_prop_name(["Source"], default_if_empty="Source")
}

safe_print(f"  [NOTION] Final Mapped columns: " + ", ".join([f"{k}->{v}" for k,v in P_MAP.items() if v]))

# 非関連アーカイブDBから除外記事タイトルを取得（起動時に1回だけ）
EXCLUDED_TITLES = get_excluded_articles()

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

def analyze_article_relevance(article_data):
    """Perform a light AI check to see if the article is relevant."""
    safe_print(f"  [AI-Lite] Checking relevance: {article_data['title'][:40]}...")

    # 除外例をプロンプトに組み込む
    exclusion_section = ""
    if EXCLUDED_TITLES:
        examples = "\n".join([f"  - {t}" for t in EXCLUDED_TITLES[:20]])
        exclusion_section = f"""
【過去に「関係なし」と判断された記事の例】（これらと類似する内容は除外してください）:
{examples}
"""

    prompt = f"""あなたは建設・鉱山機械業界の専門家です。
以下の記事タイトルと概要が、建設機械、鉱山機械、あるいはそれらの業界（メーカー動向、技術、市場)に関連があるか判定してください。
{exclusion_section}
【タイトル】: {article_data['title']}
【概要】: {article_data['summary'][:500]}

関連がある場合は 'yes'、関連がない場合は 'no' とだけ出力してください。"""

    try:
        response = model.generate_content(prompt)
        text = response.text.strip().lower()
        if "yes" in text:
            safe_print("  [AI-Lite] Judging as RELEVANT.")
            return True
        else:
            safe_print("  [AI-Lite] Judging as NOT relevant. Skipping.")
            return False
    except Exception as e:
        safe_print(f"  [WARN] AI relevance check failed (assuming relevant): {e}")
        return True

def is_duplicate(url):
    """Check if the URL already exists in Notion."""
    url_col = P_MAP["url"]
    if not url_col:
        return False
    try:
        query_method = getattr(notion.databases, "query", None)
        if query_method:
            q = query_method(database_id=DATABASE_ID, filter={"property": url_col, "url": {"equals": url}})
            if q["results"]:
                return True
    except Exception as e:
        safe_print(f"  [WARN] Duplicate check failed: {e}")
    return False

def analyze_article_with_gemini(article_data, page_text=""):
    safe_print(f"  [AI-Full] Analyzing: {article_data['title'][:40]}...")

    content = page_text if len(page_text) > 200 else article_data.get('summary', '')
    content = content[:5000]

    prompt = f"""あなたは建設・鉱山機械業界の専門ニュースアナリストです。以下の記事を分析し、必ず下記のJSON形式のみで回答してください。余分な説明文は一切不要です。

【記事タイトル】: {article_data['title']}
【フィード名（参考）】: {article_data.get('feed_name', '')}
【記事本文/概要】:
{content}

出力するJSONの形式（これ以外の出力はしないでください）:
{{
  "title_jp": "日本語タイトル（元が英語なら日本語に翻訳、元が日本語ならそのまま。出典名は含めない）",
  "title_en": "英語タイトル（元が英語ならそのまま、元が日本語なら英語に翻訳。出典名は含めない）",
  "source": "出典名（例: 日経新聞, 日刊工業新聞, Komatsu, Caterpillar, Construction Equipment Guide など簡潔に）",
  "bullet_summary": "日本語3文のみ。必ず3文で終わること。4文以上は絶対に書かない。記事本文に書かれた事実のみを簡潔にまとめること。推測・解釈・意見・評価・将来予測は一切含めない。",
  "full_body": "原文の逐語訳。要約・省略・言い換えは一切禁止。英語の場合は直訳に近い自然な日本語に翻訳し、日本語の場合は原文をそのまま全文転記する。広告・バナー・画像キャプション・ナビゲーションメニューは除外し、ニュース本文のみを先頭から順番に転記すること。本文が広告などで途切れている場合は、前後の段落を文脈に沿って繋げること（ただし記載テキストの範囲内に限る、補完・推測は禁止）。最大5000文字。入手できたテキストのみ転記し、推測や補完は禁止。",
  "brand": "関連メーカー名（例: Caterpillar, Komatsu, Liebherr。複数はカンマ区切り。不明はnone）",
  "segment": "機種セグメント（例: Excavator, Wheel Loader, Crane, Dump Truck。複数はカンマ区切り。不明はnone）",
  "region": "地域（例: North America, Japan, Europe, China。複数はカンマ区切り。不明はnone）"
}}"""

    try:
        response = model.generate_content(prompt)
        text = response.text.strip()
        # マークダウンコードブロックを除去
        text = re.sub(r'^```(?:json)?\s*', '', text)
        text = re.sub(r'\s*```$', '', text)
        text = text.strip()

        json_match = re.search(r'\{.*\}', text, re.DOTALL)
        if json_match:
            result = json.loads(json_match.group())
            safe_print(f"  [AI-Full] Done. Brand: {result.get('brand', 'N/A')}")
            return result
        else:
            safe_print(f"  [WARN] JSON not found in Gemini response.")
    except json.JSONDecodeError as e:
        safe_print(f"  [WARN] JSON parse error: {e}")
    except Exception as e:
        safe_print(f"  [ERROR] Gemini analysis failed: {e}")
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
    title_jp_col = P_MAP["title_jp"]
    title_en_col = P_MAP["title_en"]
    url_col = P_MAP["url"]
    date_col = P_MAP["date"]
    brand_col = P_MAP["brand"]
    segment_col = P_MAP["segment"]
    region_col = P_MAP["region"]
    summary_col = P_MAP["summary"]
    source_col = P_MAP["source"]

    # タイトル（EN）: Notionのtitle型プロパティ（主キー）
    title_en = result.get("title_en") or article_data['title']
    if title_en_col:
        props[title_en_col] = {"title": [{"text": {"content": title_en[:100]}}]}

    # タイトル（JP）: rich_text型プロパティ
    title_jp = result.get("title_jp") or article_data['title']
    if title_jp_col:
        props[title_jp_col] = {"rich_text": [{"text": {"content": title_jp[:100]}}]}

    if url_col: props[url_col] = {"url": article_data['link']}

    # 出典（Source）: Geminiが抽出した出典名
    source_name = result.get("source", "").strip()
    if source_name and source_col:
        props[source_col] = {"select": {"name": source_name[:50]}}
    
    # AI Results
    brand_tags = clean_multi_select(result.get("brand"))
    if brand_tags and brand_col: props[brand_col] = {"multi_select": brand_tags}
    
    segment_tags = clean_multi_select(result.get("segment"))
    if segment_tags and segment_col: props[segment_col] = {"multi_select": segment_tags}
    
    region_tags = clean_multi_select(result.get("region"))
    if region_tags and region_col: props[region_col] = {"multi_select": region_tags}
    
    if date_col: props[date_col] = {"date": {"start": article_data.get("date")}}
    
    # Remove summary from properties (we will write it to the page body instead)
    if summary_col in props:
        del props[summary_col]

    # Construct Page Body (Children)
    children = []

    # 著作権免責ブロック（ページ先頭）
    source_url = article_data.get('link', '')
    copyright_text = (
        "【著作権表示】本ページは著作権法第32条に基づく引用および社内情報収集・研究目的で"
        "転記・翻訳しています。著作権は原著作者に帰属します。商用利用・外部公開を禁じます。"
        f"  原文URL: {source_url}"
    )
    children.append({
        "object": "block",
        "type": "callout",
        "callout": {
            "rich_text": [{"type": "text", "text": {"content": copyright_text}}],
            "icon": {"emoji": "⚠️"},
            "color": "yellow_background"
        }
    })

    # Summary Section
    summary_text = result.get("bullet_summary", "")
    if isinstance(summary_text, list):
        summary_text = " ".join(summary_text)
    summary_text = str(summary_text).strip()
    if summary_text:
        children.append({
            "object": "block",
            "type": "heading_2",
            "heading_2": {"rich_text": [{"type": "text", "text": {"content": "【要約】"}}]}
        })
        # 2000字制限に合わせて分割
        for i in range(0, min(len(summary_text), 4000), 1950):
            children.append({
                "object": "block",
                "type": "paragraph",
                "paragraph": {"rich_text": [{"type": "text", "text": {"content": summary_text[i:i+1950]}}]}
            })

    # Body Section
    body_text = result.get("full_body", "")
    if isinstance(body_text, list):
        body_text = "\n".join(body_text)
    body_text = str(body_text).strip()
    if body_text:
        children.append({
            "object": "block",
            "type": "heading_2",
            "heading_2": {"rich_text": [{"type": "text", "text": {"content": "【本文引用/翻訳】"}}]}
        })
        
        # Notion paragraph blocks have a 2000 character limit.
        # Split body_text into multiple blocks of up to 1950 chars each.
        CHUNK = 1950
        chunks = [body_text[i:i+CHUNK] for i in range(0, min(len(body_text), 8000), CHUNK)]
        for chunk in chunks:
            children.append({
                "object": "block",
                "type": "paragraph",
                "paragraph": {"rich_text": [{"type": "text", "text": {"content": chunk}}]}
            })

        is_truncated = len(body_text) > 8000

        # Always provide the original link for convenience or if truncated
        children.append({
            "object": "block",
            "type": "paragraph",
            "paragraph": {
                "rich_text": [
                    {"type": "text", "text": {"content": "（続き・詳細はソース元へ： " if is_truncated else "（ソース元： "}},
                    {"type": "text", "text": {"content": article_data['link'], "link": {"url": article_data['link']}}},
                    {"type": "text", "text": {"content": " ）"}}
                ]
            }
        })

    # Attempt save with retry loop
    max_retries = 3
    for attempt in range(max_retries):
        try:
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

_FETCH_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5,ja;q=0.3',
}

def resolve_article_url(url):
    """Google News リダイレクトURLを実際の記事URLに解決する。"""
    if 'news.google.com' not in url:
        return url
    try:
        resp = requests.get(url, headers=_FETCH_HEADERS, timeout=15, allow_redirects=True)
        final_url = resp.url
        if 'news.google.com' not in final_url:
            safe_print(f"  [URL] Resolved via redirect: {final_url[:80]}")
            return final_url

        # Google Newsページ内から実際の記事URLを多段階で探す
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(resp.content, 'html.parser')

        def is_real_url(href):
            return href and href.startswith('http') and 'google' not in href

        # 1. canonical link
        tag = soup.find('link', rel='canonical')
        if tag and is_real_url(tag.get('href', '')):
            safe_print(f"  [URL] canonical: {tag['href'][:80]}")
            return tag['href']

        # 2. og:url
        tag = soup.find('meta', property='og:url')
        if tag and is_real_url(tag.get('content', '')):
            safe_print(f"  [URL] og:url: {tag['content'][:80]}")
            return tag['content']

        # 3. twitter:url
        tag = soup.find('meta', attrs={'name': 'twitter:url'})
        if tag and is_real_url(tag.get('content', '')):
            safe_print(f"  [URL] twitter:url: {tag['content'][:80]}")
            return tag['content']

        # 4. ページ内の最初の外部リンク（Google以外）
        for a in soup.find_all('a', href=True):
            href = a['href']
            if is_real_url(href):
                safe_print(f"  [URL] page link: {href[:80]}")
                return href

        safe_print(f"  [WARN] URL resolution exhausted. Using original Google URL.")
    except Exception as e:
        safe_print(f"  [WARN] URL resolution failed: {e}")
    return url

def get_page_text(url):
    """Fetch and extract clean text from a URL."""
    try:
        from bs4 import BeautifulSoup
        actual_url = resolve_article_url(url)
        safe_print(f"  [HTTP] Fetching: {actual_url[:80]}")
        resp = requests.get(actual_url, headers=_FETCH_HEADERS, timeout=15)
        if resp.status_code != 200:
            safe_print(f"  [WARN] Failed to fetch (Status: {resp.status_code})")
            return ""
        soup = BeautifulSoup(resp.content, 'html.parser')

        # ノイズ要素（タグ種別）を除去
        for s in soup(["script", "style", "nav", "header", "footer", "aside",
                        "figure", "figcaption", "form", "button", "iframe", "noscript"]):
            s.decompose()

        # 広告・ノイズ要素（クラス名/ID のパターンマッチング）を除去
        ad_pattern = re.compile(
            r'(^ad$|^ads$|advert|advertisement|banner|popup|modal|cookie|gdpr|'
            r'promo|sponsor|social[\-_]?share|share[\-_]?bar|related[\-_]?(posts|articles)|'
            r'recommend|newsletter|subscribe|comment|sidebar|widget|sticky|overlay)',
            re.IGNORECASE
        )
        for tag in soup.find_all(True):
            tag_id    = tag.get('id', '') or ''
            tag_class = ' '.join(tag.get('class', []) or [])
            if ad_pattern.search(tag_id) or ad_pattern.search(tag_class):
                tag.decompose()

        # 記事本文コンテナを優先的に探す
        article_text = ""
        for selector in ['article', 'main', '[role="main"]',
                         '.article-body', '.article-content', '.post-content',
                         '.entry-content', '.story-body', '.article__body']:
            container = soup.select_one(selector)
            if container:
                paragraphs = container.find_all('p')
                text = ' '.join(
                    p.get_text(separator=' ', strip=True)
                    for p in paragraphs if len(p.get_text(strip=True)) > 30
                )
                if len(text) > 200:
                    article_text = text
                    break

        # フォールバック: ページ全体のpタグ
        if len(article_text) < 200:
            all_p = soup.find_all('p')
            article_text = ' '.join(
                p.get_text(separator=' ', strip=True)
                for p in all_p if len(p.get_text(strip=True)) > 30
            )

        # 最終フォールバック: bodyテキスト全体
        if len(article_text) < 100:
            article_text = ' '.join(soup.get_text().split())

        clean_text = article_text[:10000]
        safe_print(f"  [DATA] Extracted text length: {len(clean_text)} chars")
        return clean_text
    except Exception as e:
        safe_print(f"  [ERROR] get_page_text failed: {e}")
        return ""

def main():
    processed_count = 0
    total_limit = 15
    safe_print("=== Starting Collection ===")
    
    # 24時間前の時刻を取得 (UTC)
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=24)
    
    for feed in RSS_FEEDS:
        if processed_count >= total_limit:
            break
            
        try:
            resp = requests.get(feed['url'], headers={'User-Agent': 'Mozilla/5.0'}, timeout=20)
            if resp.status_code != 200: continue
            entries = feedparser.parse(resp.content).entries
            
            for entry in entries:
                if processed_count >= total_limit:
                    break
                
                # 記事の公開日時を取得して24時間以内かチェック
                pub_parsed = entry.get("published_parsed")
                if pub_parsed:
                    # feedparserのpublished_parsedは通常UTC
                    entry_dt = datetime(*pub_parsed[:6]).replace(tzinfo=timezone.utc)
                    if entry_dt < cutoff:
                        continue # 24時間以上前ならスキップ
                    entry_date_str = entry_dt.isoformat()
                else:
                    # 日付が取れない場合は現在の時刻とする（またはスキップする選択肢もあるが、現行に合わせる）
                    entry_date_str = datetime.now().isoformat()

                data = {
                    "title": entry.title,
                    "link": entry.link,
                    "summary": entry.get("summary", entry.get("description", "")),
                    "date": entry_date_str,
                    "feed_name": feed['name']
                }
                
                # 1. Early Duplicate Check (Before fetching text or AI analysis)
                if is_duplicate(data['link']):
                    safe_print(f"  [SKIP] Duplicate article: {data['title'][:40]}")
                    continue

                # 2. Preliminary Relevance Check (Title & RSS Summary only)
                if not analyze_article_relevance(data):
                    continue

                # Fetch full text
                page_text = get_page_text(data['link'])
                if not page_text or len(page_text) < 200:
                    safe_print(f"  [INFO] Page text too short or empty. Falling back to RSS summary.")
                
                res = analyze_article_with_gemini(data, page_text)
                if res and save_to_notion(res, data):
                    processed_count += 1
                    if processed_count % 3 == 0: time.sleep(60)
                
                time.sleep(15) # Between articles
            
            time.sleep(5) # Between feeds
        except Exception as e:
            safe_print(f"  [ERROR] Loop error: {e}")
    
    safe_print(f"\n=== JOB FINISHED SUCCESSFULLY ===")
    safe_print(f"Successfully saved {processed_count} news items to Notion.")
    safe_print(f"==================================\n")

if __name__ == "__main__":
    main()
