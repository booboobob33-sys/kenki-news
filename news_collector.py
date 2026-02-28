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

# =============================================================================
# Configuration — 環境変数から読み込み（ハードコード厳禁）
# =============================================================================
NOTION_TOKEN  = os.getenv("NOTION_TOKEN")
DATABASE_ID   = os.getenv("DATABASE_ID")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# =============================================================================
# Utilities
# =============================================================================
def safe_print(text):
    """Safely print text handling potential encoding errors."""
    try:
        print(text, flush=True)
    except Exception:
        try:
            print(str(text).encode('utf-8', errors='replace').decode('utf-8'), flush=True)
        except Exception:
            pass

def clean_text(text):
    """HTMLタグ・生URL・余分な空白を除去してクリーンなテキストを返す。"""
    if not text:
        return ""
    # HTMLタグ除去
    text = re.sub(r'<[^>]+>', '', text)
    # 生URL除去（http/https始まりの単語）
    text = re.sub(r'https?://\S+', '', text)
    # 連続空白・改行を整理
    text = re.sub(r'\s+', ' ', text).strip()
    return text

# =============================================================================
# 起動時診断 — シークレット未設定なら即終了
# =============================================================================
def check_env_and_exit_if_empty():
    safe_print("--- Environment Diagnostics ---")
    safe_print(f"Python: {sys.version}")

    try:
        import google.generativeai as gai
        import notion_client as nc
        safe_print(f"Gemini SDK: {getattr(gai, '__version__', 'Unknown')}")
        safe_print(f"Notion SDK: {getattr(nc,  '__version__', 'Unknown')}")
    except Exception as e:
        safe_print(f"Error checking SDKs: {e}")

    mask = lambda s: (s[:4] + "***") if (s and len(s) > 4) else ("EMPTY" if not s else "SHORT")
    safe_print(f"DATABASE_ID:    {mask(DATABASE_ID)}")
    safe_print(f"GEMINI_API_KEY: {mask(GEMINI_API_KEY)}")

    if not all([NOTION_TOKEN, DATABASE_ID, GEMINI_API_KEY]):
        safe_print("\n[CRITICAL ERROR] Missing Secrets in GitHub Settings.")
        sys.exit(1)
    safe_print("-------------------------------\n")

check_env_and_exit_if_empty()

# =============================================================================
# Gemini AI セットアップ
# =============================================================================
genai.configure(api_key=GEMINI_API_KEY)

def get_best_model():
    """利用可能な Gemini モデルから最適なものを選択する（新世代優先）。"""
    safe_print("  [AI] Discovering available models...")
    try:
        available_models = [
            m.name for m in genai.list_models()
            if 'generateContent' in m.supported_generation_methods
        ]
        safe_print(f"  [AI] Found: {', '.join(available_models)}")

        # 優先リスト: 新世代・高速モデルを優先
        preferred = [
            "models/gemini-1.5-flash",
            "gemini-1.5-flash",
            "models/gemini-1.5-flash-latest",
            "models/gemini-1.5-flash-002",
            "models/gemini-1.5-pro",
        ]
        for p in preferred:
            for am in available_models:
                if p == am or p.split('/')[-1] == am.split('/')[-1]:
                    safe_print(f"  [AI] Selected: {am}")
                    return genai.GenerativeModel(model_name=am)

        if available_models:
            safe_print(f"  [AI] Using first available: {available_models[0]}")
            return genai.GenerativeModel(model_name=available_models[0])

    except Exception as e:
        safe_print(f"  [WARN] Model discovery failed: {e}")

    safe_print("  [AI] Absolute fallback: gemini-1.5-flash")
    return genai.GenerativeModel(model_name="gemini-1.5-flash")

model = get_best_model()

# =============================================================================
# Notion クライアント & スキーマ検出
# =============================================================================
notion = Client(auth=NOTION_TOKEN)

def get_db_properties():
    """Notionデータベースの実際のプロパティ名一覧を取得する。"""
    try:
        safe_print(f"  [NOTION] Fetching DB metadata for ID: {str(DATABASE_ID)[:8]}...")
        db = notion.databases.retrieve(database_id=DATABASE_ID)
        all_found = list(db.get("properties", {}).keys())
        if all_found:
            safe_print(f"  [NOTION] Properties found: {', '.join(all_found)}")
            for p in all_found:
                if "Date" in p or "日付" in p:
                    safe_print(f"  [DEBUG] Date candidate: '{p}'")
            return all_found
        safe_print("  [WARN] No properties returned from Notion API.")
    except Exception as e:
        safe_print(f"  [WARN] Could not retrieve DB schema: {e}")
    return []

ACTUAL_DB_PROPS = get_db_properties()

# Zero-Trust フォールバック（API失敗時）
if not ACTUAL_DB_PROPS:
    safe_print("  [NOTION] Using hardcoded fallback property names.")
    ACTUAL_DB_PROPS = [
        "Title", "Source URL", "Published Date（記事日付）",
        "Brand", "Region", "Segment", "Source Name", "Summary",
    ]

def get_prop_name(candidates, default_if_empty=None):
    """候補名からActual DBプロパティを検索（完全一致→ファジー一致）。"""
    for c in candidates:
        if c in ACTUAL_DB_PROPS:
            return c
    def simplify(s):
        return re.sub(r'[^a-zA-Z0-9\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FFF]', '', str(s)).lower()
    sc_list = [simplify(c) for c in candidates]
    for act in ACTUAL_DB_PROPS:
        sa = simplify(act)
        for sc in sc_list:
            if sc in sa or sa in sc:
                return act
    return default_if_empty

# プロパティマッピング
P_MAP = {
    "title":        get_prop_name(["Title"],                                      default_if_empty="Title"),
    "url":          get_prop_name(["Source URL"],                                  default_if_empty="Source URL"),
    "date":         get_prop_name(["Published Date（記事日付）", "Published Date"], default_if_empty="Published Date（記事日付）"),
    "brand":        get_prop_name(["Brand"],                                       default_if_empty="Brand"),
    "segment":      get_prop_name(["Segment"],                                     default_if_empty="Segment"),
    "region":       get_prop_name(["Region"],                                      default_if_empty="Region"),
    "summary":      get_prop_name(["Summary"],                                     default_if_empty="Summary"),
    "source_name":  get_prop_name(["Source Name"],                                 default_if_empty="Source Name"),
}

safe_print("  [NOTION] Final mapped columns: " + ", ".join([f"{k}→{v}" for k, v in P_MAP.items() if v]))

# =============================================================================
# ニュースソース設定
# =============================================================================
RSS_FEEDS = [
    # --- Specialized Industry Media ---
    {"name": "Mining.com (Mining Machine)",     "url": "https://www.mining.com/tag/mining-machinery/feed/"},
    {"name": "Construction Equipment Guide",    "url": "https://www.constructionequipmentguide.com/rss/"},
    {"name": "KHL Construction News",           "url": "https://news.google.com/rss/search?q=site:khl.com+International+Construction&hl=en-US&gl=US&ceid=US:en"},

    # --- Major Manufacturers ---
    {"name": "CAT (Caterpillar)",               "url": "https://news.google.com/rss/search?q=Caterpillar+Construction+Mining+News&hl=en-US&gl=US&ceid=US:en"},
    {"name": "Komatsu (Global)",                "url": "https://news.google.com/rss/search?q=Komatsu+Mining+Construction+News&hl=en-US&gl=US&ceid=US:en"},
    {"name": "John Deere (Construction)",       "url": "https://news.google.com/rss/search?q=John+Deere+Construction+News&hl=en-US&gl=US&ceid=US:en"},
    {"name": "XCMG / Sany News",               "url": "https://news.google.com/rss/search?q=%22XCMG%22+OR+%22Sany+Group%22+Construction&hl=en-US&gl=US&ceid=US:en"},
    {"name": "Volvo CE / Liebherr",             "url": "https://news.google.com/rss/search?q=%22Volvo+CE%22+OR+%22Liebherr%22+Machinery&hl=en-US&gl=US&ceid=US:en"},
    {"name": "Hitachi CM / Bobcat",             "url": "https://news.google.com/rss/search?q=%22Hitachi+Construction+Machinery%22+OR+%22Bobcat%22+News&hl=en-US&gl=US&ceid=US:en"},
    {"name": "Zoomlion / Kubota News",          "url": "https://news.google.com/rss/search?q=%22Zoomlion%22+OR+%22Kubota%22+Construction&hl=en-US&gl=US&ceid=US:en"},
    {"name": "JCB / Kobelco / Sumitomo",        "url": "https://news.google.com/rss/search?q=%22JCB%22+OR+%22Kobelco%22+OR+%22Sumitomo+Construction+Machinery%22&hl=en-US&gl=US&ceid=US:en"},

    # --- Japanese Media ---
    {"name": "Googleニュース (建機/鉱山機械)", "url": "https://news.google.com/rss/search?q=%E5%BB%BA%E8%A8%AD%E6%A9%9F%E6%A2%B0+OR+%E9%89%B1%E5%B1%B1%E6%A9%9F%E6%A2%B0&hl=ja&gl=JP&ceid=JP:ja"},
    {"name": "日経ニュース (重機/自動化)",     "url": "https://news.google.com/rss/search?q=site:nikkei.com+%E5%BB%BA%E8%A8%AD%E6%A9%9F%E6%A2%B0+OR+%E8%87%AA%E5%8B%95%E5%8C%96&hl=ja&gl=JP&ceid=JP:ja"},
    {"name": "Googleニュース (脱炭素/電動建機)","url": "https://news.google.com/rss/search?q=%E9%9B%BB%E5%8B%95%E5%BB%BA%E8%A8%AD%E6%A9%9F%E6%A2%B0+OR+%E8%84%B1%E7%82%AD%E7%B4%A0+%E5%BB%BA%E6%A9%9F&hl=ja&gl=JP&ceid=JP:ja"},
]

# =============================================================================
# AI 関連関数
# =============================================================================
def analyze_article_relevance(article_data):
    """軽量AIチェック: 建機・鉱山機械業界に関連するか yes/no で判定。"""
    safe_print(f"  [AI-Lite] Checking relevance: {article_data['title'][:50]}...")
    prompt = f"""あなたは建設・鉱山機械業界の専門家です。
以下の記事タイトルと概要が、建設機械、鉱山機械、あるいはそれらの業界（メーカー動向、技術、市場）に関連があるか判定してください。

【タイトル】: {article_data['title']}
【概要】: {article_data['summary'][:500]}

関連がある場合は 'yes'、関連がない場合は 'no' とだけ出力してください。"""

    try:
        response = model.generate_content(prompt)
        text = response.text.strip().lower()
        if "yes" in text:
            safe_print("  [AI-Lite] → RELEVANT")
            return True
        safe_print("  [AI-Lite] → NOT relevant. Skipping.")
        return False
    except Exception as e:
        safe_print(f"  [WARN] Relevance check failed (assuming relevant): {e}")
        return True


def analyze_article_with_gemini(article_data, page_text=""):
    """
    記事を Gemini で分析し、以下のキーを持つ dict を返す。
    - bullet_summary : 箇条書き要約（3点）
    - full_body      : 原文ベースの引用テキスト（最大2000文字、HTMLタグ・生URL除去済み）
    - brand          : 関連メーカー名（カンマ区切り）
    - segment        : セグメント（カンマ区切り）
    - region         : 地域（カンマ区切り）
    失敗時は None を返す。
    """
    safe_print(f"  [AI-Full] Analyzing: {article_data['title'][:50]}...")

    # 入力テキストを準備（原文優先・上限2000文字）
    raw_input = page_text if page_text and len(page_text) >= 200 else article_data.get("summary", "")
    raw_input = clean_text(raw_input)[:5000]  # HTMLタグ・生URL除去 + 文字数制限（AIへの入力は5000文字まで）

    # 原文が取れなかった場合のURLフォールバック
    if not raw_input:
        safe_print("  [WARN] No usable text. Returning URL-only fallback.")
        return {
            "bullet_summary": "（本文を取得できませんでした）",
            "full_body": "",
            "brand": "",
            "segment": "",
            "region": "",
        }

    prompt = f"""あなたは建設・鉱山機械業界のアナリストです。
以下の記事を分析し、JSON形式で回答してください。HTMLタグや生URLは含めないでください。

【タイトル】: {article_data['title']}
【本文（原文）】:
{raw_input}

以下のJSONキーで回答してください（値はすべて文字列、Markdown不可）:
{{
  "bullet_summary": "要点1。\\n要点2。\\n要点3。（日本語で）",
  "full_body": "【重要ルール】リンクを開かなくても内容が把握できるよう、できる限り詳しく記載すること。原文が日本語の場合はそのまま転記する（最大2000文字）。原文が英語の場合は英語原文をそのまま記載し、改行後に日本語訳を続けて記載する（合計最大2000文字）。省略せず情報を最大限含めること。HTMLタグ・URLは含めない。",
  "lang": "記事の原文言語。'ja' または 'en' または 'other'",
  "brand": "関連メーカー名をカンマ区切り（例: Caterpillar, Komatsu）。不明なら空文字。",
  "segment": "該当セグメントをカンマ区切り（例: 油圧ショベル, ホイールローダー）。不明なら空文字。",
  "region": "関連地域をカンマ区切り（例: North America, Japan）。不明なら空文字。"
}}

JSONのみ出力し、コードブロック記号（```）は使わないでください。"""

    try:
        response = model.generate_content(prompt)
        raw_text = response.text.strip()

        # コードブロック記号が含まれていれば除去
        raw_text = re.sub(r'^```[a-z]*\n?', '', raw_text)
        raw_text = re.sub(r'\n?```$', '', raw_text)

        result = json.loads(raw_text)

        # 各フィールドのクリーニング（HTMLタグ・生URL除去）
        for key in ["bullet_summary", "full_body"]:
            if key in result:
                result[key] = clean_text(str(result[key]))

        safe_print(f"  [AI-Full] Analysis complete. Body: {len(result.get('full_body',''))} chars")
        return result

    except json.JSONDecodeError as e:
        safe_print(f"  [WARN] JSON parse failed: {e}. Raw: {raw_text[:200]}")
        # JSON解析失敗時: テキスト全体を full_body として保存
        fallback_body = clean_text(raw_text)[:2000]
        return {
            "bullet_summary": "",
            "full_body": fallback_body,
            "brand": "", "segment": "", "region": "",
        }
    except Exception as e:
        safe_print(f"  [ERROR] analyze_article_with_gemini failed: {e}")
        return None

# =============================================================================
# Notion 関連関数
# =============================================================================
def is_duplicate(url):
    """
    URLが既にNotionに登録済みか確認する。
    notion-client の新旧バージョン両方に対応:
      新版 (2025-09-03+): notion.data_sources.query()
      旧版             : notion.databases.query()
    どちらも失敗した場合は requests で直接 Notion API を叩くフォールバックを持つ。
    """
    url_col = P_MAP["url"]
    if not url_col:
        return False

    filter_body = {"property": url_col, "url": {"equals": url}}

    # ── 方法1: 新版 SDK (data_sources.query) ──────────────────────────────
    try:
        data_sources = getattr(notion, "data_sources", None)
        if data_sources and hasattr(data_sources, "query"):
            q = notion.data_sources.query(
                data_source_id=DATABASE_ID,
                filter=filter_body
            )
            if q.get("results"):
                safe_print("  [DEDUP] Duplicate found (data_sources.query).")
                return True
            return False
    except Exception as e:
        safe_print(f"  [DEDUP] data_sources.query failed: {e}. Trying databases.query...")

    # ── 方法2: 旧版 SDK (databases.query) ────────────────────────────────
    try:
        databases = getattr(notion, "databases", None)
        if databases and hasattr(databases, "query"):
            q = notion.databases.query(
                database_id=DATABASE_ID,
                filter=filter_body
            )
            if q.get("results"):
                safe_print("  [DEDUP] Duplicate found (databases.query).")
                return True
            return False
    except Exception as e:
        safe_print(f"  [DEDUP] databases.query failed: {e}. Trying direct API...")

    # ── 方法3: requests で直接 Notion REST API を叩くフォールバック ──────
    try:
        api_url = f"https://api.notion.com/v1/databases/{DATABASE_ID}/query"
        headers = {
            "Authorization": f"Bearer {NOTION_TOKEN}",
            "Notion-Version": "2022-06-28",
            "Content-Type": "application/json",
        }
        payload = {"filter": filter_body, "page_size": 1}
        resp = requests.post(api_url, headers=headers, json=payload, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("results"):
                safe_print("  [DEDUP] Duplicate found (direct REST API).")
                return True
            return False
        else:
            safe_print(f"  [WARN] Direct API duplicate check returned HTTP {resp.status_code}")
    except Exception as e:
        safe_print(f"  [WARN] All duplicate check methods failed: {e}")

    return False


def clean_multi_select(val):
    """multi_select 用に値を整形。不明・空の場合は空リスト。"""
    if not val or str(val).lower() in ["none", "不明", "other", ""]:
        return []
    parts = [p.strip() for p in str(val).replace("、", ",").split(",")]
    return [{"name": p} for p in parts if p]


def save_to_notion(result, article_data):
    """
    分析結果と記事データを Notion に保存する。
    - プロパティ: Title / Source URL / Published Date / Brand / Segment /
                 Region / Source Name
    - ページ本文: 【要約】箇条書き + 【本文引用/翻訳】原文優先（英語のみ日本語訳付き、2000文字制限）+ ソースリンク
    """
    safe_print("  [NOTION] Building page...")

    # ---- プロパティ構築 ----
    props = {}
    title_col        = P_MAP["title"]
    url_col          = P_MAP["url"]
    date_col         = P_MAP["date"]
    brand_col        = P_MAP["brand"]
    segment_col      = P_MAP["segment"]
    region_col       = P_MAP["region"]
    summary_col      = P_MAP["summary"]
    source_name_col  = P_MAP["source_name"]

    if title_col:
        props[title_col] = {"title": [{"text": {"content": article_data["title"][:100]}}]}
    if url_col:
        props[url_col] = {"url": article_data["link"]}
    if source_name_col:
        props[source_name_col] = {"select": {"name": "RSS Search Collector"}}
    if date_col:
        props[date_col] = {"date": {"start": article_data.get("date")}}

    # AI結果
    brand_tags = clean_multi_select(result.get("brand"))
    if brand_tags and brand_col:
        props[brand_col] = {"multi_select": brand_tags}

    segment_tags = clean_multi_select(result.get("segment"))
    if segment_tags and segment_col:
        props[segment_col] = {"multi_select": segment_tags}

    region_tags = clean_multi_select(result.get("region"))
    if region_tags and region_col:
        props[region_col] = {"multi_select": region_tags}

    # Summary はページ本文に書くため、プロパティからは除外
    if summary_col and summary_col in props:
        del props[summary_col]

    # ---- ページ本文（children）構築 ----
    children = []

    # 【要約】セクション
    bullets_raw = result.get("bullet_summary", "")
    if isinstance(bullets_raw, list):
        bullets_raw = "\n".join(bullets_raw)
    bullets_raw = str(bullets_raw).strip()

    if bullets_raw:
        children.append({
            "object": "block", "type": "heading_2",
            "heading_2": {"rich_text": [{"type": "text", "text": {"content": "【要約】"}}]}
        })
        bullet_list = [b.strip("- •*") for b in bullets_raw.split("\n") if b.strip()]
        for b in bullet_list[:3]:
            b = clean_text(b)[:2000]
            if b:
                children.append({
                    "object": "block", "type": "bulleted_list_item",
                    "bulleted_list_item": {"rich_text": [{"type": "text", "text": {"content": b}}]}
                })

    # 【本文引用/翻訳】セクション（英語の場合は原文＋日本語訳、日本語の場合は原文そのまま）
    body_text = result.get("full_body", "")
    if isinstance(body_text, list):
        body_text = "\n".join(body_text)
    body_text = clean_text(str(body_text).strip())

    if body_text:
        children.append({
            "object": "block", "type": "heading_2",
            "heading_2": {"rich_text": [{"type": "text", "text": {"content": "【本文引用/翻訳】"}}]}
        })

        # Notion paragraphブロックは2000文字制限
        is_truncated = len(body_text) > 2000
        display_body = (body_text[:1950] + "...") if is_truncated else body_text

        children.append({
            "object": "block", "type": "paragraph",
            "paragraph": {"rich_text": [{"type": "text", "text": {"content": display_body}}]}
        })

    # ソースリンク（Read more / ソース元）— 常に表示
    link_label = "続きを読む（Read more）→ " if (body_text and len(body_text) > 2000) else "ソース元: "
    children.append({
        "object": "block", "type": "paragraph",
        "paragraph": {
            "rich_text": [
                {"type": "text", "text": {"content": link_label}},
                {"type": "text", "text": {
                    "content": article_data["title"][:50],
                    "link": {"url": article_data["link"]}
                }}
            ]
        }
    })

    # ---- Notionへ保存（リトライ付き）----
    max_retries = 3
    for attempt in range(max_retries):
        try:
            notion.pages.create(
                parent={"database_id": DATABASE_ID},
                properties=props,
                children=children
            )
            safe_print("  [SUCCESS] Saved to Notion.")
            return True
        except Exception as e:
            err_msg = str(e)
            # 存在しないプロパティ名エラーなら自動除去してリトライ
            match = re.search(r"Property ['\"](.+?)['\"] is not a property", err_msg)
            if match:
                bad_prop = match.group(1)
                safe_print(f"  [FIX] Removing unknown property '{bad_prop}' and retrying...")
                props.pop(bad_prop, None)
                continue
            safe_print(f"  [ERROR] Notion save failed (attempt {attempt+1}/{max_retries}): {err_msg}")
            if attempt < max_retries - 1:
                time.sleep(5)
            else:
                return False
    return False


def get_page_text(url):
    """
    URLから記事本文テキストを抽出して返す（最大10000文字）。

    抽出戦略（優先順）:
      1. <article> タグ内の <p> テキストを結合（最も精度が高い）
      2. <article> タグがなければ、メインコンテンツ候補タグ内の <p> を結合
      3. それも取れなければ、ページ全体の <p> を結合
      4. <p> が極端に少なければ、ノイズ除去後の全テキストにフォールバック

    ※ 画像・図・広告・ナビなどのノイズを除去してから抽出する。
    """
    try:
        from bs4 import BeautifulSoup
        safe_print(f"  [HTTP] Fetching: {url}")
        resp = requests.get(
            url,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
            timeout=15
        )
        if resp.status_code != 200:
            safe_print(f"  [WARN] HTTP {resp.status_code}")
            return ""

        soup = BeautifulSoup(resp.content, "html.parser")

        # ── ノイズタグを除去 ──────────────────────────────────────────────
        noise_tags = [
            "script", "style", "nav", "header", "footer", "aside",
            "figure", "figcaption", "picture", "img",   # 画像関連
            "iframe", "video", "audio", "source",        # メディア
            "form", "button", "input", "select",         # フォーム
            "advertisement", "ads", "related", "share",  # 広告・SNS
        ]
        for tag in soup(noise_tags):
            tag.decompose()

        # ── 段落テキストを抽出するヘルパー ───────────────────────────────
        def extract_paragraphs(container):
            """コンテナ内の <p> タグからテキストを抽出し、空行除去して結合。"""
            paras = []
            for p in container.find_all("p"):
                txt = p.get_text(" ", strip=True)
                # 極端に短い断片（キャプション・日付など）は除外
                if len(txt) > 30:
                    paras.append(txt)
            return "\n\n".join(paras)

        # ── 戦略1: <article> タグ優先 ────────────────────────────────────
        article = soup.find("article")
        if article:
            text = extract_paragraphs(article)
            if len(text) > 200:
                safe_print(f"  [DATA] Extracted {len(text)} chars (<article> paragraphs)")
                return text[:10000]

        # ── 戦略2: メインコンテンツ候補タグ ──────────────────────────────
        for selector in [
            {"role": "main"}, {"id": "main"}, {"id": "content"},
            {"id": "article-body"}, {"class": "article-body"},
            {"class": "post-content"}, {"class": "entry-content"},
            {"class": "story-body"}, {"class": "article__body"},
        ]:
            container = soup.find(attrs=selector) or soup.find("main")
            if container:
                text = extract_paragraphs(container)
                if len(text) > 200:
                    safe_print(f"  [DATA] Extracted {len(text)} chars (main content paragraphs)")
                    return text[:10000]

        # ── 戦略3: ページ全体の <p> タグを結合 ───────────────────────────
        text = extract_paragraphs(soup)
        if len(text) > 200:
            safe_print(f"  [DATA] Extracted {len(text)} chars (all paragraphs)")
            return text[:10000]

        # ── 戦略4: 全テキストフォールバック ──────────────────────────────
        text = " ".join(soup.get_text().split())[:10000]
        safe_print(f"  [DATA] Extracted {len(text)} chars (full text fallback)")
        return text

    except Exception as e:
        safe_print(f"  [ERROR] get_page_text failed: {e}")
        return ""

# =============================================================================
# メイン処理
# =============================================================================
def main():
    processed_count = 0
    total_limit = 15  # 1回の実行で最大15件
    safe_print("=== Starting News Collection ===")

    now    = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=24)  # 直近24時間のみ対象

    for feed in RSS_FEEDS:
        if processed_count >= total_limit:
            break

        feed_name = feed["name"]
        safe_print(f"\n--- Feed: {feed_name} ---")

        try:
            resp = requests.get(feed["url"], headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
            if resp.status_code != 200:
                safe_print(f"  [WARN] Feed fetch failed: HTTP {resp.status_code}")
                continue
            entries = feedparser.parse(resp.content).entries
            safe_print(f"  [FEED] {len(entries)} entries found.")

            for entry in entries:
                if processed_count >= total_limit:
                    break

                title = getattr(entry, "title", "").strip()
                link  = getattr(entry, "link",  "").strip()
                if not title or not link:
                    continue

                # 24時間フィルター
                pub_parsed = entry.get("published_parsed")
                if pub_parsed:
                    entry_dt = datetime(*pub_parsed[:6], tzinfo=timezone.utc)
                    if entry_dt < cutoff:
                        continue  # 古い記事はスキップ
                    entry_date_str = entry_dt.isoformat()
                else:
                    # 日付不明は現在時刻（UTC・タイムゾーン付き）
                    entry_date_str = datetime.now(timezone.utc).isoformat()

                data = {
                    "title":   title,
                    "link":    link,
                    "summary": clean_text(entry.get("summary", entry.get("description", ""))),
                    "date":    entry_date_str,
                }

                safe_print(f"\n  → {title[:60]}")

                # ① 重複チェック（早期リターン）
                if is_duplicate(data["link"]):
                    safe_print("  [SKIP] Duplicate.")
                    continue

                # ② 関連性チェック（軽量AI）
                if not analyze_article_relevance(data):
                    continue

                # ③ 本文取得
                page_text = get_page_text(data["link"])
                if not page_text or len(page_text) < 200:
                    safe_print("  [INFO] Short/empty page text. Using RSS summary as fallback.")

                # ④ Gemini 全文分析
                result = analyze_article_with_gemini(data, page_text)
                if not result:
                    safe_print("  [SKIP] AI analysis returned None.")
                    continue

                # ⑤ Notion 保存
                if save_to_notion(result, data):
                    processed_count += 1
                    safe_print(f"  [COUNT] {processed_count}/{total_limit} saved.")

                    # Gemini レート制限対策: 3件ごとに60秒待機
                    if processed_count % 3 == 0:
                        safe_print("  [RATE LIMIT] Sleeping 60s (every 3 articles)...")
                        time.sleep(60)

                time.sleep(15)  # 記事間インターバル

            time.sleep(5)  # フィード間インターバル

        except Exception as e:
            safe_print(f"  [ERROR] Feed loop error ({feed_name}): {e}")
            traceback.print_exc()

    safe_print(f"\n=== COLLECTION COMPLETE ===")
    safe_print(f"Saved {processed_count} articles to Notion.")
    safe_print(f"===========================\n")


if __name__ == "__main__":
    main()
