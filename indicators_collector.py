"""
indicators_collector.py
建設機械市場指標データ収集スクリプト

取得指標:
  - 金価格          (FRED: GOLDAMGBD228NLBM)
  - 銅価格          (FRED: PCOPPUSDM)
  - 原油価格 WTI    (FRED: DCOILWTICO)
  - 石炭価格        (World Bank: COAL)
  - 鉄鉱石価格      (World Bank: IRON)
  - 北米住宅着工    (FRED: HOUST)
  - 日本住宅着工    (e-Stat API)
  - 欧州建設生産    (Eurostat API)
  - 中国製造業PMI   (FRED: CHPMINDXM)

出力: Google Sheets（gspread）
実行: GitHub Actions 週1回（月曜 JST 6:00）
"""

import os
import sys
import json
import time
import requests
from datetime import datetime, timezone
import gspread
from google.oauth2.service_account import Credentials

# =============================================================================
# 環境変数から設定を読み込み
# =============================================================================
FRED_API_KEY              = os.getenv("FRED_API_KEY")
ESTAT_API_KEY             = os.getenv("ESTAT_API_KEY")
GOOGLE_SHEETS_CREDENTIALS = os.getenv("GOOGLE_SHEETS_CREDENTIALS")
INDICATORS_SHEET_ID       = os.getenv("INDICATORS_SHEET_ID")

def safe_print(text):
    try:
        print(text, flush=True)
    except Exception:
        print(str(text).encode("utf-8", errors="replace").decode("utf-8"), flush=True)

def check_env():
    safe_print("--- Environment Diagnostics ---")
    missing = []
    for name, val in [
        ("FRED_API_KEY",              FRED_API_KEY),
        ("ESTAT_API_KEY",             ESTAT_API_KEY),
        ("GOOGLE_SHEETS_CREDENTIALS", GOOGLE_SHEETS_CREDENTIALS),
        ("INDICATORS_SHEET_ID",       INDICATORS_SHEET_ID),
    ]:
        status = "OK" if val else "MISSING"
        safe_print(f"  {name}: {status}")
        if not val:
            missing.append(name)
    if missing:
        safe_print(f"\n[CRITICAL] Missing secrets: {', '.join(missing)}")
        sys.exit(1)
    safe_print("-------------------------------\n")

# =============================================================================
# Google Sheets 接続
# =============================================================================
def connect_sheets():
    """サービスアカウントでGoogle Sheetsに接続する。"""
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds_dict = json.loads(GOOGLE_SHEETS_CREDENTIALS)
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(INDICATORS_SHEET_ID)
    safe_print(f"  [SHEETS] Connected: {spreadsheet.title}")
    return spreadsheet

# =============================================================================
# シートの取得 or 作成
# =============================================================================
def get_or_create_sheet(spreadsheet, sheet_name, headers):
    """シートが存在しなければ作成してヘッダーを書き込む。"""
    try:
        sheet = spreadsheet.worksheet(sheet_name)
        safe_print(f"  [SHEETS] Found sheet: {sheet_name}")
    except gspread.WorksheetNotFound:
        sheet = spreadsheet.add_worksheet(title=sheet_name, rows=500, cols=10)
        sheet.append_row(headers)
        safe_print(f"  [SHEETS] Created sheet: {sheet_name}")
    return sheet

def is_date_already_recorded(sheet, date_str):
    """同じ日付のデータが既に存在するか確認（重複防止）。"""
    try:
        col_a = sheet.col_values(1)  # A列（日付列）
        return date_str in col_a
    except Exception:
        return False

def append_if_new(sheet, row_data, date_str):
    """日付が未記録の場合のみ行を追記する。"""
    if is_date_already_recorded(sheet, date_str):
        safe_print(f"  [SKIP] Already recorded: {date_str}")
        return False
    sheet.append_row(row_data)
    safe_print(f"  [OK] Appended: {date_str}")
    return True

# =============================================================================
# FRED API 共通取得関数
# =============================================================================
def fetch_gold_price(start_year=2020):
    """
    freegoldapi.com から月次金価格データを取得（無料・APIキー不要）。
    GitHubリポジトリのCSVを直接取得する。
    戻り値: [(date_str, value_float), ...] 古い順
    """
    url = "https://raw.githubusercontent.com/datasets/gold-prices/main/data/monthly.csv"
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        results = []
        for line in resp.text.strip().split("\n")[1:]:  # ヘッダースキップ
            parts = line.strip().split(",")
            if len(parts) < 2:
                continue
            date_str = parts[0].strip()   # 例: "2020-01-01"
            val_str  = parts[1].strip()
            try:
                year = int(date_str[:4])
                if year < start_year:
                    continue
                results.append((date_str, float(val_str)))
            except Exception:
                continue
        results.sort(key=lambda x: x[0])
        safe_print(f"  [GOLD] 金価格: {len(results)}件取得")
        return results
    except Exception as e:
        safe_print(f"  [ERROR] 金価格取得失敗: {e}")
        return []


def fetch_fred(series_id, label, start_date="2020-01-01"):
    """
    FRED APIから過去データを複数件取得して返す。
    戻り値: [(date_str, value_float), ...] 古い順
    """
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id":         series_id,
        "api_key":           FRED_API_KEY,
        "file_type":         "json",
        "sort_order":        "asc",
        "observation_start": start_date,
    }
    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        obs = resp.json().get("observations", [])
        results = [(o["date"], float(o["value"])) for o in obs if o["value"] != "."]
        safe_print(f"  [FRED] {label}: {len(results)}件取得 (最新: {results[-1] if results else 'なし'})")
        return results
    except Exception as e:
        safe_print(f"  [ERROR] FRED {label}: {e}")
    return []

# =============================================================================
# World Bank API
# =============================================================================
def fetch_china_pmi(start_year=2020):
    """
    中国国家統計局(NBS)の製造業PMI（官方PMI）を取得。
    OECDのAPI（無料・APIキー不要）から取得する。
    エンドポイント: https://sdmx.oecd.org/public/rest/data/
    代替: GitHub公開CSVデータを使用
    戻り値: [(date_str, value_float), ...] 古い順
    """
    # OECD Stats API経由でChinese Manufacturing PMIを取得
    # Dataset: MEI_BTS_COS, Series: CN.BSCICP02.STSA.M (China PMI)
    url = "https://sdmx.oecd.org/public/rest/data/OECD.SDD.STES,DSD_STES@DF_CLI/CHN.BSCICP02.STSA.M"
    params = {
        "startPeriod": f"{start_year}-01",
        "format": "csvfilewithlabels",
    }
    try:
        resp = requests.get(url, params=params, timeout=20,
                            headers={"Accept": "text/csv"})
        if resp.status_code == 200 and resp.text.strip():
            results = []
            for line in resp.text.strip().split("\n")[1:]:
                parts = line.split(",")
                if len(parts) < 2:
                    continue
                try:
                    # 期間列と値列を探す（CSVヘッダーに依存）
                    # 通常: TIME_PERIOD, OBS_VALUE
                    date_str = None
                    val = None
                    for p in parts:
                        p = p.strip().strip('"')
                        if len(p) == 7 and p[4] == "-":  # "2020-01" 形式
                            date_str = p + "-01"
                        elif p.replace(".", "").replace("-", "").isdigit() and "." in p:
                            val = float(p)
                    if date_str and val and int(date_str[:4]) >= start_year:
                        results.append((date_str, val))
                except Exception:
                    continue
            if results:
                results.sort(key=lambda x: x[0])
                safe_print(f"  [OECD] 中国製造業PMI: {len(results)}件取得")
                return results

    except Exception as e:
        safe_print(f"  [WARN] OECD PMI取得失敗: {e}")

    # フォールバック: 国家統計局公表値の静的リスト（2020年以降の主要値）
    # GitHub datasets/pmi からも取得試行
    try:
        url2 = "https://raw.githubusercontent.com/datasets/pmi/refs/heads/main/data/manufacturing-pmi.csv"
        resp2 = requests.get(url2, timeout=15)
        if resp2.status_code == 200:
            results = []
            for line in resp2.text.strip().split("\n")[1:]:
                parts = line.strip().split(",")
                if len(parts) < 3:
                    continue
                try:
                    date_str = parts[0].strip()
                    # China列を探す（ヘッダーで確認）
                    year = int(date_str[:4])
                    if year < start_year:
                        continue
                    # China PMIは通常3列目付近
                    val = float(parts[2].strip()) if parts[2].strip() else None
                    if val and 40 < val < 65:  # PMIの妥当範囲
                        results.append((date_str[:7] + "-01", val))
                except Exception:
                    continue
            if results:
                results.sort(key=lambda x: x[0])
                safe_print(f"  [GitHub] 中国製造業PMI: {len(results)}件取得")
                return results
    except Exception as e2:
        safe_print(f"  [WARN] GitHub PMI取得失敗: {e2}")

    safe_print("  [WARN] 中国製造業PMI: 全ソースで取得失敗")
    return []


def fetch_worldbank_commodity(commodity_id, label, start_year=2020):
    """
    World Bank Commodity Price API（Pink Sheet）から月次データを取得。
    commodity_id: "COAL_AUS"（石炭）, "IRON_ORE"（鉄鉱石）など
    エンドポイント: https://api.worldbank.org/v2/en/indicator/PCOALAUUSDM
    ※ World Bank Commodity APIは indicator形式を使用
    """
    # 石炭・鉄鉱石はFREDからも取得可能なので FRED を代替使用
    fred_map = {
        "COAL":     "PCOALAUUSDM",   # 石炭価格 (World Bank series via FRED)
        "IRON_ORE": "PIORECRUSDM",   # 鉄鉱石価格 (World Bank series via FRED)
    }
    series_id = fred_map.get(commodity_id)
    if not series_id:
        safe_print(f"  [WARN] Unknown commodity: {commodity_id}")
        return []

    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id":         series_id,
        "api_key":           FRED_API_KEY,
        "file_type":         "json",
        "sort_order":        "asc",
        "observation_start": f"{start_year}-01-01",
    }
    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        obs = resp.json().get("observations", [])
        results = [(o["date"], float(o["value"])) for o in obs if o["value"] != "."]
        safe_print(f"  [FRED/WB] {label}: {len(results)}件取得")
        return results
    except Exception as e:
        safe_print(f"  [ERROR] WorldBank/FRED {label}: {e}")
    return []

# =============================================================================
# e-Stat API（日本住宅着工）
# =============================================================================
def fetch_estat_housing(start_year=2020):
    """
    e-Stat APIから日本の新設住宅着工戸数（月次）を取得。
    statsDataId: 0003103119
      = 建築着工統計調査・住宅着工統計・新設住宅着工戸数合計（全国・月次）
    戻り値: [(date_str, value_float), ...] 古い順
    """
    url = "https://api.e-stat.go.jp/rest/3.0/app/json/getStatsData"
    # 複数のstatsDataIdを試して最初に成功したものを使う
    candidate_ids = [
        "0003103119",   # 新設住宅着工戸数（月次・全国）
        "0003103532",   # フォールバック（旧ID）
    ]
    for stats_id in candidate_ids:
        params = {
            "appId":             ESTAT_API_KEY,
            "statsDataId":       stats_id,
            "metaGetFlg":        "N",
            "cntGetFlg":         "N",
            "explanationGetFlg": "N",
            "limit":             300,
            "startPosition":     1,
        }
        try:
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            # エラーレスポンスチェック
            status = data.get("GET_STATS_DATA", {}).get("RESULT", {}).get("STATUS", 1)
            if status != 0:
                safe_print(f"  [WARN] e-Stat ID {stats_id}: status={status}, 次のIDを試します")
                continue

            values = (
                data.get("GET_STATS_DATA", {})
                    .get("STATISTICAL_DATA", {})
                    .get("DATA_INF", {})
                    .get("VALUE", [])
            )
            results = []
            for item in values:
                raw_time = item.get("@time", "")
                val      = item.get("$", "")
                if not val or val in ("-", "***", "..."):
                    continue
                try:
                    # "@time" 形式: "2024000010"（年4桁 + "0000" + 月2桁）
                    year  = int(raw_time[:4])
                    month_str = raw_time[6:8] if len(raw_time) >= 8 else "00"
                    month = int(month_str)
                    if year < start_year or month == 0:
                        continue
                    date_str = f"{year}-{month:02d}-01"
                    # 地域コード「全国」のみ（@areaが"00000"または未指定）
                    area = item.get("@area", "00000")
                    if area not in ("00000", "0", ""):
                        continue
                    # 合計行のみ（@cat01が存在する場合は「合計」コードのみ）
                    results.append((date_str, float(val)))
                except Exception:
                    continue

            if results:
                results.sort(key=lambda x: x[0])
                # 同一日付は最初の値のみ残す
                seen = {}
                deduped = []
                for d, v in results:
                    if d not in seen:
                        seen[d] = True
                        deduped.append((d, v))
                safe_print(f"  [e-Stat] 日本住宅着工 (ID:{stats_id}): {len(deduped)}件取得")
                return deduped
            else:
                safe_print(f"  [WARN] e-Stat ID {stats_id}: データ0件、次のIDを試します")
        except Exception as e:
            safe_print(f"  [ERROR] e-Stat ID {stats_id}: {e}")
    safe_print("  [ERROR] e-Stat: 全IDで取得失敗")
    return []

# =============================================================================
# Eurostat API（欧州建設生産指数）
# =============================================================================
def fetch_eurostat_construction(start_year=2020):
    """
    Eurostat APIからEU建設生産指数（月次）を取得。
    複数のパラメータセットを試してどれかで成功したら返す。
    戻り値: [(date_str, value_float), ...] 古い順
    """
    base_url = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/sts_copr_m"

    # パラメータセットを複数用意（APIバージョンごとに変わりやすいため）
    param_sets = [
        # セット1: 最新仕様（unit=I21）
        {
            "format": "JSON", "lang": "EN",
            "geo": "EU27_2020", "nace_r2": "F",
            "s_adj": "NSA", "unit": "I21",
            "sinceTimePeriod": f"{start_year}-01",
        },
        # セット2: unit=I15（2015年基準）
        {
            "format": "JSON", "lang": "EN",
            "geo": "EU27_2020", "nace_r2": "F",
            "s_adj": "NSA", "unit": "I15",
            "sinceTimePeriod": f"{start_year}-01",
        },
        # セット3: unitなし・EU28
        {
            "format": "JSON", "lang": "EN",
            "geo": "EU28", "nace_r2": "F",
            "s_adj": "NSA",
            "sinceTimePeriod": f"{start_year}-01",
        },
        # セット4: 季節調整あり
        {
            "format": "JSON", "lang": "EN",
            "geo": "EU27_2020", "nace_r2": "F",
            "s_adj": "SCA", "unit": "I21",
            "sinceTimePeriod": f"{start_year}-01",
        },
    ]

    for i, params in enumerate(param_sets):
        try:
            resp = requests.get(base_url, params=params, timeout=20)
            if resp.status_code != 200:
                safe_print(f"  [WARN] Eurostat セット{i+1}: {resp.status_code}")
                continue
            data = resp.json()
            vals = data.get("value", {})
            dims = data.get("dimension", {})
            time_idx = dims.get("time", {}).get("category", {}).get("index", {})

            results = []
            for t, idx in sorted(time_idx.items()):
                v = vals.get(str(idx))
                if v is not None:
                    # 日付フォーマットを統一（"2020-01" → "2020-01-01"）
                    date_str = t + "-01" if len(t) == 7 else t
                    results.append((date_str, float(v)))

            if results:
                safe_print(f"  [Eurostat] EU建設生産指数(セット{i+1}): {len(results)}件取得")
                return results
        except Exception as e:
            safe_print(f"  [ERROR] Eurostat セット{i+1}: {e}")

    safe_print("  [WARN] Eurostat: 全パラメータセットで取得失敗")
    return []

# =============================================================================
# 各指標を収集してSheetsに書き込む
# =============================================================================
def write_bulk(sheet, rows):
    """
    複数行をまとめてSheetsに追記する（重複スキップ）。
    Google Sheets APIの書き込みレート制限（429）対策:
      - 既存データを一括取得して重複チェック
      - 新規データをまとめて1回のAPIコールで書き込む
      - それでも429が出た場合は60秒待ってリトライ
    """
    import gspread.exceptions

    # 既存の日付を一括取得（APIコール1回で済む）
    try:
        existing_dates = set(sheet.col_values(1))
    except Exception:
        existing_dates = set()

    # 新規データだけ抽出
    new_rows = [[date_str, val] for date_str, val in rows if date_str not in existing_dates]

    if not new_rows:
        safe_print(f"  [SHEETS] 新規データなし（全件重複）")
        return

    # まとめて1回で書き込む（APIコール削減）
    for attempt in range(3):
        try:
            sheet.append_rows(new_rows, value_input_option="USER_ENTERED")
            safe_print(f"  [SHEETS] {len(new_rows)}件追記完了")
            return
        except gspread.exceptions.APIError as e:
            if "429" in str(e):
                wait = 60 * (attempt + 1)
                safe_print(f"  [WARN] 429 Rate limit. {wait}秒待機してリトライ...")
                time.sleep(wait)
            else:
                safe_print(f"  [ERROR] Sheets書き込みエラー: {e}")
                return
    safe_print(f"  [ERROR] 3回リトライ後も失敗。スキップします。")


def collect_and_write(spreadsheet):
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    safe_print(f"\n=== Collecting indicators ({today}) ===\n")

    # ── 1. 金価格（USD/troy oz）────────────────────────────────────────────
    # freegoldapi.com: 無料・APIキー不要・月次データ
    sheet = get_or_create_sheet(spreadsheet, "金価格", ["日付", "金価格 (USD/oz)"])
    rows = fetch_gold_price()
    write_bulk(sheet, rows)
    time.sleep(2)

    # ── 2. 銅価格（USD/lb）────────────────────────────────────────────────
    sheet = get_or_create_sheet(spreadsheet, "銅価格", ["日付", "銅価格 (USD/lb)"])
    rows = fetch_fred("PCOPPUSDM", "銅価格")
    write_bulk(sheet, rows)
    time.sleep(2)

    # ── 3. 原油価格 WTI（USD/bbl）─────────────────────────────────────────
    sheet = get_or_create_sheet(spreadsheet, "原油価格WTI", ["日付", "WTI原油 (USD/bbl)"])
    rows = fetch_fred("DCOILWTICO", "原油WTI")
    write_bulk(sheet, rows)
    time.sleep(2)

    # ── 4. 石炭価格（USD/ton）─────────────────────────────────────────────
    # PCOALAUUSDM = オーストラリア炭（Newcastle）月次
    sheet = get_or_create_sheet(spreadsheet, "石炭価格", ["日付", "石炭価格 (USD/ton)"])
    rows = fetch_worldbank_commodity("COAL", "石炭価格")
    write_bulk(sheet, rows)
    time.sleep(2)

    # ── 5. 鉄鉱石価格（USD/dmtu）─────────────────────────────────────────
    # PIORECRUSDM = 鉄鉱石スポット価格（中国向け）月次
    sheet = get_or_create_sheet(spreadsheet, "鉄鉱石価格", ["日付", "鉄鉱石価格 (USD/dmtu)"])
    rows = fetch_worldbank_commodity("IRON_ORE", "鉄鉱石価格")
    write_bulk(sheet, rows)
    time.sleep(2)

    # ── 6. 北米住宅着工件数（千件）────────────────────────────────────────
    sheet = get_or_create_sheet(spreadsheet, "北米住宅着工", ["日付", "北米住宅着工 (千件)"])
    rows = fetch_fred("HOUST", "北米住宅着工")
    write_bulk(sheet, rows)
    time.sleep(2)

    # ── 7. 日本住宅着工（前年比%・OECD/FRED経由）────────────────────────
    # WSCNDW01JPM661S = 日本の住宅着工・前年同月比（月次・OECD MEI）
    # WSCNDW01JPA661S = 年次版（月次がない場合のフォールバック）
    sheet = get_or_create_sheet(spreadsheet, "日本住宅着工", ["日付", "日本住宅着工 前年比(%)"])
    rows = fetch_fred("WSCNDW01JPM661S", "日本住宅着工(月次)")
    if not rows:
        rows = fetch_fred("WSCNDW01JPA661S", "日本住宅着工(年次)")
    write_bulk(sheet, rows)
    time.sleep(2)

    # ── 8. 欧州建設生産指数（2021=100）────────────────────────────────────
    sheet = get_or_create_sheet(spreadsheet, "欧州建設生産指数", ["年月", "EU建設生産指数 (2021=100)"])
    rows = fetch_eurostat_construction()
    write_bulk(sheet, rows)
    time.sleep(2)

    # ── 9. 中国製造業PMI ──────────────────────────────────────────────────
    # 国家統計局(NBS)の官方製造業PMI - GitHub公開データから取得
    sheet = get_or_create_sheet(spreadsheet, "中国製造業PMI", ["日付", "中国製造業PMI"])
    rows = fetch_china_pmi()
    write_bulk(sheet, rows)

    safe_print("\n=== Collection complete ===")

# =============================================================================
# メイン
# =============================================================================
def main():
    check_env()
    spreadsheet = connect_sheets()
    collect_and_write(spreadsheet)

if __name__ == "__main__":
    main()
