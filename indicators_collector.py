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
    e-Stat APIから日本の住宅着工戸数（月次）を取得。
    統計ID: 0003103532（建築着工統計調査）
    start_year以降のデータを全件取得して返す。
    戻り値: [(date_str, value_float), ...] 古い順
    """
    url = "https://api.e-stat.go.jp/rest/3.0/app/json/getStatsData"
    params = {
        "appId":             ESTAT_API_KEY,
        "statsDataId":       "0003103532",
        "metaGetFlg":        "N",
        "cntGetFlg":         "N",
        "explanationGetFlg": "N",
        "limit":             200,        # 十分な件数を取得
        "startPosition":     1,
    }
    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
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
            if not val or val == "-":
                continue
            # "@time": "2024000010" → 年=2024, 月=10
            try:
                year  = int(raw_time[:4])
                month = int(raw_time[6:8]) if len(raw_time) >= 8 else 0
                if year < start_year or month == 0:
                    continue
                date_str = f"{year}-{month:02d}-01"
                results.append((date_str, float(val)))
            except Exception:
                continue
        # 古い順にソート
        results.sort(key=lambda x: x[0])
        safe_print(f"  [e-Stat] 日本住宅着工: {len(results)}件取得")
        return results
    except Exception as e:
        safe_print(f"  [ERROR] e-Stat: {e}")
    return []

# =============================================================================
# Eurostat API（欧州建設生産指数）
# =============================================================================
def fetch_eurostat_construction(start_year=2020):
    """
    Eurostat APIからEU建設生産指数（月次）を取得。
    データセット: sts_copr_m（建設生産指数）
    2020年以降の全データを取得して返す。
    戻り値: [(date_str, value_float), ...] 古い順
    """
    url = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/sts_copr_m"
    params = {
        "format":          "JSON",
        "lang":            "EN",
        "geo":             "EU27_2020",
        "nace_r2":         "F",        # 建設業
        "s_adj":           "NSA",      # 季節調整なし
        "unit":            "I21",      # 2021年=100の指数
        "sinceTimePeriod": f"{start_year}-01",
    }
    try:
        resp = requests.get(url, params=params, timeout=20)
        if resp.status_code == 400:
            # パラメータを緩めて再試行（unitなし）
            params2 = {k: v for k, v in params.items() if k != "unit"}
            resp = requests.get(url, params=params2, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        vals = data.get("value", {})
        dims = data.get("dimension", {})
        time_cat = dims.get("time", {}).get("category", {})
        time_idx = time_cat.get("index", {})   # {"2020-01": 0, "2020-02": 1, ...}

        results = []
        for t, idx in sorted(time_idx.items()):
            v = vals.get(str(idx))
            if v is not None:
                results.append((t, float(v)))

        safe_print(f"  [Eurostat] EU建設生産指数: {len(results)}件取得")
        return results
    except Exception as e:
        safe_print(f"  [ERROR] Eurostat: {e}")
    return []

# =============================================================================
# 各指標を収集してSheetsに書き込む
# =============================================================================
def write_bulk(sheet, rows):
    """複数行をまとめてSheetsに追記する（重複スキップ）。"""
    added = 0
    for date_str, val in rows:
        if append_if_new(sheet, [date_str, val], date_str):
            added += 1
    safe_print(f"  [SHEETS] {added}件追記完了")


def collect_and_write(spreadsheet):
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    safe_print(f"\n=== Collecting indicators ({today}) ===\n")

    # ── 1. 金価格（USD/troy oz）────────────────────────────────────────────
    # GOLDPMGBD228NLBM = ロンドン金午後値決め（GOLDAMGBD228NLBMの後継）
    sheet = get_or_create_sheet(spreadsheet, "金価格", ["日付", "金価格 (USD/oz)"])
    rows = fetch_fred("GOLDPMGBD228NLBM", "金価格")
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

    # ── 7. 日本住宅着工（戸）──────────────────────────────────────────────
    sheet = get_or_create_sheet(spreadsheet, "日本住宅着工", ["期間", "日本住宅着工 (戸)"])
    rows = fetch_estat_housing()
    write_bulk(sheet, rows)
    time.sleep(2)

    # ── 8. 欧州建設生産指数（2021=100）────────────────────────────────────
    sheet = get_or_create_sheet(spreadsheet, "欧州建設生産指数", ["年月", "EU建設生産指数 (2021=100)"])
    rows = fetch_eurostat_construction()
    write_bulk(sheet, rows)
    time.sleep(2)

    # ── 9. 中国製造業PMI ──────────────────────────────────────────────────
    # CMRMKSCU01MLSAM = Caixin中国製造業PMI（FREDで利用可能な代替series）
    sheet = get_or_create_sheet(spreadsheet, "中国製造業PMI", ["日付", "中国製造業PMI"])
    rows = fetch_fred("CMRMKSCU01MLSAM", "中国製造業PMI")
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
