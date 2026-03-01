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
def fetch_fred(series_id, label):
    """
    FRED APIから最新値を1件取得して返す。
    戻り値: (date_str, value_float) or (None, None)
    """
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id":        series_id,
        "api_key":          FRED_API_KEY,
        "file_type":        "json",
        "sort_order":       "desc",
        "limit":            1,
        "observation_start": "2020-01-01",
    }
    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        obs = resp.json().get("observations", [])
        if obs and obs[0]["value"] != ".":
            date_str = obs[0]["date"]
            value    = float(obs[0]["value"])
            safe_print(f"  [FRED] {label}: {value} ({date_str})")
            return date_str, value
        safe_print(f"  [WARN] {label}: No data returned")
    except Exception as e:
        safe_print(f"  [ERROR] FRED {label}: {e}")
    return None, None

# =============================================================================
# World Bank API
# =============================================================================
def fetch_worldbank(indicator_code, label):
    """
    World Bank APIから最新の月次コモディティ価格を取得。
    indicator_code例: "PCOALAUUSDM"（石炭）, "PIORECRUSDM"（鉄鉱石）
    """
    url = f"https://api.worldbank.org/v2/en/indicator/{indicator_code}"
    params = {
        "format":   "json",
        "mrv":      3,        # 最新3件
        "frequency": "M",
    }
    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        # World Bank レスポンスは [metadata, [data...]] の形式
        if len(data) >= 2 and data[1]:
            for item in data[1]:
                if item.get("value") is not None:
                    date_str = item["date"]          # 例: "2024M10"
                    value    = float(item["value"])
                    # 表示用に年月フォーマットを整形
                    display_date = date_str.replace("M", "-")
                    safe_print(f"  [WB] {label}: {value} ({display_date})")
                    return display_date, value
        safe_print(f"  [WARN] WorldBank {label}: No data")
    except Exception as e:
        safe_print(f"  [ERROR] WorldBank {label}: {e}")
    return None, None

# =============================================================================
# e-Stat API（日本住宅着工）
# =============================================================================
def fetch_estat_housing():
    """
    e-Stat APIから日本の住宅着工戸数（月次）を取得。
    統計ID: 0003103532（建築着工統計調査）
    """
    url = "https://api.e-stat.go.jp/rest/3.0/app/json/getStatsData"
    params = {
        "appId":       ESTAT_API_KEY,
        "statsDataId": "0003103532",  # 建築着工統計
        "metaGetFlg":  "N",
        "cntGetFlg":   "N",
        "explanationGetFlg": "N",
        "limit":       3,
        "startPosition": 1,
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
        if values:
            latest = values[0]
            date_str = latest.get("@time", "").replace("CY", "")  # 例: "2024000010"
            val      = latest.get("$", "")
            if val and val != "-":
                value = float(val)
                safe_print(f"  [e-Stat] 日本住宅着工: {value}戸 ({date_str})")
                return date_str, value
        safe_print("  [WARN] e-Stat: No housing data")
    except Exception as e:
        safe_print(f"  [ERROR] e-Stat: {e}")
    return None, None

# =============================================================================
# Eurostat API（欧州建設生産指数）
# =============================================================================
def fetch_eurostat_construction():
    """
    Eurostat APIからEU建設生産指数（月次）を取得。
    データセット: sts_copr_m（建設生産指数）
    """
    url = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/sts_copr_m"
    params = {
        "format":   "JSON",
        "lang":     "EN",
        "geo":      "EU27_2020",
        "nace_r2":  "F",         # 建設業
        "s_adj":    "NSA",       # 季節調整なし
        "unit":     "I21",       # 2021年=100の指数
        "sinceTimePeriod": "2023-01",
    }
    try:
        resp = requests.get(url, params=params, timeout=20)
        resp.raise_for_status()
        data  = resp.json()
        vals  = data.get("value", {})
        dims  = data.get("dimension", {})
        times = list(dims.get("time", {}).get("category", {}).get("index", {}).keys())

        if vals and times:
            # 最新のデータを取得（降順でソート）
            sorted_times = sorted(times, reverse=True)
            for t in sorted_times:
                idx = dims["time"]["category"]["index"][t]
                v   = vals.get(str(idx))
                if v is not None:
                    safe_print(f"  [Eurostat] EU建設生産指数: {v} ({t})")
                    return t, float(v)
        safe_print("  [WARN] Eurostat: No data")
    except Exception as e:
        safe_print(f"  [ERROR] Eurostat: {e}")
    return None, None

# =============================================================================
# 各指標を収集してSheetsに書き込む
# =============================================================================
def collect_and_write(spreadsheet):
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    safe_print(f"\n=== Collecting indicators ({today}) ===\n")

    # ── 1. 金価格（USD/troy oz）────────────────────────────────────────────
    sheet = get_or_create_sheet(spreadsheet, "金価格",
        ["日付", "金価格 (USD/oz)"])
    date, val = fetch_fred("GOLDAMGBD228NLBM", "金価格")
    if date and val:
        append_if_new(sheet, [date, val], date)
    time.sleep(2)

    # ── 2. 銅価格（USD/lb）────────────────────────────────────────────────
    sheet = get_or_create_sheet(spreadsheet, "銅価格",
        ["日付", "銅価格 (USD/lb)"])
    date, val = fetch_fred("PCOPPUSDM", "銅価格")
    if date and val:
        append_if_new(sheet, [date, val], date)
    time.sleep(2)

    # ── 3. 原油価格 WTI（USD/bbl）─────────────────────────────────────────
    sheet = get_or_create_sheet(spreadsheet, "原油価格WTI",
        ["日付", "WTI原油 (USD/bbl)"])
    date, val = fetch_fred("DCOILWTICO", "原油WTI")
    if date and val:
        append_if_new(sheet, [date, val], date)
    time.sleep(2)

    # ── 4. 石炭価格（USD/ton）─────────────────────────────────────────────
    sheet = get_or_create_sheet(spreadsheet, "石炭価格",
        ["年月", "石炭価格 (USD/ton)"])
    date, val = fetch_worldbank("PCOALAUUSDM", "石炭価格")
    if date and val:
        append_if_new(sheet, [date, val], date)
    time.sleep(2)

    # ── 5. 鉄鉱石価格（USD/dmtu）─────────────────────────────────────────
    sheet = get_or_create_sheet(spreadsheet, "鉄鉱石価格",
        ["年月", "鉄鉱石価格 (USD/dmtu)"])
    date, val = fetch_worldbank("PIORECRUSDM", "鉄鉱石価格")
    if date and val:
        append_if_new(sheet, [date, val], date)
    time.sleep(2)

    # ── 6. 北米住宅着工件数（千件）────────────────────────────────────────
    sheet = get_or_create_sheet(spreadsheet, "北米住宅着工",
        ["日付", "北米住宅着工 (千件)"])
    date, val = fetch_fred("HOUST", "北米住宅着工")
    if date and val:
        append_if_new(sheet, [date, val], date)
    time.sleep(2)

    # ── 7. 日本住宅着工（戸）──────────────────────────────────────────────
    sheet = get_or_create_sheet(spreadsheet, "日本住宅着工",
        ["期間", "日本住宅着工 (戸)"])
    date, val = fetch_estat_housing()
    if date and val:
        append_if_new(sheet, [date, val], date)
    time.sleep(2)

    # ── 8. 欧州建設生産指数（2021=100）────────────────────────────────────
    sheet = get_or_create_sheet(spreadsheet, "欧州建設生産指数",
        ["年月", "EU建設生産指数 (2021=100)"])
    date, val = fetch_eurostat_construction()
    if date and val:
        append_if_new(sheet, [date, val], date)
    time.sleep(2)

    # ── 9. 中国製造業PMI ──────────────────────────────────────────────────
    sheet = get_or_create_sheet(spreadsheet, "中国製造業PMI",
        ["日付", "中国製造業PMI"])
    date, val = fetch_fred("CHPMINDXM", "中国製造業PMI")
    if date and val:
        append_if_new(sheet, [date, val], date)

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
