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
def fetch_gold_price(start_year=2015):
    """
    金価格（USD/troy oz）月次データを取得。
    ソース優先順:
      1. FRED GOLDAMGBD228NLBM (ロンドン金市場AM値、最新まで)
      2. FRED GOLDPMGBD228NLBM (ロンドン金市場PM値)
      3. ECB EXR XAU/USD (数ヶ月ラグあり)
      4. GitHub datasets CSV (フォールバック)
    戻り値: [(date_str, value_float), ...] 古い順
    """
    # ── ソース1&2: FRED 金価格 (最も確実・最新まで月次)
    for sid in ["GOLDAMGBD228NLBM", "GOLDPMGBD228NLBM"]:
        try:
            url = "https://api.stlouisfed.org/fred/series/observations"
            params = {
                "series_id":         sid,
                "api_key":           FRED_API_KEY,
                "file_type":         "json",
                "sort_order":        "asc",
                "observation_start": f"{start_year}-01-01",
                "observation_end":   datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            }
            resp = requests.get(url, params=params, timeout=15)
            if resp.status_code == 200:
                obs = resp.json().get("observations", [])
                tmp = [(o["date"], float(o["value"])) for o in obs
                       if o.get("value", ".") != "."]
                if tmp:
                    tmp.sort(key=lambda x: x[0])
                    safe_print(f"  [GOLD] FRED {sid}: {len(tmp)}件取得（最新: {tmp[-1][0]}）")
                    return tmp
        except Exception as e:
            safe_print(f"  [WARN] FRED {sid}: {e}")

    # ── ソース3: ECB EXR XAU/USD（確定値は数ヶ月ラグあり）
    try:
        url = "https://data-api.ecb.europa.eu/service/data/EXR/M.XAU.USD.SP00.A"
        params = {
            "startPeriod": f"{start_year}-01",
            "detail": "dataonly",
            "format": "csvfilewithlabels",
        }
        resp = requests.get(url, params=params, timeout=20,
                            headers={"Accept": "text/csv"})
        if resp.status_code == 200 and resp.text.strip():
            lines = resp.text.strip().split("\n")
            header = [h.strip().strip('"') for h in lines[0].split(",")]
            try:
                ti = header.index("TIME_PERIOD")
                vi = header.index("OBS_VALUE")
            except ValueError:
                ti, vi = 0, 1
            tmp = []
            for line in lines[1:]:
                parts = [p.strip().strip('"') for p in line.split(",")]
                if len(parts) <= max(ti, vi):
                    continue
                try:
                    period = parts[ti]
                    val = float(parts[vi])
                    year = int(period[:4])
                    if year < start_year:
                        continue
                    tmp.append((period[:7] + "-01", val))
                except Exception:
                    continue
            if tmp:
                tmp.sort(key=lambda x: x[0])
                safe_print(f"  [GOLD] ECB XAU/USD: {len(tmp)}件取得（最新: {tmp[-1][0]}）")
                return tmp
    except Exception as e:
        safe_print(f"  [WARN] ECB gold: {e}")

    # ── ソース4: GitHub datasets CSV (フォールバック)
    try:
        url = "https://raw.githubusercontent.com/datasets/gold-prices/main/data/monthly.csv"
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        tmp = []
        for line in resp.text.strip().split("\n")[1:]:
            parts = line.strip().split(",")
            if len(parts) < 2:
                continue
            date_str = parts[0].strip()
            val_str  = parts[1].strip()
            try:
                year = int(date_str[:4])
                if year < start_year:
                    continue
                tmp.append((date_str, float(val_str)))
            except Exception:
                continue
        if tmp:
            tmp.sort(key=lambda x: x[0])
            safe_print(f"  [GOLD] GitHub CSV: {len(tmp)}件取得（最新: {tmp[-1][0]}）")
            return tmp
    except Exception as e:
        safe_print(f"  [ERROR] 金価格全ソース失敗: {e}")

    return []


def fetch_fred(series_id, label, start_date="2015-01-01"):
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
def fetch_worldbank_commodity(commodity_id, label, start_year=2015):
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
def fetch_estat_housing(start_year=2015):
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
def fetch_japan_housing(start_year=2015):
    """
    日本の新設住宅着工戸数（月次・絶対値）を取得。
    ソース優先順:
      1. e-Stat 住宅着工統計 DB (statsDataId=0003103532, 月次時系列)
      2. OECD.Stat API (PRMNTO01.JPN.ST.M)
      3. Trading Economics / MLIT 公式 (スクレイピング不可のためスキップ)
    戻り値: [(date_str, value_float), ...] 古い順
    """
    # ── ソース1: e-Stat データベース検索 (正しいstatsDataIdを動的特定)
    # 住宅着工統計 (toukei=00600120) の月次データ一覧を取得
    try:
        search_url = "https://api.e-stat.go.jp/rest/3.0/app/json/getStatsList"
        search_params = {
            "appId":       ESTAT_API_KEY,
            "statsCode":   "00600120",   # 建築着工統計調査
            "explanationGetFlg": "N",
            "limit":       100,
        }
        r = requests.get(search_url, params=search_params, timeout=20)
        if r.status_code == 200:
            tables = (r.json().get("GET_STATS_LIST", {})
                              .get("DATALIST_INF", {})
                              .get("TABLE_INF", []))
            if isinstance(tables, dict):
                tables = [tables]
            # 月次の時系列データを探す（CYCLE="月次"かつタイトルに着工戸数）
            candidate_ids = []
            for t in tables:
                stat_id = t.get("@id", "")
                title = t.get("TITLE", "")
                if isinstance(title, dict):
                    title = title.get("$", "")
                cycle = str(t.get("CYCLE", ""))
                survey = str(t.get("SURVEY_DATE", ""))
                # 月次で戸数データ
                if ("月" in cycle or len(survey) == 6) and stat_id:
                    candidate_ids.append(stat_id)
            safe_print(f"  [e-Stat] 候補ID数: {len(candidate_ids)}")
    except Exception as e:
        safe_print(f"  [WARN] e-Stat list: {e}")
        candidate_ids = []

    # 固定IDも追加（建築着工統計 住宅着工統計 月次 時系列）
    fixed_ids = [
        "0003103532",  # 新設住宅着工戸数 月次（利用関係別）
        "0003115785",
        "0003115786",
        "0003130749",
        "0003130750",
        "0003143880",
        "0003143881",
        "0003155440",
        "0003155441",
        "0003166875",
        "0003166876",
        "0003178205",
    ]

    all_ids = fixed_ids + [x for x in candidate_ids if x not in fixed_ids]

    for stats_id in all_ids:
        try:
            url = "https://api.e-stat.go.jp/rest/3.0/app/json/getStatsData"
            params = {
                "appId":       ESTAT_API_KEY,
                "statsDataId": stats_id,
                "metaGetFlg":  "Y",
                "cntGetFlg":   "N",
                "limit":       100000,
            }
            resp = requests.get(url, params=params, timeout=25)
            if resp.status_code != 200:
                continue
            data = resp.json()
            result_status = (data.get("GET_STATS_DATA", {})
                                 .get("RESULT", {}).get("STATUS", 1))
            if result_status != 0:
                continue

            values = (data.get("GET_STATS_DATA", {})
                         .get("STATISTICAL_DATA", {})
                         .get("DATA_INF", {})
                         .get("VALUE", []))
            if not values:
                continue

            # メタデータから「合計」カテゴリのコードを特定
            class_inf = (data.get("GET_STATS_DATA", {})
                            .get("STATISTICAL_DATA", {})
                            .get("CLASS_INF", {})
                            .get("CLASS_OBJ", []))
            if isinstance(class_inf, dict):
                class_inf = [class_inf]

            total_codes = set()
            for cls in class_inf:
                classes = cls.get("CLASS", [])
                if isinstance(classes, dict):
                    classes = [classes]
                for c in classes:
                    name = c.get("@name", "")
                    code = c.get("@code", "")
                    if name in ("合計", "総数", "計", "total", "Total"):
                        total_codes.add(code)

            results = []
            seen_dates = {}
            for item in values:
                raw_time = item.get("@time", "")
                val_str = item.get("$", "")
                if not val_str or val_str in ("-", "***", "...", "－", "x"):
                    continue
                try:
                    year = int(raw_time[:4])
                    month = int(raw_time[4:6]) if len(raw_time) >= 6 else 0
                    if year < start_year or month < 1 or month > 12:
                        continue
                    val = float(val_str.replace(",", ""))
                    date_key = f"{year}-{month:02d}-01"
                    # 合計コードのデータを優先、または全体を取得して後でフィルタ
                    cat_code = item.get("@cat01", item.get("@cat", ""))
                    is_total = (cat_code in total_codes or
                                not total_codes or
                                cat_code in ("", "000", "00"))
                    if is_total:
                        # 大きい値（合計）を選ぶ
                        if date_key not in seen_dates or seen_dates[date_key] < val:
                            seen_dates[date_key] = val
                except Exception:
                    continue

            if seen_dates:
                results = sorted(seen_dates.items())
                # 合理的な範囲チェック（月次着工戸数は5000〜200000の範囲）
                results = [(d, v) for d, v in results if 5000 <= v <= 200000]
                if len(results) >= 12:  # 最低1年分あれば有効
                    safe_print(f"  [e-Stat] ID:{stats_id} {len(results)}件（最新: {results[-1][0]}）")
                    return results
        except Exception as e:
            pass

    # ── ソース2: OECD SDMX API（絶対値）
    for measure in ["PRMNTO01.JPN.ST.M", "PRMNTO01.JPN.STSA.M", "WSCNDW01.JPN.ST.M"]:
        try:
            url = f"https://sdmx.oecd.org/public/rest/data/OECD.SDD.STES,DSD_STES@DF_MEI_BTS/{measure}"
            params = {"startPeriod": f"{start_year}-01", "format": "csvfilewithlabels"}
            resp = requests.get(url, params=params, timeout=25,
                                headers={"Accept": "text/csv"})
            if resp.status_code != 200 or not resp.text.strip():
                continue
            lines = resp.text.strip().split("\n")
            header = [h.strip().strip('"') for h in lines[0].split(",")]
            try:
                ti = header.index("TIME_PERIOD")
                vi = header.index("OBS_VALUE")
            except ValueError:
                ti, vi = 0, 1
            tmp = []
            for line in lines[1:]:
                parts = [p.strip().strip('"') for p in line.split(",")]
                if len(parts) <= max(ti, vi):
                    continue
                try:
                    period = parts[ti]
                    val = float(parts[vi])
                    year = int(period[:4])
                    if year < start_year:
                        continue
                    tmp.append((period[:7] + "-01", val))
                except Exception:
                    continue
            if tmp:
                tmp.sort(key=lambda x: x[0])
                safe_print(f"  [OECD] {measure}: {len(tmp)}件（最新: {tmp[-1][0]}）")
                return tmp
        except Exception as e:
            safe_print(f"  [WARN] OECD {measure}: {e}")

    safe_print("  [ERROR] 日本住宅着工: 全ソース取得失敗")
    return []

def fetch_eurostat_housing(start_year=2015):
    """
    EU住宅着工（建設許可件数, 月次・絶対値）を取得。
    Eurostat: sts_cobp_m (Building Permits, residential buildings)
    フォールバック: FRED ドイツ+フランス住宅着工合算
    """
    # ── ソース1: Eurostat sts_cobp_m
    base_url = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/sts_cobp_m"
    param_sets = [
        # 住宅建物許可件数 EU27 絶対値（NR=件数）
        {"format": "JSON", "lang": "EN", "geo": "EU27_2020",
         "indic_bt": "BPBU", "s_adj": "NSA", "unit": "NR",
         "sinceTimePeriod": f"{start_year}-01"},
        # unit=THS（千件）
        {"format": "JSON", "lang": "EN", "geo": "EU27_2020",
         "indic_bt": "BPBU", "s_adj": "NSA", "unit": "THS",
         "sinceTimePeriod": f"{start_year}-01"},
        # EU28
        {"format": "JSON", "lang": "EN", "geo": "EU28",
         "indic_bt": "BPBU", "s_adj": "NSA", "unit": "NR",
         "sinceTimePeriod": f"{start_year}-01"},
        # 季節調整あり
        {"format": "JSON", "lang": "EN", "geo": "EU27_2020",
         "indic_bt": "BPBU", "s_adj": "SCA", "unit": "NR",
         "sinceTimePeriod": f"{start_year}-01"},
        # 生産指数（フォールバック用）
        {"format": "JSON", "lang": "EN", "geo": "EU27_2020",
         "nace_r2": "F", "s_adj": "NSA", "unit": "I21",
         "sinceTimePeriod": f"{start_year}-01",
         "_dataset": "sts_copr_m"},
        {"format": "JSON", "lang": "EN", "geo": "EU27_2020",
         "nace_r2": "F", "s_adj": "NSA", "unit": "I15",
         "sinceTimePeriod": f"{start_year}-01",
         "_dataset": "sts_copr_m"},
    ]

    for i, params in enumerate(param_sets):
        dataset = params.pop("_dataset", "sts_cobp_m")
        url = f"https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/{dataset}"
        try:
            resp = requests.get(url, params=params, timeout=20)
            if resp.status_code != 200:
                safe_print(f"  [WARN] Eurostat セット{i+1}: {resp.status_code}")
                continue
            data = resp.json()
            vals = data.get("value", {})
            dims = data.get("dimension", {})
            time_idx = (dims.get("time", {})
                           .get("category", {})
                           .get("index", {}))
            results = []
            for t, idx in sorted(time_idx.items(), key=lambda x: x[0]):
                v = vals.get(str(idx))
                if v is not None:
                    date_str = t + "-01" if len(t) == 7 else t
                    results.append((date_str, float(v)))
            if results:
                safe_print(f"  [Eurostat] EU住宅(セット{i+1}/{dataset}): {len(results)}件（最新: {results[-1][0]}）")
                return results
        except Exception as e:
            safe_print(f"  [WARN] Eurostat セット{i+1}: {e}")

    # ── ソース2: FRED ドイツ+フランス+スペイン住宅着工合算
    safe_print("  [WARN] Eurostat直接取得失敗、FREDフォールバック...")
    combined = {}
    for sid, label in [
        ("PRMNTO01DEM661N", "ドイツ"),
        ("PRMNTO01FRM661N", "フランス"),
        ("PRMNTO01ESM661N", "スペイン"),
    ]:
        try:
            url = "https://api.stlouisfed.org/fred/series/observations"
            params = {
                "series_id": sid,
                "api_key": FRED_API_KEY,
                "file_type": "json",
                "sort_order": "asc",
                "observation_start": f"{start_year}-01-01",
            }
            resp = requests.get(url, params=params, timeout=15)
            if resp.status_code == 200:
                for o in resp.json().get("observations", []):
                    val = o.get("value", ".")
                    if val != ".":
                        d = o["date"]
                        combined[d] = combined.get(d, 0) + float(val)
                safe_print(f"  [FRED] {label}住宅着工取得")
        except Exception:
            pass

    if combined:
        results = sorted(combined.items())
        safe_print(f"  [FRED] EU住宅着工(DE+FR+ES合算): {len(results)}件（最新: {results[-1][0]}）")
        return results

    safe_print("  [ERROR] EU住宅着工: 全ソース失敗")
    return []


def fetch_eurostat_construction(start_year=2015):
    """後方互換のためのラッパー（使用箇所を移行済み）"""
    return fetch_eurostat_housing(start_year)

# =============================================================================
# 各指標を収集してSheetsに書き込む
# =============================================================================
def write_bulk(sheet, rows):
    """
    シートを全クリアして最新データを全件書き込む。
    重複チェックではなく毎回上書きすることで常に最新データを保証する。
    """
    import gspread.exceptions

    if not rows:
        safe_print(f"  [SHEETS] データなし、スキップ")
        return

    # ヘッダーを保持
    try:
        header = sheet.row_values(1)
    except Exception:
        header = []

    # 日付順にソート
    sorted_rows = sorted(rows, key=lambda x: x[0])
    write_data = [[date_str, val] for date_str, val in sorted_rows]

    # クリアして全件書き込み（リトライ付き）
    for attempt in range(3):
        try:
            sheet.clear()
            if header:
                sheet.append_rows([header] + write_data, value_input_option="USER_ENTERED")
            else:
                sheet.append_rows(write_data, value_input_option="USER_ENTERED")
            safe_print(f"  [SHEETS] {len(write_data)}件書き込み完了（最新: {sorted_rows[-1][0]}）")
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

    # ── 7. 日本住宅着工（月次・絶対値）────────────────────────────────
    # OECD API から月次着工件数（絶対値）を直接取得
    sheet = get_or_create_sheet(spreadsheet, "日本住宅着工", ["日付", "日本住宅着工 (戸)"])
    rows = fetch_japan_housing()
    write_bulk(sheet, rows)
    time.sleep(2)

    # ── 8. ニッケル価格（USD/mt）─────────────────────────────────────────
    # FRED: PNICKUSDM = London Metal Exchange ニッケル月次価格
    sheet = get_or_create_sheet(spreadsheet, "ニッケル価格", ["日付", "ニッケル価格 (USD/mt)"])
    rows = fetch_fred("PNICKUSDM", "ニッケル価格")
    write_bulk(sheet, rows)
    time.sleep(2)

    # ── 9. 欧州住宅着工（月次・絶対値）────────────────────────────────────
    sheet = get_or_create_sheet(spreadsheet, "欧州建設生産指数", ["日付", "EU住宅着工 (件)"])
    rows = fetch_eurostat_housing()
    write_bulk(sheet, rows)
    time.sleep(2)

    safe_print("\n=== Collection complete ===")


# =============================================================================
# グラフ自動生成
# =============================================================================
def delete_chart_sheets(spreadsheet, chart_titles):
    """
    グラフ専用シート（自動生成されたもの）を全て削除する。
    グラフシートはsheetType="OBJECT"で判別できる。
    """
    try:
        meta = spreadsheet.fetch_sheet_metadata()
        sheets_meta = meta.get("sheets", [])
        delete_requests = []
        for s in sheets_meta:
            props = s.get("properties", {})
            # グラフ専用シートはsheetType == "OBJECT"
            if props.get("sheetType") == "OBJECT":
                delete_requests.append({
                    "deleteSheet": {"sheetId": props["sheetId"]}
                })
        if delete_requests:
            spreadsheet.batch_update({"requests": delete_requests})
            safe_print(f"  [CHART] 既存グラフシート {len(delete_requests)}枚を削除")
        else:
            safe_print(f"  [CHART] 削除対象のグラフシートなし")
    except Exception as e:
        safe_print(f"  [WARN] グラフシート削除失敗: {e}")


def create_chart(spreadsheet, sheet_name, title, x_col=0, y_col=1):
    """
    指定シートに折れ線グラフを作成する。
    グラフ専用シートは事前にdelete_chart_sheetsで削除済み前提。
    Google Sheets API の batchUpdate を使用。
    """
    try:
        sheet = spreadsheet.worksheet(sheet_name)
    except Exception:
        safe_print(f"  [CHART] シートが見つかりません: {sheet_name}")
        return

    sheet_id = sheet.id

    # データ行数を取得
    try:
        nrows = len(sheet.col_values(1))  # A列のデータ件数
        if nrows < 2:
            safe_print(f"  [CHART] データ不足でグラフ作成スキップ: {sheet_name}")
            return
    except Exception:
        nrows = 200

    # グラフ追加リクエスト
    request = {
        "addChart": {
            "chart": {
                "spec": {
                    "title": title,
                    "basicChart": {
                        "chartType": "LINE",
                        "legendPosition": "BOTTOM_LEGEND",
                        "axis": [
                            {"position": "BOTTOM_AXIS", "title": "日付"},
                            {"position": "LEFT_AXIS",  "title": title},
                        ],
                        "domains": [{
                            "domain": {
                                "sourceRange": {
                                    "sources": [{
                                        "sheetId": sheet_id,
                                        "startRowIndex": 1,
                                        "endRowIndex": nrows,
                                        "startColumnIndex": x_col,
                                        "endColumnIndex": x_col + 1,
                                    }]
                                }
                            }
                        }],
                        "series": [{
                            "series": {
                                "sourceRange": {
                                    "sources": [{
                                        "sheetId": sheet_id,
                                        "startRowIndex": 1,
                                        "endRowIndex": nrows,
                                        "startColumnIndex": y_col,
                                        "endColumnIndex": y_col + 1,
                                    }]
                                }
                            },
                            "targetAxis": "LEFT_AXIS",
                        }],
                        "headerCount": 0,
                    }
                },
                "position": {
                    "newSheet": True  # グラフ専用シートに配置
                }
            }
        }
    }

    try:
        spreadsheet.batch_update({"requests": [request]})
        safe_print(f"  [CHART] グラフ作成完了: {title}")
    except Exception as e:
        safe_print(f"  [CHART] グラフ作成失敗 {sheet_name}: {e}")


def create_all_charts(spreadsheet):
    """全指標のグラフを一括作成する。実行前に古いグラフシートを全削除。"""
    safe_print("\n=== Creating charts ===\n")
    # 既存グラフシートを全削除してから再作成（重複防止）
    chart_titles = [
        "金価格 (USD/oz)", "銅価格 (USD/lb)", "WTI原油価格 (USD/bbl)",
        "石炭価格 (USD/ton)", "鉄鉱石価格 (USD/dmtu)", "ニッケル価格 (USD/mt)",
        "北米住宅着工 (千件)", "日本住宅着工 (戸)", "EU住宅着工 (件)",
    ]
    delete_chart_sheets(spreadsheet, chart_titles)
    time.sleep(3)  # 削除後に少し待機
    charts = [
        ("金価格",       "金価格 (USD/oz)"),
        ("銅価格",       "銅価格 (USD/lb)"),
        ("原油価格WTI",  "WTI原油価格 (USD/bbl)"),
        ("石炭価格",     "石炭価格 (USD/ton)"),
        ("鉄鉱石価格",   "鉄鉱石価格 (USD/dmtu)"),
        ("ニッケル価格", "ニッケル価格 (USD/mt)"),
        ("北米住宅着工", "北米住宅着工 (千件)"),
        ("日本住宅着工", "日本住宅着工 (戸)"),
        ("欧州建設生産指数", "EU住宅着工 (件)"),
    ]
    for sheet_name, title in charts:
        create_chart(spreadsheet, sheet_name, title)
        time.sleep(2)  # APIレート制限対策
    safe_print("\n=== Charts complete ===")

# =============================================================================
# メイン
# =============================================================================
def main():
    check_env()
    spreadsheet = connect_sheets()
    collect_and_write(spreadsheet)
    create_all_charts(spreadsheet)

if __name__ == "__main__":
    main()
