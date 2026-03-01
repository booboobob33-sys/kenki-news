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
      1. FRED API (FRED series: GOLDAMGBD228NLBM の後継として World Bank PGOLD)
      2. GitHub datasets CSV (2025-09まで)
    戻り値: [(date_str, value_float), ...] 古い順
    """
    results = []

    # ── ソース1: FREDのWorld Bank金価格 (PWLDGOLD→GOLDAMGBD228NLBM後継)
    # FREDで月次金価格として確実に存在するもの: GOLDAMGBD228NLBM廃止後は
    # "Gold Fixing Price 10:30 A.M. (London time) in London Bullion Market"
    # series_id: GOLDAMGBD228NLBM → 廃止
    # 代替: ICE Benchmark Administration 提供の "GOLDAMGBD228NLBM" 後継なし
    # → World Bank Commodity Price: PGOLD (USD per troy oz, monthly)
    try:
        url = "https://api.stlouisfed.org/fred/series/observations"
        params = {
            "series_id":         "GOLDAMGBD228NLBM",
            "api_key":           FRED_API_KEY,
            "file_type":         "json",
            "sort_order":        "asc",
            "observation_start": f"{start_year}-01-01",
        }
        resp = requests.get(url, params=params, timeout=15)
        if resp.status_code == 200:
            obs = resp.json().get("observations", [])
            for o in obs:
                val = o.get("value", ".")
                if val == ".":
                    continue
                results.append((o["date"], float(val)))
    except Exception:
        pass

    if results:
        results.sort(key=lambda x: x[0])
        safe_print(f"  [GOLD] FRED GOLDAMGBD228NLBM: {len(results)}件取得")
        return results

    # ── ソース2: FRED World Bank Gold Price (PWLDGOLD)
    try:
        for sid in ["PWLDGOLD", "GOLDPMGBD228NLBM"]:
            url = "https://api.stlouisfed.org/fred/series/observations"
            params = {
                "series_id":         sid,
                "api_key":           FRED_API_KEY,
                "file_type":         "json",
                "sort_order":        "asc",
                "observation_start": f"{start_year}-01-01",
            }
            resp = requests.get(url, params=params, timeout=15)
            if resp.status_code == 200:
                obs = resp.json().get("observations", [])
                tmp = []
                for o in obs:
                    val = o.get("value", ".")
                    if val == ".":
                        continue
                    tmp.append((o["date"], float(val)))
                if tmp:
                    tmp.sort(key=lambda x: x[0])
                    safe_print(f"  [GOLD] FRED {sid}: {len(tmp)}件取得")
                    return tmp
    except Exception:
        pass

    # ── ソース3: ECB API 金/USD価格 (月次、最新まで)
    try:
        url = "https://data-api.ecb.europa.eu/service/data/EXR/M.XAU.USD.SP00.A"
        params = {
            "startPeriod": f"{start_year}-01",
            "format": "csvfilewithlabels",
        }
        resp = requests.get(url, params=params, timeout=20,
                            headers={"Accept": "text/csv"})
        if resp.status_code == 200 and resp.text.strip():
            tmp = []
            lines = resp.text.strip().split("\n")
            # ヘッダー行を探してTIME_PERIOD/OBS_VALUEの列インデックスを特定
            header = [h.strip().strip('"') for h in lines[0].split(",")]
            try:
                ti = header.index("TIME_PERIOD")
                vi = header.index("OBS_VALUE")
            except ValueError:
                ti, vi = 0, 1
            for line in lines[1:]:
                parts = [p.strip().strip('"') for p in line.split(",")]
                if len(parts) <= max(ti, vi):
                    continue
                try:
                    period = parts[ti]  # "2020-01"
                    val = float(parts[vi])
                    year = int(period[:4])
                    if year < start_year:
                        continue
                    # Gold/USD from ECB is price of 1 troy oz in USD
                    # ECB series XAU/USD = USD per troy oz (inverted: need 1/val * 1000?)
                    # 実際にはXAU=1 oz, EXR gives USD per 1 XAU → そのまま使える
                    date_str = period + "-01"
                    tmp.append((date_str, val))
                except Exception:
                    continue
            if tmp:
                tmp.sort(key=lambda x: x[0])
                safe_print(f"  [GOLD] ECB XAU/USD: {len(tmp)}件取得")
                return tmp
    except Exception as e:
        safe_print(f"  [WARN] ECB gold: {e}")

    # ── ソース4: GitHub datasets CSV (フォールバック、〜2025-09)
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
            safe_print(f"  [GOLD] GitHub CSV: {len(tmp)}件取得")
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
def fetch_eurostat_construction(start_year=2015):
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
            break
        except gspread.exceptions.APIError as e:
            if "429" in str(e):
                wait = 60 * (attempt + 1)
                safe_print(f"  [WARN] 429 Rate limit. {wait}秒待機してリトライ...")
                time.sleep(wait)
            else:
                safe_print(f"  [ERROR] Sheets書き込みエラー: {e}")
                return
    else:
        safe_print(f"  [ERROR] 3回リトライ後も失敗。スキップします。")
        return

    # 書き込み後にシート全体を日付順（昇順）に並び替え
    try:
        all_values = sheet.get_all_values()
        if len(all_values) < 3:
            return
        header = all_values[0]
        data_rows = all_values[1:]
        data_rows.sort(key=lambda r: r[0])
        sheet.clear()
        sheet.append_rows([header] + data_rows, value_input_option="USER_ENTERED")
        safe_print(f"  [SHEETS] 日付順ソート完了")
    except Exception as e:
        safe_print(f"  [WARN] ソート処理失敗（データは書き込み済み）: {e}")


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
        "石炭価格 (USD/ton)", "鉄鉱石価格 (USD/dmtu)", "北米住宅着工 (千件)",
        "日本住宅着工 前年比(%)", "EU建設生産指数 (2021=100)",
    ]
    delete_chart_sheets(spreadsheet, chart_titles)
    time.sleep(3)  # 削除後に少し待機
    charts = [
        ("金価格",       "金価格 (USD/oz)"),
        ("銅価格",       "銅価格 (USD/lb)"),
        ("原油価格WTI",  "WTI原油価格 (USD/bbl)"),
        ("石炭価格",     "石炭価格 (USD/ton)"),
        ("鉄鉱石価格",   "鉄鉱石価格 (USD/dmtu)"),
        ("北米住宅着工", "北米住宅着工 (千件)"),
        ("日本住宅着工", "日本住宅着工 前年比(%)"),
        ("欧州建設生産指数", "EU建設生産指数 (2021=100)"),
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
