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
def _yfinance_gold_monthly(start_year=2015):
    """
    Yahoo Finance (GC=F 金先物) から日次データを取得し月次平均に変換する。
    ※ yfinance は非公式APIのため、インストール済みの場合のみ動作。
    戻り値: {yyyymm: [prices...]} の辞書、または空 {}
    """
    try:
        import yfinance as yf
        from datetime import date
        start = f"{start_year}-01-01"
        end   = date.today().strftime("%Y-%m-%d")
        # auto_adjust=False で MultiIndex列を回避
        hist = yf.download("GC=F", start=start, end=end,
                           interval="1d", auto_adjust=True,
                           progress=False)
        if hist is None or hist.empty:
            return {}
        monthly = {}
        # Close列は単純列 or MultiIndex両対応
        if hasattr(hist.columns, "levels"):
            # MultiIndex: ("Close", "GC=F") のような場合
            close_col = [c for c in hist.columns if c[0] == "Close"]
            if close_col:
                close_series = hist[close_col[0]]
            else:
                close_series = hist.iloc[:, 3]
        else:
            close_series = hist["Close"] if "Close" in hist.columns else hist.iloc[:, 3]

        for ts, val in close_series.items():
            try:
                price = float(val)
                if price <= 0 or price != price:  # NaN check
                    continue
                ym = ts.strftime("%Y%m")
                monthly.setdefault(ym, []).append(price)
            except Exception:
                continue
        return monthly
    except ImportError:
        safe_print("  [WARN] yfinance未インストール")
        return {}
    except Exception as e:
        safe_print(f"  [WARN] yfinance GC=F: {e}")
        return {}


def _stooq_gold_monthly(start_year=2015):
    """
    Stooq.com から金価格（GC.F）の月次または日次CSVを取得し月次データを返す。
    認証不要・HTTPS・requests のみで動作。
    戻り値: {yyyymm: price} の辞書、または空 {}
    """
    headers = {
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/122.0.0.0 Safari/537.36"),
        "Accept": "text/html,application/xhtml+xml,*/*",
        "Accept-Language": "en-US,en;q=0.9",
    }

    def _parse_stooq_csv(text, monthly_bar=True):
        """CSV本文をパースして {yyyymm: close} を返す"""
        lines = text.strip().split("\n")
        if len(lines) < 2:
            return {}
        # ヘッダー確認
        header = lines[0].strip().lower()
        if "date" not in header:
            safe_print(f"  [WARN] Stooq: 予期しないヘッダー: {lines[0][:80]}")
            return {}
        # Close列インデックスを動的に取得
        cols = [c.strip().lower() for c in lines[0].split(",")]
        try:
            date_i  = cols.index("date")
            close_i = cols.index("close")
        except ValueError:
            date_i, close_i = 0, 4
        monthly = {}
        for line in lines[1:]:
            parts = line.strip().split(",")
            if len(parts) <= max(date_i, close_i):
                continue
            try:
                date_str = parts[date_i].strip()
                close    = float(parts[close_i].strip())
                if close <= 0 or date_str[:4].isdigit() is False:
                    continue
                year = int(date_str[:4])
                if year < start_year:
                    continue
                ym = date_str[:7].replace("-", "")  # "YYYYMM"
                if monthly_bar:
                    monthly[ym] = close
                else:
                    # 日次の場合は月内の最後の値（後で上書き = 末日値）
                    monthly[ym] = close
            except Exception:
                continue
        return monthly

    # ── 試行1: 月次足（i=m）
    try:
        url = "https://stooq.com/q/d/l/?s=gc.f&i=m"
        resp = requests.get(url, headers=headers, timeout=20)
        safe_print(f"  [GOLD] Stooq月次: HTTP {resp.status_code}, "
                   f"len={len(resp.text)}, "
                   f"先頭80字: {resp.text[:80].strip()!r}")
        if resp.status_code == 200 and resp.text.strip():
            monthly = _parse_stooq_csv(resp.text, monthly_bar=True)
            if monthly:
                safe_print(f"  [GOLD] Stooq月次: {len(monthly)}件取得（最新: {max(monthly)}）")
                return monthly
    except Exception as e:
        safe_print(f"  [WARN] Stooq月次: {e}")

    # ── 試行2: 日次足（i=d）でフォールバック
    try:
        url = "https://stooq.com/q/d/l/?s=gc.f&i=d"
        resp = requests.get(url, headers=headers, timeout=20)
        safe_print(f"  [GOLD] Stooq日次: HTTP {resp.status_code}, "
                   f"len={len(resp.text)}")
        if resp.status_code == 200 and resp.text.strip():
            monthly = _parse_stooq_csv(resp.text, monthly_bar=False)
            if monthly:
                safe_print(f"  [GOLD] Stooq日次→月次変換: {len(monthly)}件（最新: {max(monthly)}）")
                return monthly
    except Exception as e:
        safe_print(f"  [WARN] Stooq日次: {e}")

    safe_print("  [WARN] Stooq: 全試行失敗")
    return {}


def _imf_pcps_gold_monthly(start_year=2015):
    """
    IMF PCPS SDMX API から金価格月次データを取得。
    エンドポイント: https://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData/PCPS/M.W00.PGOLD.USD
    通常は1〜2ヶ月ラグで更新。
    戻り値: [(date_str, value), ...] または []
    """
    try:
        # HTTPS に修正（port 80 HTTP は GitHub Actions でブロックされる）
        url = ("https://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData"
               f"/PCPS/M.W00.PGOLD.USD?startPeriod={start_year}")
        resp = requests.get(url, timeout=25,
                            headers={"Accept": "application/json"})
        if resp.status_code != 200:
            safe_print(f"  [WARN] IMF PCPS: HTTP {resp.status_code}")
            return []
        data = resp.json()
        # SDMX-JSON 構造: CompactData > DataSet > Series > Obs
        obs_list = (data.get("CompactData", {})
                        .get("DataSet", {})
                        .get("Series", {})
                        .get("Obs", []))
        if not obs_list:
            return []
        # 単一obs の場合はdictで返ることがある
        if isinstance(obs_list, dict):
            obs_list = [obs_list]
        tmp = []
        for obs in obs_list:
            period = obs.get("@TIME_PERIOD", "")   # "2025-01"
            val    = obs.get("@OBS_VALUE", "")
            if not period or not val:
                continue
            try:
                year = int(period[:4])
                if year < start_year:
                    continue
                date_str = period[:7] + "-01"
                tmp.append((date_str, float(val)))
            except Exception:
                continue
        tmp.sort(key=lambda x: x[0])
        return tmp
    except Exception as e:
        safe_print(f"  [WARN] IMF PCPS SDMX: {e}")
        return []


def fetch_gold_price(start_year=2015):
    """
    金価格（USD/troy oz）月次データを取得。

    ソース戦略（2026-03確認済み）:
    ──────────────────────────────────────────────────
    ※ FRED GOLDPMGBD228NLBM / GOLDAMGBD228NLBM
       → 2022年1月31日にFREDから完全削除済み（ICE著作権）。使用不可。
    ──────────────────────────────────────────────────
    優先順:
      1. IMF PCPS SDMX API (PGOLD, 月次, HTTPS)
         → 通常は1〜2ヶ月ラグで更新。2025年途中まで取得可能。

      2. Stooq.com GC.F 月次CSV（認証不要・HTTPS）
         → 最新月まで取得可能。IMFより新しい月を補完。

      3. yfinance GC=F 日次 → 月次平均（インストール済み時のみ）
         → Stooq失敗時のバックアップ。

      4. ECB EXR XAU/USD 月次（フォールバック）

      5. GitHub datasets CSV（最終フォールバック、2024年末程度まで）

    戻り値: [(date_str, value_float), ...] 古い順
    """
    combined = {}  # "YYYY-MM-01" -> float

    # ── ソース1: IMF PCPS SDMX（月次公式、HTTPS、1〜2ヶ月ラグ）─────
    imf_data = _imf_pcps_gold_monthly(start_year)
    if imf_data:
        for d, v in imf_data:
            combined[d] = v
        safe_print(f"  [GOLD] IMF PCPS: {len(imf_data)}件（最新: {imf_data[-1][0]}）")
    else:
        safe_print("  [WARN] IMF PCPS: データなし")

    # ── ソース2: Stooq GC.F 月次（認証不要・IMF未収録月を補完）──────
    stooq_monthly = _stooq_gold_monthly(start_year)
    if stooq_monthly:
        added = 0
        for ym, price in sorted(stooq_monthly.items()):
            date_str = f"{ym[:4]}-{ym[4:6]}-01"
            if date_str not in combined:
                combined[date_str] = price
                added += 1
        if added:
            safe_print(f"  [GOLD] Stooq: {added}ヶ月を補完")
        else:
            safe_print("  [GOLD] Stooq: IMFデータで全月カバー済み")
    else:
        safe_print("  [WARN] Stooq: 取得不可、yfinanceに移行")
        # ── ソース3: yfinance GC=F（Stooq失敗時のみ）────────────────
        yf_monthly = _yfinance_gold_monthly(start_year)
        if yf_monthly:
            added = 0
            for ym, prices in sorted(yf_monthly.items()):
                date_str = f"{ym[:4]}-{ym[4:6]}-01"
                if date_str not in combined:
                    combined[date_str] = sum(prices) / len(prices)
                    added += 1
            if added:
                safe_print(f"  [GOLD] yfinance GC=F: {added}ヶ月を補完")

    # ソース1〜3 でデータが取れた場合はここで返す
    if combined:
        result = sorted(combined.items(), key=lambda x: x[0])
        safe_print(f"  [GOLD] 最終: {len(result)}件（最新: {result[-1][0]}）")
        return result

    # ── ソース3: ECB EXR XAU/USD（フォールバック）─────────────────
    try:
        url = "https://data-api.ecb.europa.eu/service/data/EXR/M.XAU.USD.SP00.A"
        params = {
            "startPeriod": f"{start_year}-01",
            "detail":      "dataonly",
            "format":      "csvfilewithlabels",
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
                    val    = float(parts[vi])
                    year   = int(period[:4])
                    if year < start_year:
                        continue
                    tmp.append((period[:7] + "-01", val))
                except Exception:
                    continue
            if tmp:
                tmp.sort(key=lambda x: x[0])
                safe_print(f"  [GOLD] ECB XAU/USD: {len(tmp)}件（最新: {tmp[-1][0]}）")
                return tmp
    except Exception as e:
        safe_print(f"  [WARN] ECB gold: {e}")

    # ── ソース4: GitHub datasets CSV（最終フォールバック）────────────
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
            safe_print(f"  [GOLD] GitHub CSV: {len(tmp)}件（最新: {tmp[-1][0]}）")
            return tmp
    except Exception as e:
        safe_print(f"  [ERROR] 金価格 全ソース失敗: {e}")

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
def fetch_eurostat_housing(start_year=2015):
    """
    EU建築許可（月次インデックス）取得。
    正しいパラメータ (2026-02確認済み):
      indic_bt: BPRM_DW (dwellings数) または BPRM_SQM (床面積m²)
      unit: I21 (2021=100) または I15 (2015=100)
      s_adj: SCA (季節調整済) または NSA (未調整)
      geo: EU27_2020
    最新データ: 2026M01 まで存在
    """
    # ── ソース1: Eurostat sts_cobp_m (正しいパラメータ) ─────
    eurostat_attempts = [
        # 住宅数インデックス・季節調整済・2021=100
        {"indic_bt": "BPRM_DW", "unit": "I21", "s_adj": "SCA", "geo": "EU27_2020"},
        # 住宅数インデックス・未調整・2021=100
        {"indic_bt": "BPRM_DW", "unit": "I21", "s_adj": "NSA", "geo": "EU27_2020"},
        # 住宅数インデックス・季節調整済・2015=100
        {"indic_bt": "BPRM_DW", "unit": "I15", "s_adj": "SCA", "geo": "EU27_2020"},
        # 床面積インデックス・季節調整済・2021=100
        {"indic_bt": "BPRM_SQM", "unit": "I21", "s_adj": "SCA", "geo": "EU27_2020"},
        # EA20 (ユーロ圏20カ国) フォールバック
        {"indic_bt": "BPRM_DW", "unit": "I21", "s_adj": "SCA", "geo": "EA20"},
    ]
    for params_extra in eurostat_attempts:
        try:
            url = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/sts_cobp_m"
            params = {
                "format": "JSON",
                "lang": "EN",
                "sinceTimePeriod": f"{start_year}-01",
            }
            params.update(params_extra)
            resp = requests.get(url, params=params, timeout=25)
            if resp.status_code != 200:
                safe_print(f"  [WARN] Eurostat sts_cobp_m ({params_extra}): HTTP {resp.status_code}")
                continue
            data = resp.json()
            vals = data.get("value", {})
            dims = data.get("dimension", {})
            time_idx = dims.get("time", {}).get("category", {}).get("index", {})
            if not time_idx:
                continue
            results = []
            for t, idx in sorted(time_idx.items()):
                v = vals.get(str(idx))
                if v is not None:
                    date_str = t + "-01" if len(t) == 7 else t
                    results.append((date_str, float(v)))
            if results:
                safe_print(f"  [Eurostat] EU住宅許可({params_extra['indic_bt']},{params_extra['unit']},"
                           f"{params_extra['s_adj']}): {len(results)}件（最新: {results[-1][0]}）")
                return results
        except Exception as e:
            safe_print(f"  [WARN] Eurostat: {e}")

    # ── ソース2: FRED ドイツ建築許可 DEUPERMITMISMEI ─────────
    safe_print("  [WARN] Eurostat失敗 → FREDフォールバック...")
    for sid in ["DEUPERMITMISMEI", "FRAPERMITMISMEI", "ESPERMITMISMEI"]:
        try:
            url = "https://api.stlouisfed.org/fred/series/observations"
            params = {
                "series_id":         sid,
                "api_key":           FRED_API_KEY,
                "file_type":         "json",
                "sort_order":        "asc",
                "observation_start": f"{start_year}-01-01",
            }
            resp = requests.get(url, params=params, timeout=15)
            if resp.status_code != 200:
                continue
            data = resp.json()
            if "error_message" in data:
                continue
            tmp = [(o["date"], float(o["value"])) for o in data.get("observations", [])
                   if o.get("value", ".") not in (".", "", "NA")]
            if tmp:
                safe_print(f"  [FRED] {sid}: {len(tmp)}件（最新: {tmp[-1][0]}）")
                return tmp
        except Exception:
            pass

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
        "北米住宅着工 (千件)", "EU住宅着工 (件)",
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
