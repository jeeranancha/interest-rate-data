import streamlit as st
import pandas as pd
import requests
from datetime import timedelta, datetime
import time
import base64
import json
import logging

# --- CONFIGURATION ---
MAX_LOOKBACK = 14
st.set_page_config(page_title="Market-Interest rate extractor", page_icon="🏦", layout="wide")

logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

st.title("🏦 Market-Interest rate extractor")
st.markdown("Automated synchronization via direct API integration with BOT and FRED.")

# --- SIDEBAR INPUTS ---
st.sidebar.header("API Credentials")
bot_token_input = st.sidebar.text_input("BOT API Token", type="password")
fred_api_key = st.sidebar.text_input("FRED API Token", type="password")

st.sidebar.header("Data Selection")
selected_date = st.sidebar.date_input("EFFECTIVE_DATE", datetime.today().date())
fetch_btn = st.sidebar.button("Fetch Data", type="primary")

# --- BOT FETCH FUNCTION (ENHANCED) ---
def fetch_bot_data(token_input, api_info, target_date, debug_capture=None):
    path = api_info["path"]
    api_type = api_info["type"]
    base_url = "https://gateway.api.bot.or.th"
    auth_header = token_input if token_input.startswith("Bearer ") else f"Bearer {token_input}"

    final_client_id = token_input
    try:
        decoded = json.loads(base64.b64decode(token_input + "==").decode('utf-8'))
        final_client_id = decoded.get('id', token_input)
    except Exception: pass

    headers = {"X-IBM-Client-Id": final_client_id, "Authorization": auth_header, "accept": "application/json"}
    
    last_raw_response = None
    last_url = None

    for i in range(MAX_LOOKBACK):
        check_date = target_date - timedelta(days=i)
        check_date_str = check_date.strftime("%Y-%m-%d")
        url = f"{base_url}{path}?start_period={check_date_str}&end_period={check_date_str}"
        last_url = url

        try:
            resp = requests.get(url, headers=headers, timeout=15)
            if debug_capture is not None:
                debug_capture["last_url"] = url
                debug_capture["status_code"] = resp.status_code
            
            if resp.status_code in [401, 403]: return Exception("Auth Failed (401/403)")
            
            resp.raise_for_status()
            res_json = resp.json()
            last_raw_response = res_json

            result_block = res_json.get("result", {})
            data_field = result_block.get("data")
            
            if not data_field: continue

            rate = None
            
            # --- 1. Interbank (THOR/Call) ---
            if api_type == "interbank":
                ON_VALUES = {"O/N", "ON", "Overnight", "o/n", "call", "1d"}
                records = data_field.get("data_detail", data_field) if isinstance(data_field, dict) else data_field
                if isinstance(records, list):
                    for rec in records:
                        term_val = str(next((rec.get(k) for k in ("term_type_name_eng", "tenor", "term") if rec.get(k) is not None), "")).strip()
                        if term_val.upper() in [v.upper() for v in ON_VALUES]:
                            rate = next((rec.get(k) for k in ("weighted_average_interest_rate", "rate", "value") if rec.get(k) not in (None, "", "N/A")), None)
                            if rate is not None: break

            # --- 2. Policy Rate (FIXED) ---
            elif api_type == "policy":
                # แก้ไข: เพิ่มการรองรับข้อมูลที่เป็น String (เช่น "1.00" จากในภาพ)
                if isinstance(data_field, (int, float)): 
                    rate = data_field
                elif isinstance(data_field, str):
                    try: rate = float(data_field)
                    except: rate = None
                elif isinstance(data_field, dict):
                    rate = next((data_field.get(k) for k in ("value", "rate", "policy_rate_percent") if data_field.get(k) is not None), None)

            # --- 3. Saving Rate ---
            elif api_type == "saving":
                records = data_field.get("data_detail", [data_field]) if isinstance(data_field, dict) else data_field
                if isinstance(records, list):
                    for rec in records:
                        if not isinstance(rec, dict): continue
                        desc = " ".join([str(rec.get(k, "")).lower() for k in ("account_type_name_eng", "description", "term")])
                        is_target = any(x in desc for x in ["saving", "call", "1d"])
                        val = next((rec.get(k) for k in ("avg_saving_rate_percent", "value", "rate") if rec.get(k) not in (None, "", "N/A")), None)
                        if is_target and val is not None:
                            rate = val
                            break

            if rate is not None:
                try: return (check_date_str, float(rate))
                except: continue

        except Exception as e:
            if debug_capture is not None: debug_capture["error_msg"] = str(e)
            continue

    if debug_capture is not None:
        debug_capture["raw"] = last_raw_response
    return Exception(f"No valid data in last {MAX_LOOKBACK} days")

# --- FRED FETCH FUNCTION ---
def fetch_fred_data(api_key, series_id, target_date):
    time.sleep(0.6)
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {"series_id": series_id, "api_key": api_key, "file_type": "json", "observation_end": target_date.strftime("%Y-%m-%d"), "sort_order": "desc", "limit": 10}
    try:
        response = requests.get(url, params=params, timeout=15)
        data = response.json()
        observations = data.get("observations", [])
        for obs in observations:
            val = obs.get("value")
            if val not in [".", None, ""]: return (obs.get("date"), float(val))
        return Exception("No data")
    except: return Exception("Connection error")

# --- MAIN EXECUTION ---
if fetch_btn:
    if not bot_token_input or not fred_api_key:
        st.warning("⚠️ Please provide all API tokens.")
    else:
        with st.spinner("Extracting market rates..."):
            request_date = selected_date
            api_mappings = [
                ("THOR_OIS", "1D", "BOT", {"path": "/Stat-InterbankTransactionRate/v2/INTRBNK_TXN_RATE", "type": "interbank"}),
                ("THB_DISCOUNTING", "1D", "BOT", {"path": "/PolicyRate/v3/policy_rate", "type": "policy"}),
                ("THB_SAVING", "1D", "BOT", {"path": "/Stat-AverageRetailInterestRate/v2/AVG_RETAIL_IR", "type": "saving"}),
                ("USD_SOFR", "1D", "FRED", "SOFR"),
                ("USD_DISCOUNTING", "1D", "FRED", "DFEDTARU"),
                ("USD_DISCOUNTING", "1M", "FRED", "DGS1MO"),
                ("USD_DISCOUNTING", "3M", "FRED", "DGS3MO"),
                ("USD_DISCOUNTING", "6M", "FRED", "DGS6MO"),
                ("USD_DISCOUNTING", "1Y", "FRED", "DGS1"),
                ("USD_DISCOUNTING", "2Y", "FRED", "DGS2"),
                ("USD_DISCOUNTING", "3Y", "FRED", "DGS3"),
                ("USD_DISCOUNTING", "5Y", "FRED", "DGS5"),
            ]

            results, errors = [], []
            for curve, tenor, source, api_info in api_mappings:
                dbg = {}
                res = fetch_bot_data(bot_token_input, api_info, request_date, debug_capture=dbg) if source == "BOT" else fetch_fred_data(fred_api_key, api_info, request_date)
                
                if isinstance(res, Exception):
                    errors.append((f"{curve} ({source})", str(res), dbg))
                    v_date, r_val = "N/A", "N/A"
                else: v_date, r_val = res

                stale = (request_date - datetime.strptime(v_date, "%Y-%m-%d").date()).days if v_date != "N/A" else "N/A"
                results.append({"CURVE_NAME": curve, "TENOR": tenor, "RATE_VALUE": f"{r_val:.6f}" if isinstance(r_val, (int, float)) else r_val, "EFFECTIVE_DATE": request_date.strftime("%Y-%m-%d"), "VALUE_DATE": v_date, "STALE_DAYS": stale})

            df = pd.DataFrame(results).sort_values(by=["CURVE_NAME", "TENOR"])
            if errors:
                with st.expander("🔍 DETAILED ERROR LOG (For Analysis)", expanded=True):
                    for l, m, d in errors:
                        st.error(f"❌ {l}: {m}")
                        st.write(f"**Last Attempted URL:** `{d.get('last_url')}`")
                        if d.get("status_code"): st.write(f"**HTTP Status:** `{d.get('status_code')}`")
                        if d.get("raw"): st.json(d["raw"])
                        st.divider()
            
            st.success(f"✅ Synchronized {len(df)} market rates")
            st.dataframe(df, use_container_width=True)
            st.download_button("📥 Download CSV", df.to_csv(index=False), f"mkt_ir_{request_date.strftime('%Y%m%d')}.csv", "text/csv")
else:
    st.info("👈 Enter tokens and click 'Fetch Data' to begin.")
