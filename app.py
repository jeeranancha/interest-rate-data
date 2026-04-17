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

# --- BOT FETCH FUNCTION ---
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
            
            if resp.status_code == 404: continue # ลองวันที่ก่อนหน้าถ้า Path ผิดในบางวัน
            if resp.status_code in [401, 403]: return Exception("Auth Failed (401/403)")
            
            resp.raise_for_status()
            res_json = resp.json()
            last_raw_response = res_json

            result_block = res_json.get("result", {})
            data_field = result_block.get("data")
            if not data_field: continue

            rate = None
            
            # Case 1: Interbank
            if api_type == "interbank":
                records = data_field.get("data_detail", data_field) if isinstance(data_field, dict) else data_field
                if isinstance(records, list):
                    for rec in records:
                        term = str(rec.get("term_type_name_eng", "")).upper()
                        if term in ["O/N", "ON", "CALL"]:
                            rate = rec.get("weighted_average_interest_rate")
                            if rate: break

            # Case 2: Policy (FIXED)
            elif api_type == "policy":
                if isinstance(data_field, (int, float)): rate = data_field
                elif isinstance(data_field, str): 
                    try: rate = float(data_field)
                    except: rate = None

            # Case 3: Saving (FIXED PATH)
            elif api_type == "saving":
                records = data_field.get("data_detail", []) if isinstance(data_field, dict) else data_field
                if isinstance(records, list):
                    for rec in records:
                        # หาบรรทัดที่เป็น Saving Account
                        acc_type = str(rec.get("account_type_name_eng", "")).lower()
                        if "saving" in acc_type:
                            val = rec.get("avg_saving_rate_percent") or rec.get("value")
                            if val:
                                rate = val
                                break

            if rate is not None:
                return (check_date_str, float(rate))

        except: continue

    if debug_capture is not None: debug_capture["raw"] = last_raw_response
    return Exception(f"No valid data in last {MAX_LOOKBACK} days")

# --- FRED FETCH FUNCTION ---
def fetch_fred_data(api_key, series_id, target_date):
    time.sleep(0.6)
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {"series_id": series_id, "api_key": api_key, "file_type": "json", "observation_end": target_date.strftime("%Y-%m-%d"), "sort_order": "desc", "limit": 10}
    try:
        resp = requests.get(url, params=params, timeout=15)
        obs = resp.json().get("observations", [])
        for o in obs:
            if o.get("value") not in [".", None, ""]: return (o.get("date"), float(o.get("value")))
        return Exception("No data")
    except: return Exception("Error")

# --- MAIN ---
if fetch_btn:
    if not bot_token_input or not fred_api_key:
        st.warning("⚠️ Please provide all API tokens.")
    else:
        with st.spinner("Fetching..."):
            request_date = selected_date
            api_mappings = [
                ("THOR_OIS", "1D", "BOT", {"path": "/Stat-InterbankTransactionRate/v2/INTRBNK_TXN_RATE", "type": "interbank"}),
                ("THB_DISCOUNTING", "1D", "BOT", {"path": "/PolicyRate/v3/policy_rate", "type": "policy"}),
                # แก้ไข PATH ที่นี่
                ("THB_SAVING", "1D", "BOT", {"path": "/Stat-RetailInterestRate/v2/RETAIL_IR", "type": "saving"}),
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
                    errors.append((curve, str(res), dbg))
                    v_date, r_val = "N/A", "N/A"
                else: v_date, r_val = res

                stale = (request_date - datetime.strptime(v_date, "%Y-%m-%d").date()).days if v_date != "N/A" else "N/A"
                results.append({"CURVE_NAME": curve, "TENOR": tenor, "RATE_VALUE": f"{r_val:.6f}" if isinstance(r_val, (int, float)) else r_val, "EFFECTIVE_DATE": request_date.strftime("%Y-%m-%d"), "VALUE_DATE": v_date, "STALE_DAYS": stale})

            df = pd.DataFrame(results).sort_values(by=["CURVE_NAME", "TENOR"])
            if errors:
                with st.expander("🔍 Error Logs"):
                    for l, m, d in errors:
                        st.error(f"{l}: {m}")
                        if d.get("raw"): st.json(d["raw"])
            
            st.success(f"✅ Synced {len(df)} rates")
            st.dataframe(df, use_container_width=True)
            st.download_button("📥 Download", df.to_csv(index=False), "mkt_ir.csv")
