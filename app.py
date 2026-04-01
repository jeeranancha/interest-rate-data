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

# Setup logging
logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

st.title("🏦 Market-Interest rate extractor")
st.markdown("Automated synchronization via direct API integration with BOT and FRED.")

# --- SIDEBAR INPUTS ---
st.sidebar.header("API Credentials")
# Re-implemented as a single automated field as requested
bot_token_input = st.sidebar.text_input("BOT API Token", type="password", help="The long eyJ... token from BOT portal")
fred_api_key = st.sidebar.text_input("FRED API Token", type="password")

st.sidebar.header("Data Selection")
selected_date = st.sidebar.date_input("EFFECTIVE_DATE", datetime.today().date())
fetch_btn = st.sidebar.button("Fetch Data", type="primary")

# --- BOT FETCH FUNCTION ---
def fetch_bot_data(token_input, api_info, target_date):
    path = api_info["path"]
    api_type = api_info["type"]
    base_url = "https://gateway.api.bot.or.th"
    auth_header = token_input if token_input.startswith("Bearer ") else f"Bearer {token_input}"
    
    # AUTO-DECODE: Extract hidden Client ID from the Base64 token
    final_client_id = token_input
    try:
        decoded = json.loads(base64.b64decode(token_input + "==").decode('utf-8'))
        final_client_id = decoded.get('id', token_input)
    except Exception as e:
        logging.error(f"Token decode error: {str(e)}")
        pass

    headers = {
        "X-IBM-Client-Id": final_client_id,
        "Authorization": auth_header,
        "accept": "application/json"
    }

    # Iterate backwards using MAX_LOOKBACK
    for i in range(MAX_LOOKBACK):
        check_date = target_date - timedelta(days=i)
        check_date_str = check_date.strftime("%Y-%m-%d")
        url = f"{base_url}{path}?start_period={check_date_str}&end_period={check_date_str}"

        try:
            resp = requests.get(url, headers=headers, timeout=15)
            
            # Critical: Stop immediately if credentials are wrong
            if resp.status_code in [401, 403]:
                return Exception("Authentication Failed: Check your BOT API Token.")
            
            resp.raise_for_status()
            res_json = resp.json()
            result_block = res_json.get("result", {})
            data_field = result_block.get("data")
            
            if not data_field:
                continue
                
            rate = None
            logging.error(f"[DEBUG] {api_type} data_field sample: {str(data_field)[:500]}")

            # Case 1: Interbank / List-based (THOR_OIS)
            # BOT API may use 'term', 'tenor', or 'type' for the field name,
            # and 'O/N', 'ON', or 'Overnight' for the overnight value.
            if api_type == "interbank" and isinstance(data_field, list):
                ON_KEYS = ("term", "tenor", "type", "period")
                ON_VALUES = {"O/N", "ON", "Overnight", "overnight", "o/n"}
                RATE_KEYS = ("rate", "value", "avg_rate", "rate_value", "weighted_avg_rate")
                for rec in data_field:
                    term_val = next((rec.get(k) for k in ON_KEYS if rec.get(k) is not None), None)
                    if str(term_val).strip() in ON_VALUES:
                        rate = next((rec.get(k) for k in RATE_KEYS if rec.get(k) is not None), None)
                        if rate is not None:
                            break
                # Fallback: if no O/N label found, try first record
                if rate is None and data_field:
                    RATE_KEYS = ("rate", "value", "avg_rate", "rate_value", "weighted_avg_rate")
                    rate = next((data_field[0].get(k) for k in RATE_KEYS if data_field[0].get(k) is not None), None)

            # Case 2: Policy / Dict-based (THB_DISCOUNTING)
            elif api_type == "policy":
                POLICY_RATE_KEYS = ("value", "rate", "policy_rate_percent", "rate_value",
                                    "mid", "policy_rate", "interestRate", "interest_rate")
                if isinstance(data_field, dict):
                    rate = next((data_field.get(k) for k in POLICY_RATE_KEYS if data_field.get(k) is not None), None)
                elif isinstance(data_field, list) and len(data_field) > 0:
                    rate = next((data_field[0].get(k) for k in POLICY_RATE_KEYS if data_field[0].get(k) is not None), None)

            if rate is not None:
                # Type conversion safety
                try:
                    return (check_date_str, float(rate))
                except (ValueError, TypeError):
                    logging.error(f"Invalid rate format: {rate}")
                    continue

        except requests.exceptions.RequestException as e:
            logging.error(f"BOT Request Error: {str(e)}")
            continue

    return Exception(f"No valid data found in last {MAX_LOOKBACK} days")

# --- FRED FETCH FUNCTION ---
def fetch_fred_data(api_key, series_id, target_date):
    time.sleep(0.6) # Anti-throttle delay
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_end": target_date.strftime("%Y-%m-%d"),
        "sort_order": "desc",
        "limit": 10
    }
    try:
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        
        # Check for error inside JSON (even with 200 OK)
        if "error_code" in data:
            return Exception(f"FRED Server Error: {data.get('error_message')}")
            
        observations = data.get("observations", [])
        for obs in observations:
            val = obs.get("value")
            if val not in [".", None, ""]:
                # Convert date string to date object then back to string for consistency
                obs_date = obs.get("date")
                return (obs_date, float(val))
                
        return Exception("No valid numerical data found")
    except Exception as e:
        logging.error(f"FRED failure: {str(e)}")
        return Exception("FRED connection error")

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

            results = []
            errors = []

            for curve, tenor, source, api_info in api_mappings:
                if source == "BOT":
                    res = fetch_bot_data(bot_token_input, api_info, request_date)
                else:
                    res = fetch_fred_data(fred_api_key, api_info, request_date)

                if isinstance(res, Exception):
                    errors.append(f"{curve} ({source}): {str(res)}")
                    val_date, rate_val = "N/A", "N/A"
                else:
                    val_date, rate_val = res

                # Calculate Staleness
                stale_days = "N/A"
                if val_date != "N/A":
                    v_date = datetime.strptime(val_date, "%Y-%m-%d").date()
                    stale_days = (request_date - v_date).days

                results.append({
                    "CURVE_NAME": curve,
                    "TENOR": tenor,
                    "RATE_VALUE": rate_val,
                    "EFFECTIVE_DATE": request_date.strftime("%Y-%m-%d"),
                    "VALUE_DATE": val_date,
                    "STALE_DAYS": stale_days
                })

            # Create and Sort DataFrame
            df = pd.DataFrame(results)
            df = df.sort_values(by=["CURVE_NAME", "TENOR"])

            if errors:
                st.warning("⚠️ Completed with some errors")
                with st.expander("Details"):
                    for e in errors:
                        st.error(e)
            else:
                st.success(f"✅ Successfully synchronized {len(df)} market rates")

            st.subheader("Data Preview")
            st.dataframe(df, use_container_width=True)

            csv = df.to_csv(index=False)
            st.download_button(
                label="📥 Download CSV", 
                data=csv, 
                file_name=f"rates_{request_date.strftime('%Y-%m-%d')}.csv",
                mime="text/csv"
            )

else:
    st.info("👈 Enter tokens and click 'Fetch Data' to begin.")