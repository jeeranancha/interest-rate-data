import streamlit as st
import pandas as pd
import requests
from datetime import timedelta
import datetime
import time

# --- CONFIGURATION ---
st.set_page_config(page_title="Market-Interest rate extractor", page_icon="🏦", layout="wide")

st.title("🏦 Market-Interest rate extractor")
st.markdown("Automated synchronization via direct API integration with BOT and FRED.")

# --- SIDEBAR INPUTS ---
st.sidebar.header("API Credentials")
# Note: For BOT, the Client ID is usually separate from the Token/Secret.
# I have labeled this clearly so you know what to input.
bot_client_id = st.sidebar.text_input("BOT Client ID", type="password", help="The X-IBM-Client-Id provided by BOT")
bot_token = st.sidebar.text_input("BOT Access Token", type="password", help="The Authorization Bearer token")
fred_api_key = st.sidebar.text_input("FRED API Token", type="password")

st.sidebar.header("Data Selection")
selected_date = st.sidebar.date_input("EFFECTIVE_DATE", datetime.date.today())
fetch_btn = st.sidebar.button("Fetch Data", type="primary")

# --- BOT FETCH FUNCTION ---
def fetch_bot_data(client_id, token, path, target_date):
    base_url = "https://gateway.api.bot.or.th"
    auth_header = token if token.startswith("Bearer ") else f"Bearer {token}"
    
    headers = {
        "X-IBM-Client-Id": client_id,
        "Authorization": auth_header,
        "accept": "application/json"
    }

    # Iterate backwards up to 14 days to find the first valid data point (handles holidays)
    for i in range(14):
        check_date = target_date - timedelta(days=i)
        check_date_str = check_date.strftime("%Y-%m-%d")
        url = f"{base_url}{path}?start_period={check_date_str}&end_period={check_date_str}"

        try:
            resp = requests.get(url, headers=headers, timeout=15)
            
            # Critical: Stop immediately if credentials are wrong
            if resp.status_code in [401, 403]:
                raise Exception("Authentication Failed: Check your BOT Client ID and Token.")
            
            resp.raise_for_status()
            res_json = resp.json()
            result_block = res_json.get("result", {})
            data_field = result_block.get("data")
            
            # If data is empty for this specific date, continue the loop to the previous day
            if not data_field:
                continue
                
            rate = None
            
            # Case 1: List-based data (THOR/Interbank)
            if isinstance(data_field, list) and len(data_field) > 0:
                if "INTRBNK_TXN_RATE" in path:
                    # Filter for Over-Night rate
                    for rec in data_field:
                        if rec.get("term") == "O/N":
                            rate = rec.get("rate")
                            break
                else:
                    rate = data_field[0].get("rate")
            
            # Case 2: Dictionary-based data (Policy Rate)
            elif isinstance(data_field, dict):
                rate = data_field.get("value") or data_field.get("rate")
            
            if rate is not None:
                return (check_date_str, float(rate))

        except requests.exceptions.RequestException as e:
            # Raise a clean error without exposing the full URL/Headers
            raise Exception(f"Connection Error: {resp.status_code if 'resp' in locals() else 'Unknown'}")

    raise Exception("No data found in the last 14 days. (Likely a system or mapping error)")

# --- FRED FETCH FUNCTION ---
def fetch_fred_data(api_key, series_id, target_date):
    time.sleep(0.5) # Anti-throttle delay
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_end": target_date.strftime("%Y-%m-%d"),
        "sort_order": "desc",
        "limit": 5
    }
    try:
        response = requests.get(url, params=params, timeout=15)
        
        # Clean error handling to avoid leaking API Key in UI
        if response.status_code != 200:
            raise Exception(f"FRED Server returned status {response.status_code}")
            
        observations = response.json().get("observations", [])
        for obs in observations:
            val = obs.get("value")
            if val not in [".", None, ""]:
                return (obs.get("date"), float(val))
                
        raise Exception("No valid numerical data found for this series.")
    except Exception as e:
        # Re-raise as a generic error to keep the UI clean
        raise Exception(f"FRED API Error: {str(e)}")

# --- MAIN EXECUTION ---
if fetch_btn:
    if not bot_client_id or not bot_token or not fred_api_key:
        st.warning("⚠️ Please provide all API credentials in the sidebar.")
    else:
        with st.spinner("Extracting market rates..."):
            effective_date_str = selected_date.strftime("%Y-%m-%d")

            api_mappings = [
                ("THOR_OIS", "1D", "BOT", "/Stat-InterbankTransactionRate/v2/INTRBNK_TXN_RATE"),
                ("THB_DISCOUNTING", "1D", "BOT", "/PolicyRate/v3/policy_rate"),
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

            for curve, tenor, source, api_uid in api_mappings:
                try:
                    if source == "BOT":
                        val_date, rate_val = fetch_bot_data(bot_client_id, bot_token, api_uid, selected_date)
                    else:
                        val_date, rate_val = fetch_fred_data(fred_api_key, api_uid, selected_date)
                except Exception as e:
                    errors.append(f"❌ {curve} ({tenor}) from {source}: {str(e)}")
                    val_date, rate_val = "N/A", "N/A"

                results.append({
                    "CURVE_NAME": curve,
                    "TENOR": tenor,
                    "RATE_VALUE": rate_val,
                    "EFFECTIVE_DATE": effective_date_str,
                    "VALUE_DATE": val_date
                })

            df = pd.DataFrame(results)

            if errors:
                st.warning("⚠️ Some data points could not be retrieved.")
                with st.expander("View Error Details"):
                    for e in errors:
                        st.error(e)
            else:
                st.success("✅ Synchronization Complete")

            st.subheader("Data Preview")
            # Displaying dataframe with numeric formatting where possible
            st.dataframe(df, use_container_width=True)

            csv = df.to_csv(index=False)
            st.download_button(
                label="📥 Download CSV",
                data=csv,
                file_name=f"market_rates_{effective_date_str}.csv",
                mime="text/csv"
            )

else:
    st.info("👈 Enter your API credentials in the sidebar and click 'Fetch Data' to begin.")