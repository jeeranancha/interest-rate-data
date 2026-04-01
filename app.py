import streamlit as st
import pandas as pd
import requests
from datetime import timedelta
import datetime

st.set_page_config(page_title="Treasury Data Portal", page_icon="🏦", layout="wide")

st.title("🏦 Treasury Data Portal")
st.markdown("Fetch interest rate data from Bank of Thailand (BOT) and St. Louis Fed (FRED) easily.")

# --- SIDEBAR INPUTS ---
st.sidebar.header("API Credentials")
bot_client_id = st.sidebar.text_input("BOT Client ID", type="password", help="Enter your Bank of Thailand X-IBM-Client-Id")
fred_api_key = st.sidebar.text_input("FRED API Key", type="password", help="Enter your St. Louis Fed API Key")

st.sidebar.header("Data Selection")
selected_date = st.sidebar.date_input("EFFECTIVE_DATE", datetime.date.today())
fetch_btn = st.sidebar.button("Fetch Data", type="primary")

# --- HELPER FUNCTIONS ---
def extract_latest_bot_rate(json_data, selected_date_str):
    """
    Safely finds the latest period/rate entry on or before the selected date
    for dynamic BOT API responses.
    """
    try:
        # Standard BOT structure
        records = json_data.get('result', {}).get('data', {}).get('data_detail', [])
        
        # If not standard, look for any lists recursively
        if not records:
            def find_list(d):
                if isinstance(d, dict):
                    for k, v in d.items():
                        res = find_list(v)
                        if res is not None: return res
                elif isinstance(d, list):
                    return d
                return None
            records = find_list(json_data) or []
            
        if not records:
            return None, "Structure error: No valid data array found in BOT response"
            
        valid_records = []
        for r in records:
            # Dynamic key guessing for period date
            r_date = r.get('period') or r.get('date') or r.get('data_date') or r.get('effective_date')
            # Dynamic key guessing for rate value
            r_rate = r.get('rate') or r.get('value') or r.get('interest_rate') or r.get('policy_rate') or r.get('thor')
            
            if r_date and r_rate is not None:
                # Keep records strictly less than or equal to requested date
                if r_date <= selected_date_str:
                    try:
                        valid_records.append((r_date, float(r_rate)))
                    except ValueError:
                        pass
        
        if not valid_records:
            return None, "No historical data found in the 7-day lookback window."
            
        # Sort by date descending and get the most recent one
        valid_records.sort(key=lambda x: x[0], reverse=True)
        return valid_records[0], None
    except Exception as e:
        return None, f"Parse error: {str(e)}"

def fetch_bot_data(client_id, path, target_date):
    """
    Fetches Bank of Thailand endpoint looking back up to 7 days
    """
    start_date = target_date - timedelta(days=7)
    base_url = "https://apigw1.bot.or.th/bot/public"
    url = f"{base_url}{path}"
    
    headers = {
        "X-IBM-Client-Id": client_id,
        "Accept": "application/json"
    }
    
    # Send both start and end dates formatted properly
    params = {
        "start_period": start_date.strftime("%Y-%m-%d"),
        "end_period": target_date.strftime("%Y-%m-%d")
    }
    
    try:
        # Avoid hanging requests
        response = requests.get(url, headers=headers, params=params, timeout=15)
        
        # Handle 401 Unauthorized gracefully
        if response.status_code == 401:
            return Exception("Unauthorized (Check BOT Client ID)")
        
        response.raise_for_status()
        
        # Extract the value from the custom JSON parser
        data, err = extract_latest_bot_rate(response.json(), target_date.strftime("%Y-%m-%d"))
        if err:
            return Exception(err)
            
        return data  # Returns (date_str, rate_float)
    except requests.exceptions.RequestException as e:
        return Exception(f"HTTP Error: {e}")

def fetch_fred_data(api_key, series_id, target_date):
    """
    Fetches the latest FRED data for a specific series on or before observation_end
    """
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_end": target_date.strftime("%Y-%m-%d"),
        "sort_order": "desc",
        "limit": 1
    }
    
    try:
        response = requests.get(url, params=params, timeout=15)
        
        if response.status_code == 400 and "api_key" in response.text.lower():
             return Exception("Unauthorized (Check FRED API Key)")
             
        response.raise_for_status()
        data = response.json()
        
        observations = data.get("observations", [])
        if observations:
            obs = observations[0]
            val = obs.get("value")
            val_date = obs.get("date")
            
            # FRED sometimes returns '.' during bank holidays within continuous observation sets
            # We will try expanding the limit sequentially if a '.' is hit
            if val == "." or val is None:
                params["limit"] = 5
                resp2 = requests.get(url, params=params, timeout=15)
                resp2.raise_for_status()
                obs2 = resp2.json().get("observations", [])
                
                for o in obs2:
                    if o.get("value") not in [".", None]:
                        return (o.get("date"), float(o.get("value")))
                return Exception("Missing internal data (Returns '.')")
                
            return (val_date, float(val))
            
        return Exception("No data observations found")
    except requests.exceptions.RequestException as e:
        return Exception(f"HTTP Error: {e}")

# --- MAIN EXECUTION LOGIC ---
if fetch_btn:
    if not fred_api_key or not bot_client_id:
        st.warning("⚠️ Please provide both BOT Client ID and FRED API Key in the sidebar.")
    else:
        with st.spinner("Fetching data from BOT and FRED APIs..."):
            effective_date_str = selected_date.strftime("%Y-%m-%d")
            
            # Define API metadata mapping exactly as requested
            api_mappings = [
                # (CURVE_NAME, TENOR, SOURCE, ID_OR_PATH)
                ("THOR_OIS", "1D", "BOT", "/stat/v1/financial_markets/thor_rate/"),
                ("THB_DISCOUNTING", "1D", "BOT", "/stat/v1/monetary_policy/policy_rate/"),
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
            
            # Process each request row
            for curve, tenor, source, api_uid in api_mappings:
                rate_val, val_date = None, None
                res = None
                
                if source == "BOT":
                    res = fetch_bot_data(bot_client_id, api_uid, selected_date)
                else:
                    res = fetch_fred_data(fred_api_key, api_uid, selected_date)
                
                # Check for exceptions/errors vs clean data tuple
                if isinstance(res, Exception):
                    errors.append(f"**{source} ({curve} {tenor})**: {str(res)}")
                else:
                    val_date, rate_val = res
                
                # Append constructed row
                results.append({
                    "CURVE_NAME": curve,
                    "TENOR": tenor,
                    "RATE_VALUE": rate_val if rate_val is not None else "N/A",
                    "EFFECTIVE_DATE": effective_date_str,
                    "VALUE_DATE": val_date if val_date is not None else "N/A"
                })
            
            # Construct DataFrame exactly matching required columns
            df = pd.DataFrame(results, columns=["CURVE_NAME", "TENOR", "RATE_VALUE", "EFFECTIVE_DATE", "VALUE_DATE"])
            
            # Output and Rendering
            if errors:
                st.warning("⚠️ Data fetch finished with some errors (likely due to invalid credentials, incorrect path or missing data).")
                with st.expander("View Error Details"):
                    for e in errors:
                        st.error(e)
            else:
                st.success(f"✅ Data fetch complete for **{effective_date_str}**!")
            
            st.subheader("Data Preview")
            st.dataframe(df, use_container_width=True)
            
            # Export controls
            st.divider()
            csv_data = df.to_csv(index=False)
            st.download_button(
                label="📥 Download as CSV",
                data=csv_data,
                file_name=f"Treasury_Rates_{effective_date_str}.csv",
                mime="text/csv",
                type="primary"
            )
else:
    st.info("👈 Enter your API credentials in the sidebar, select a date, and click 'Fetch Data' to begin.")
