import streamlit as st
import pandas as pd
import requests
from datetime import timedelta
import datetime

st.set_page_config(page_title="Market-Interest rate extractor", page_icon="🏦", layout="wide")

st.title("🏦 Market-Interest rate extractor")
st.markdown("Automated synchronization via direct API integration with the official Bank of Thailand and St. Louis Fed portals.")

# --- SIDEBAR INPUTS ---
st.sidebar.header("API Credentials")
bot_client_id = st.sidebar.text_input("BOT API Token - Interest Rates", type="password", help="Enter your Bank of Thailand API Token")
fred_api_key = st.sidebar.text_input("FRED API Token", type="password", help="Enter your St. Louis Fed API Key")

st.sidebar.header("Data Selection")
selected_date = st.sidebar.date_input("EFFECTIVE_DATE", datetime.date.today())
fetch_btn = st.sidebar.button("Fetch Data", type="primary")

# --- HELPER FUNCTIONS ---
def extract_latest_bot_rate(json_data, selected_date_str):
    """
    Directly extracts rate from the new BOT v2/v3 Response format
    """
    try:
        result = json_data.get('result', {})
        
        # Handle nested data structure (e.g., {"value": 2.5})
        rate = result.get('data')
        if isinstance(rate, dict):
            rate = rate.get('value') or rate.get('rate') or rate.get('data')
            
        # Try getting other common keys if still None
        if rate is None:
            # Look for ANY numeric value in the result keys
            for k, v in result.items():
                if isinstance(v, (int, float)):
                    rate = v
                    break
            
        rate_date = result.get('effective_datetime') or result.get('timestamp') or selected_date_str
        
        if rate is not None:
             return (str(rate_date)[:10], float(rate)), None
             
        return None, f"No numeric rate found in result keys: {list(result.keys())}"
    except Exception as e:
        return None, f"Parse error: {str(e)}"

def fetch_bot_data(client_id, path, target_date):
    """
    Fetches Bank of Thailand endpoint looking back up to 7 days
    """
    start_date = target_date - timedelta(days=7)
    
    # NEW 2026 BOT GATEWAY URL
    base_url = "https://gateway.api.bot.or.th"
    url = f"{base_url}{path}"
    
    # 2026 MIGRATION FIX:
    # Most new BOT v2/v3 endpoints require the token as a Bearer.
    token = client_id if client_id.startswith("Bearer ") else f"Bearer {client_id}"
    
    # Try to decode the 'id' from the Base64 token if possible
    final_client_id = client_id
    try:
        import base64, json
        decoded = json.loads(base64.b64decode(client_id + "==").decode('utf-8'))
        final_client_id = decoded.get('id', client_id)
    except:
        pass

    headers = {
        "X-IBM-Client-Id": final_client_id,
        "Authorization": token,
        "accept": "application/json"
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
             return Exception("Unauthorized (Check FRED API Token)")
             
        response.raise_for_status()
        data = response.json()
        
        observations = data.get("observations", [])
        if observations:
            obs = observations[0]
            val = obs.get("value")
            val_date = obs.get("date")
            
            # If value is missing or '.', extend search back (FRED limit=1 with sort DESC handles search)
            if val == "." or val is None or val == "":
                # Try expanding the query range slightly
                params["limit"] = 10 
                resp2 = requests.get(url, params=params, timeout=15)
                resp2.raise_for_status()
                obs2 = resp2.json().get("observations", [])
                
                for o in obs2:
                    if o.get("value") not in [".", None, ""]:
                        return (o.get("date"), float(o.get("value")))
                return Exception("Data unavailable in the last 10 observations")
                
            return (val_date, float(val))
            
        return Exception(f"No series observations found for {series_id}")
    except requests.exceptions.RequestException as e:
        return Exception(f"Connection/HTTP Error: {e}")

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
            
            # FORCE all columns to strings to prevent 'pyarrow.lib.ArrowTypeError' crashes in Streamlit
            df = df.astype(str)
            
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
