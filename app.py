import streamlit as st
import pandas as pd
import requests
from datetime import timedelta
import datetime

st.set_page_config(page_title="Market-Interest rate extractor", page_icon="🏦", layout="wide")

st.title("🏦 Market-Interest rate extractor")
st.markdown("Automated synchronization via direct API integration with BOT and FRED.")

# --- SIDEBAR INPUTS ---
st.sidebar.header("API Credentials")
bot_client_id = st.sidebar.text_input("BOT API Token", type="password")
fred_api_key = st.sidebar.text_input("FRED API Token", type="password")

st.sidebar.header("Data Selection")
selected_date = st.sidebar.date_input("EFFECTIVE_DATE", datetime.date.today())
fetch_btn = st.sidebar.button("Fetch Data", type="primary")

# --- BOT PARSERS ---
def parse_thor(result, fallback_date):
    data_list = result.get("data", [])
    if isinstance(data_list, list) and len(data_list) > 0:
        rec = data_list[0]
        rate = rec.get("rate")
        date = rec.get("period") or fallback_date
        if rate is not None:
            return (date, float(rate))
    return None

def parse_policy_rate(result, fallback_date):
    data = result.get("data", {})
    if isinstance(data, dict):
        rate = data.get("value") or data.get("rate")
        date = data.get("effective_date") or fallback_date
        if rate is not None:
            return (date, float(rate))
    return None

# --- BOT FETCH ---
def fetch_bot_data(client_id, path, target_date):
    base_url = "https://gateway.api.bot.or.th"

    headers = {
        "X-IBM-Client-Id": client_id,
        "Authorization": f"Bearer {client_id}",
        "accept": "application/json"
    }

    for i in range(14):
        check_date = target_date - timedelta(days=i)
        check_date_str = check_date.strftime("%Y-%m-%d")

        url = f"{base_url}{path}?start_period={check_date_str}&end_period={check_date_str}"

        try:
            resp = requests.get(url, headers=headers, timeout=15)

            if resp.status_code == 401:
                return Exception("Unauthorized (Check BOT Token)")

            resp.raise_for_status()
            json_data = resp.json()
            result = json_data.get("result", {})

            # 🔥 Route to correct parser
            if "thor_rate" in path:
                parsed = parse_thor(result, check_date_str)
            elif "policy_rate" in path:
                parsed = parse_policy_rate(result, check_date_str)
            else:
                parsed = None

            if parsed:
                return parsed

        except Exception:
            continue

    return Exception("No BOT data found (14-day lookback)")

# --- FRED FETCH ---
def fetch_fred_data(api_key, series_id, target_date):
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
        response.raise_for_status()
        data = response.json()

        for obs in data.get("observations", []):
            val = obs.get("value")
            if val not in [".", None, ""]:
                return (obs.get("date"), float(val))

        return Exception("No valid FRED data")

    except Exception as e:
        return Exception(f"FRED Error: {str(e)}")

# --- MAIN ---
if fetch_btn:
    if not bot_client_id or not fred_api_key:
        st.warning("⚠️ Please provide API credentials.")
    else:
        with st.spinner("Fetching data..."):

            effective_date_str = selected_date.strftime("%Y-%m-%d")

            api_mappings = [
                ("THOR_OIS", "1D", "BOT", "/stat/v1/financial_markets/thor_rate"),
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

                if source == "BOT":
                    res = fetch_bot_data(bot_client_id, api_uid, selected_date)
                else:
                    res = fetch_fred_data(fred_api_key, api_uid, selected_date)

                if isinstance(res, Exception):
                    errors.append(f"{curve} ({source}): {str(res)}")
                    val_date, rate_val = None, None
                else:
                    val_date, rate_val = res

                results.append({
                    "CURVE_NAME": curve,
                    "TENOR": tenor,
                    "RATE_VALUE": rate_val if rate_val else "N/A",
                    "EFFECTIVE_DATE": effective_date_str,
                    "VALUE_DATE": val_date if val_date else "N/A"
                })

            df = pd.DataFrame(results)
            df = df.astype(str)

            if errors:
                st.warning("⚠️ Completed with some errors")
                with st.expander("Details"):
                    for e in errors:
                        st.error(e)
            else:
                st.success("✅ All data fetched successfully")

            st.subheader("Data Preview")
            st.dataframe(df, use_container_width=True)

            csv = df.to_csv(index=False)
            st.download_button("📥 Download CSV", csv, file_name=f"rates_{effective_date_str}.csv")

else:
    st.info("👈 Enter credentials and click Fetch Data")
