from fredapi import Fred

# Your FRED API key
FRED_API_KEY = "c3269990bc1c013bbfe7e7bb0693bc88"
fred = Fred(api_key=FRED_API_KEY)

# Indicators that were missing metadata
indicators = {
    "Median Household Income": "MEHOINUSA646N",
    "Income Share of Top 1%": "WFRBST01134",
    "Corporate Profits": "CP"
}

for name, code in indicators.items():
    meta = fred.get_series_info(code)

    print(f"📊 Metadata for {name} ({code}):")
    print(f"   - Frequency: {meta.get('frequency_short', 'N/A')}")
    print(f"   - Units: {meta.get('units_short', 'N/A')}")
    print(f"   - Last Updated: {meta.get('last_updated', 'N/A')}")
