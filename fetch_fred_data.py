from fredapi import Fred
import psycopg2
import pandas as pd

print("🚀 Script started...")

# Your FRED API Key
FRED_API_KEY = "c3269990bc1c013bbfe7e7bb0693bc88"

# Initialize FRED API
fred = Fred(api_key=FRED_API_KEY)

# Define economic indicators with correct Series IDs
indicators = {
    "Income Share of Top 1%": "WFRBST01134",
    "GDP Growth Rate": "A191RL1Q225SBEA",
    "Corporate Profits": "CP",
    "Median Household Income": "MEHOINUSA646N"
}

print("📊 Connecting to PostgreSQL...")
try:
    conn = psycopg2.connect(
        dbname="economic_data",
        user="postgres",
        password="f813Vtp7!",
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()
    print("✅ PostgreSQL connection successful!")
except Exception as e:
    print(f"❌ ERROR: PostgreSQL connection failed! {e}")
    exit()

# Fetch and insert data for each indicator
for name, code in indicators.items():
    try:
        print(f"📥 Fetching data for {name} ({code})...")
        data = fred.get_series(code)

        # Fetch metadata
        meta = fred.get_series_info(code)

        # Extract metadata
        frequency = meta.get("frequency_short", "N/A")
        units = meta.get("units_short", "N/A")
        last_updated = meta.get("last_updated", "N/A")

        # Debugging print to verify metadata before inserting
        print(f"📊 Metadata for {name} ({code}):")
        print(f"   - Frequency: {frequency}")
        print(f"   - Units: {units}")
        print(f"   - Last Updated: {last_updated}")

        # Convert to DataFrame
        df = pd.DataFrame(data, columns=["value"])
        df["date"] = df.index
        df["indicator_name"] = name
        df["indicator_code"] = code

        # Force metadata into the DataFrame (ensures every row has metadata)
        df["frequency"] = frequency
        df["units"] = units
        df["last_updated"] = last_updated

        print(f"💾 Inserting data for {name} into the database...")
        for _, row in df.iterrows():
            row_frequency = frequency if frequency else "Unknown"
            row_units = units if units else "N/A"
            row_last_updated = last_updated if last_updated else "1970-01-01"

            print(f"📊 Debugging Insert for {name}:")
            print(f"   - Date: {row['date']}")
            print(f"   - Value: {row['value']}")
            print(f"   - Frequency: {row_frequency}")
            print(f"   - Units: {row_units}")
            print(f"   - Last Updated: {row_last_updated}")

            cur.execute("""
                INSERT INTO fred_data (indicator_name, indicator_code, date, value, frequency, units, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (row["indicator_name"], row["indicator_code"], row["date"], row["value"], row_frequency, row_units, row_last_updated))


        print(f"✅ Successfully inserted data for {name}")

    except Exception as e:
        print(f"❌ ERROR fetching {name}: {e}")

print("📢 Committing changes to the database...")
conn.commit()
cur.close()
conn.close()

print("🎉 Data scraping completed and saved to PostgreSQL!")
