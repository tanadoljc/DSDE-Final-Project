from kafka import KafkaConsumer
import requests
import pandas as pd
import json
import redis
from datetime import datetime

consumer = KafkaConsumer(
    'coordinates-topic',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

r = redis.Redis(host='localhost', port=6379, db=0)

start = "2022-09-02"
end = "2025-01-16"

for message in consumer:
    coord = message.value
    lat = coord['lat']
    lon = coord['lon']
    rounded_lat = round(lat, 2)
    rounded_lon = round(lon, 2)

    print(f"📡 Fetching PM2.5 for ({lat}, {lon})")

    url = "https://air-quality-api.open-meteo.com/v1/air-quality"
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "pm2_5",
        "start_date": start,
        "end_date": end,
        "timezone": "Asia/Bangkok"
    }

    try:
        response = requests.get(url, params=params)
        data = response.json()
        hourly = data.get("hourly", {})
        timestamps = hourly.get("time", [])
        values = hourly.get("pm2_5", [])

        if timestamps and values:
            df = pd.DataFrame({
                "datetime": pd.to_datetime(timestamps),
                "pm2_5": values
            })
            df['date'] = df['datetime'].dt.date
            df = df.groupby('date').mean(numeric_only=True).reset_index()

            records = []
            for _, row in df.iterrows():
                date_str = row['date']
                pm = row['pm2_5']
                key = f"pm25:{rounded_lat}:{rounded_lon}:{date_str}"
                r.set(key, pm)
                print(f"✅ Cached: {key} = {pm}")
                records.append({
                    "date": date_str,
                    "pm2_5": pm,
                    "rounded_lat": rounded_lat,
                    "rounded_lon": rounded_lon
                })

            # Save to CSV
            if records:
                out_df = pd.DataFrame(records)
                out_df = out_df[["date", "pm2_5", "rounded_lat", "rounded_lon"]]
                out_df.to_csv("pm25_data.csv", mode='a', header=not pd.io.common.file_exists("pm25_data.csv"), index=False)
                print("📝 Saved CSV: pm25_data.csv")
        else:
            print(f"❌ No data for ({lat}, {lon})")
    except Exception as e:
        print(f"❌ Error: {e}")
