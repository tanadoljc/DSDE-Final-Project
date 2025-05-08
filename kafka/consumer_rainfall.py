from kafka import KafkaConsumer
import requests
import pandas as pd
import redis
import json
from datetime import datetime

consumer = KafkaConsumer(
    'coordinates-topic',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

r = redis.Redis(host='localhost', port=6379, db=0)

# Fixed date range
start = "20220902"
end = "20250116"

for message in consumer:
    coord = message.value
    lat = coord['lat']
    lon = coord['lon']
    rounded_lat = round(lat, 2)
    rounded_lon = round(lon, 2)

    url = (
        f"https://power.larc.nasa.gov/api/temporal/daily/point?"
        f"parameters=PRECTOTCORR&community=RE&longitude={lon}&latitude={lat}"
        f"&start={start}&end={end}&format=JSON"
    )

    try:
        response = requests.get(url)
        data = response.json()
        rain_data = data['properties']['parameter']['PRECTOTCORR']

        records = []
        for date_str, value in rain_data.items():
            if value != -999.0:
                # Cache in Redis
                key = f"rain:{rounded_lat}:{rounded_lon}:{date_str}"
                r.set(key, value)
                print(f"✅ Cached: {key} = {value}")

                # Prepare record for CSV
                records.append({
                    "date": datetime.strptime(date_str, "%Y%m%d").date(),
                    "lat": lon,
                    "long": lat,
                    "rounded_lat": rounded_lat,
                    "rounded_lon": rounded_lon,
                    "Precipitation (mm)": value
                })

        # Save to CSV if there are any valid records
        if records:
            df = pd.DataFrame(records)
            df = df[["date", "lat", "long", "rounded_lat", "rounded_lon", "Precipitation (mm)"]]
            df.to_csv("rainfall_data.csv", mode='a', header=not pd.io.common.file_exists("rainfall_data.csv"), index=False)
            print("📝 Saved CSV: rainfall_data.csv")

    except Exception as e:
        print(f"❌ Error fetching rainfall: {e}")
