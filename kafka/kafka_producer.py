from kafka import KafkaProducer
import pandas as pd
import json
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

df = pd.read_csv('../used_data.csv')
df['rounded_lat'] = df['lat'].round(2)
df['rounded_lon'] = df['long'].round(2)
unique_coords = df[['rounded_lat', 'rounded_lon']].drop_duplicates()

unique_coords.sample(n=2, )

for _, row in unique_coords.iterrows():
    payload = {'lat': row['rounded_lat'], 'lon': row['rounded_lon']}
    producer.send('coordinates-topic', value=payload)
    print(f"Sent to Kafka: {payload}")
    time.sleep(0.5)  # pacing requests