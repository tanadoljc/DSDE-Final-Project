## 🚀 External Data Pipeline

### 1. เตรียมไฟล์และติดตั้ง dependencies
```bash
pip install -r requirements.txt
```

### 2. สั่งให้ docker-compose ทำงาน
```bash
docker-compose up -d
```

### 3. สร้าง Kafka topic
```bash
docker exec -it final-kafka-1 bash
/app/init_kafka.sh
```

### 4. เปิด Airflow Web UI
- URL: http://localhost:8080
- เปิดใช้งาน DAG `external_data_pipeline`

### 5. ตรวจสอบ Redis
```bash
redis-cli
keys *
```