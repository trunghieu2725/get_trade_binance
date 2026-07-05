# Binance ClickHouse Pipeline

Dự án này dùng để thu thập dữ liệu giao dịch Binance, lưu vào ClickHouse, điều phối bằng Airflow và biến đổi dữ liệu bằng dbt.

## Thành phần chính

- **ClickHouse**: database phân tích để lưu raw, staging và mart data.
- **Airflow**: điều phối pipeline ingestion và load dữ liệu.
- **Kafka + Zookeeper**: hỗ trợ luồng dữ liệu realtime.
- **dbt**: quản lý model SQL và biến đổi dữ liệu trong ClickHouse.
- **Python scripts**: tải dữ liệu Binance batch/realtime và load vào ClickHouse.

## Yêu cầu

- Docker
- Docker Compose
- Python 3.11 nếu muốn chạy script/dbt trực tiếp trên máy local

## Cài đặt nhanh

Clone repository:

```bash
git clone <repository-url>
cd ClickHouse
```

Tạo file `.env` từ mẫu bên dưới:

```env
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=change_me

POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow

AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=admin

AIRFLOW_DB_CONN=postgresql+psycopg2://airflow:airflow@airflow-postgres/airflow
```

Khởi động các service:

```bash
docker compose up -d
```

Kiểm tra container:

```bash
docker compose ps
```

## Truy cập dịch vụ

- Airflow UI: http://localhost:8080
- ClickHouse HTTP: http://localhost:8123
- ClickHouse native port: `localhost:9000`
- Kafka host port: `localhost:9092`

Tài khoản Airflow mặc định sẽ lấy theo `.env`:

- Username: giá trị của `AIRFLOW_ADMIN_USER`
- Password: giá trị của `AIRFLOW_ADMIN_PASSWORD`

## Cài đặt Python dependencies

Nếu cần chạy script hoặc dbt trên local:

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

Trên macOS/Linux:

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Chạy Airflow pipeline

Sau khi `docker compose up -d`, mở Airflow UI tại http://localhost:8080, bật DAG:

```text
binance_daily_pipeline
```

DAG này sẽ:

1. Tải dữ liệu trade Binance theo ngày.
2. Lưu file parquet vào thư mục `data/spot_trade_daily`.
3. Load dữ liệu vào ClickHouse.

## Pipeline realtime với Kafka

Ngoài batch pipeline theo ngày, project còn có pipeline realtime để lấy trade trong ngày từ Binance WebSocket và đẩy vào Kafka.

Luồng realtime hiện tại:

```text
Binance WebSocket -> Python Producer -> Kafka topic binance-trade
```

Kafka và Zookeeper đã được cấu hình trong `docker-compose.yaml`. Sau khi chạy:

```bash
docker compose up -d
```

Kafka sẽ lắng nghe ở:

- Trong Docker network: `kafka:29092`
- Từ máy local: `localhost:9092`

Chạy producer realtime từ máy local:

```bash
python airflow/dags/scripts/ingestion/web/ingestion_binance_trade_realtime/producer.py
```

Mặc định producer sẽ:

- Kết nối Binance WebSocket.
- Lấy trade realtime cho các symbol `BTCUSDT,ETHUSDT,ADAUSDT`.
- Gửi message vào Kafka topic `binance-trade`.

Có thể truyền symbol khác khi chạy:

```bash
python airflow/dags/scripts/ingestion/web/ingestion_binance_trade_realtime/producer.py --symbols BTCUSDT,ETHUSDT,ARBUSDT
```

Hoặc truyền Kafka bootstrap server và topic:

```bash
python airflow/dags/scripts/ingestion/web/ingestion_binance_trade_realtime/producer.py --bootstrap localhost:9092 --topic binance-trade
```

Chạy consumer để kiểm tra dữ liệu realtime đang vào Kafka:

```bash
python airflow/dags/scripts/ingestion/web/ingestion_binance_trade_realtime/consumer.py
```

Consumer hiện tại dùng để đọc topic `binance-trade` và in message ra console. Phần dbt model realtime đang đọc từ source `raw.raw_binance_trade_realtime_persist`, vì vậy để persist dữ liệu realtime vào ClickHouse cần có bảng Kafka engine/materialized view tương ứng trong ClickHouse để đổ dữ liệu từ topic `binance-trade` sang bảng raw persist.

## Chạy dbt

dbt project nằm trong thư mục `dbt_project`.

Tạo file profile local cho dbt. Khuyến nghị không commit file profile chứa password lên GitHub.

Ví dụ `dbt_project/profiles.yml`:

```yaml
binance_pipeline:
  target: dev
  outputs:
    dev:
      type: clickhouse
      threads: 1
      host: localhost
      port: 8123
      user: default
      password: "{{ env_var('CLICKHOUSE_PASSWORD') }}"
      database: "{{ env_var('DBT_DATABASE', 'stg_dev') }}"
      schema: "{{ env_var('DBT_DATABASE', 'stg_dev') }}"
      secure: false
      verify: false
```

Chạy dbt:

```bash
cd dbt_project
dbt deps
dbt debug --profiles-dir .
dbt run --profiles-dir .
```

Nếu muốn chạy theo target khác:

```bash
dbt run --profiles-dir . --target prod
```

## Cấu trúc thư mục

```text
.
|-- airflow/
|   `-- dags/
|       |-- binance_trade_dag.py
|       `-- scripts/
|-- data/
|-- dbt_project/
|   |-- models/
|   |-- dbt_project.yml
|   `-- packages.yml
|-- docker-compose.yaml
|-- Dockerfile
|-- requirements.txt
`-- README.md
```





## Dừng container

Dừng container:

```bash
docker compose down
```

Dừng và xóa volume local:

```bash
docker compose down -v
```

Lệnh `down -v` sẽ xóa dữ liệu ClickHouse và Postgres local, chỉ dùng khi bạn chắc chắn không cần giữ data.
