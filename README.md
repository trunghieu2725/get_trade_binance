# Binance ClickHouse Pipeline

Du an nay dung de thu thap du lieu giao dich Binance, luu vao ClickHouse, dieu phoi bang Airflow va bien doi du lieu bang dbt.

## Thanh phan chinh

- **ClickHouse**: database phan tich de luu raw, staging va mart data.
- **Airflow**: dieu phoi pipeline ingestion va load du lieu.
- **Kafka + Zookeeper**: ho tro luong du lieu realtime.
- **dbt**: quan ly model SQL va bien doi du lieu trong ClickHouse.
- **Python scripts**: tai du lieu Binance batch/realtime va load vao ClickHouse.

## Yeu cau

- Docker
- Docker Compose
- Python 3.11 neu muon chay script/dbt truc tiep tren may local

## Cai dat nhanh

Clone repository:

```bash
git clone <repository-url>
cd ClickHouse
```

Tao file `.env` tu mau ben duoi:

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

Khoi dong cac service:

```bash
docker compose up -d
```

Kiem tra container:

```bash
docker compose ps
```

## Truy cap dich vu

- Airflow UI: http://localhost:8080
- ClickHouse HTTP: http://localhost:8123
- ClickHouse native port: `localhost:9000`
- Kafka host port: `localhost:9092`

Tai khoan Airflow mac dinh se lay theo `.env`:

- Username: gia tri cua `AIRFLOW_ADMIN_USER`
- Password: gia tri cua `AIRFLOW_ADMIN_PASSWORD`

## Cai dat Python dependencies

Neu can chay script hoac dbt tren local:

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

Tren macOS/Linux:

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Chay Airflow pipeline

Sau khi `docker compose up -d`, mo Airflow UI tai http://localhost:8080, bat DAG:

```text
binance_daily_pipeline
```

DAG nay se:

1. Tai du lieu trade Binance theo ngay.
2. Luu file parquet vao thu muc `data/spot_trade_daily`.
3. Load du lieu vao ClickHouse.

## Chay dbt

dbt project nam trong thu muc `dbt_project`.

Tao file profile local cho dbt. Khuyen nghi khong commit file profile chua password len GitHub.

Vi du `dbt_project/profiles.yml`:

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

Chay dbt:

```bash
cd dbt_project
dbt deps
dbt debug --profiles-dir .
dbt run --profiles-dir .
```

Neu muon chay theo target khac:

```bash
dbt run --profiles-dir . --target prod
```

## Cau truc thu muc

```text
.
├── airflow/
│   └── dags/
│       ├── binance_trade_dag.py
│       └── scripts/
├── data/
├── dbt_project/
│   ├── models/
│   ├── dbt_project.yml
│   └── packages.yml
├── docker-compose.yaml
├── Dockerfile
├── requirements.txt
└── README.md
```

## Luu y khi commit len GitHub

Khong nen commit cac file va thu muc local sau:

- `.env`
- `venv/`
- `clickhouse_data/`
- `clickhouse_logs/`
- `airflow_postgres_data/`
- `airflow/logs/`
- `dbt_project/target/`
- `dbt_project/dbt_packages/`
- `dbt_project/logs/`
- cac file `.parquet`, `.csv` sinh ra trong qua trinh chay pipeline

Neu da tung commit file chua password, hay doi password va xoa secret khoi lich su Git truoc khi public repository.

## Dung container

Dung container:

```bash
docker compose down
```

Dung va xoa volume local:

```bash
docker compose down -v
```

Lenh `down -v` se xoa du lieu ClickHouse va Postgres local, chi dung khi ban chac chan khong can giu data.
