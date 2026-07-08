FROM apache/airflow:2.8.1-python3.11

# Chuyển sang user airflow để cài gói vào đúng môi trường
USER airflow

# Copy file và phân quyền cho user airflow
COPY --chown=airflow:root requirements.txt /opt/airflow/requirements.txt

# Cài đặt toàn bộ các gói trong file requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt
RUN pip install clickhouse-connect clickhouse-sqlalchemy psycopg2-binary