from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
import subprocess
import os

# =========================
# CONFIG
# =========================
# Tự động lấy đường dẫn tuyệt đối đến thư mục chứa file DAG này
DAGS_FOLDER = os.path.dirname(os.path.realpath(__file__))

# Nối đường dẫn động vào các script con
SCRIPT = os.path.join(DAGS_FOLDER,"scripts","ingestion", "web","ingestion_binance_trade_realtime", "ingestion_binance_trade_realtime.py")


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# =========================
# FUNCTIONS
# =========================


def realtime_task():


    # Thêm capture_output=True và text=True để gom log lỗi của file script
    result = subprocess.run(
        [
            "python",
            SCRIPT
        ],
        capture_output=True,
        text=True
    )
    
    # In log chuẩn (nếu có)
    if result.stdout:
        print(f"--- SCRIPT STDOUT ---")
        print(result.stdout)
        
    # In lỗi chi tiết (Chỗ này sẽ nói rõ script bị thiếu cái gì)
    if result.stderr:
        print(f"--- SCRIPT STDERR ---")
        print(result.stderr)
        
    # Giữ nguyên cơ chế báo lỗi cho Airflow biết
    if result.returncode != 0:
        raise Exception(f"Script failed with exit code {result.returncode}")

# def run_load():
#     subprocess.run(["python", LOAD_SCRIPT], check=True)



# =========================
# DAG
# =========================
with DAG(
    dag_id="binance_realtime_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="30 01 * * *", 
    catchup=False,
    max_active_runs=1,
    tags=["binance", "etl"],
) as dag:

    run_realtime_task = PythonOperator(
        task_id="ingestion_step",
        python_callable=realtime_task,
    )

    run_realtime_task 