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
INGESTION_SCRIPT = os.path.join(DAGS_FOLDER,"scripts","ingestion", "web", "ingestion_binance_trade_batchjob.py")

LOAD_SCRIPT = os.path.join(DAGS_FOLDER,"scripts", "load", "load_binance_trade_batchjob.py")

DELETE_SCRIPT = os.path.join(DAGS_FOLDER,"scripts", "delete", "delete_binance_trade_realtime.py")


DELETE_SCRIPT_BATCHJOB = os.path.join(DAGS_FOLDER,"scripts", "delete", "delete_binance_trade_batchjob.py")


DBT_PROJECT = "/opt/airflow/dbt_project"
DBT_PROFILES = "/opt/airflow/dbt_profiles/"


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# =========================
# FUNCTIONS
# =========================
# def run_ingestion():
#     subprocess.run(["python", INGESTION_SCRIPT], check=True)
def get_run_date(context):

    conf = context["dag_run"].conf or {}

    if conf.get("date"):
        return conf["date"]

    return (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")


def run_ingestion(**context):

    run_date = get_run_date(context)
    # Thêm capture_output=True và text=True để gom log lỗi của file script
    result = subprocess.run(
        [
            "python",
            INGESTION_SCRIPT,
            "--date",
            run_date
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

def run_load(**context):

    run_date = (get_run_date(context) - timedelta(days=2)).strftime("%Y-%m-%d")
    # Thêm capture_output=True và text=True để gom log lỗi lại
    result = subprocess.run(
        [
            "python",
            LOAD_SCRIPT,
            "--date",
            run_date
        ],
        capture_output=True,
        text=True
    )

    
    # In cả log chuẩn và log lỗi ra Airflow Logs
    if result.stdout:
        print(f"STDOUT:\n{result.stdout}")
    if result.stderr:
        print(f"STDERR:\n{result.stderr}")
        
    # Vẫn phải raise lỗi để Airflow biết là task bị Fail
    if result.returncode != 0:
        raise Exception(f"Script failed with exit code {result.returncode}")
    
def run_dbt(**context):

    run_date = get_run_date(context)

    cmd = [
        "dbt",
        "run",
        "--project-dir",
        DBT_PROJECT,
        "--profiles-dir",
        DBT_PROFILES,         # nếu profiles.yml nằm trong dbt_project
        "--select",
        "stg_binance_trade_batchjob",
        "--vars",
        f'{{"run_date":"{run_date}"}}'
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)

    print(result.stdout)

    if result.stderr:
        print(result.stderr)

    if result.returncode != 0:
        raise Exception("dbt run failed")
    
def delete_realtime(**context):

    run_date = get_run_date(context)
    # Thêm capture_output=True và text=True để gom log lỗi lại
    result = subprocess.run(
        [
            "python",
            DELETE_SCRIPT,
            "--date",
            run_date
        ],
        capture_output=True,
        text=True
    )

    
    # In cả log chuẩn và log lỗi ra Airflow Logs
    if result.stdout:
        print(f"STDOUT:\n{result.stdout}")
    if result.stderr:
        print(f"STDERR:\n{result.stderr}")
        
    # Vẫn phải raise lỗi để Airflow biết là task bị Fail
    if result.returncode != 0:
        raise Exception(f"Script delete realtime failed with exit code {result.returncode}")
    

def delete_batchjob(**context):

    run_date = get_run_date(context)
    # Thêm capture_output=True và text=True để gom log lỗi lại
    result = subprocess.run(
        [
            "python",
            DELETE_SCRIPT_BATCHJOB,
            "--date",
            run_date
        ],
        capture_output=True,
        text=True
    )

    
    # In cả log chuẩn và log lỗi ra Airflow Logs
    if result.stdout:
        print(f"STDOUT:\n{result.stdout}")
    if result.stderr:
        print(f"STDERR:\n{result.stderr}")
        
    # Vẫn phải raise lỗi để Airflow biết là task bị Fail
    if result.returncode != 0:
        raise Exception(f"Script delete batchjob failed with exit code {result.returncode}")

# =========================
# DAG
# =========================
with DAG(
    dag_id="binance_daily_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="00 3 * * *", 
    catchup=False,
    max_active_runs=1,
    tags=["binance", "etl"],
) as dag:

    ingestion_task = PythonOperator(
        task_id="ingestion_step",
        python_callable=run_ingestion,
    )

    load_task = PythonOperator(
        task_id="load_clickhouse_step",
        python_callable=run_load,
    )
    dbt_task = PythonOperator(
        task_id="dbt_staging_step",
    
        python_callable=run_dbt,
    )
    delete_realtime_task = PythonOperator(
        task_id="delete_realtime_step",
        python_callable=delete_realtime,
    )
    delete_batchjon_task = PythonOperator(
        task_id="delete_batchjob_step",
        python_callable=delete_batchjob,
    )
    ingestion_task >> load_task >> dbt_task >> delete_realtime_task >> delete_batchjon_task