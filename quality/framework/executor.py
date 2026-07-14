from pathlib import Path
import yaml
import clickhouse_connect
from pathlib import Path
import json
import os
import yaml
import clickhouse_connect
from dotenv import load_dotenv

load_dotenv()


class ClickHouseExecutor:

    def __init__(self, settings_path):
        with open(settings_path, "r") as f:
            self.settings = yaml.safe_load(f)

        self.client = self._connect()


    def _connect(self):
        ch = self.settings["clickhouse"]

        return clickhouse_connect.get_client(
            host=os.path.expandvars(ch["host"]),
            port=int(os.path.expandvars(str(ch["port"]))),
            database=os.path.expandvars(ch["database"]),
            username=os.path.expandvars(ch["username"]),
            password=os.path.expandvars(ch["password"])
        )

    def execute_sql_file(self, sql_file: str) -> dict:

        sql_path = Path(sql_file)

        if not sql_path.exists():
            raise FileNotFoundError(sql_path)

        sql = sql_path.read_text(encoding="utf-8")

        result = self.client.query(sql)

        # Không có kết quả
        if len(result.result_rows) == 0:
            return {}

        row = result.result_rows[0]
        columns = result.column_names

        return dict(zip(columns, row))
    def save_quality_result(self, result):

        self.client.insert(
            "quality.quality_results",
            [
                [
                    result["run_id"],
                    result["rule_id"],
                    result["rule_name"],
                    result["layer"],
                    result["severity"],
                    result["status"],
                    result["metric"],
                    result["value"],
                    result["threshold"],
                    json.dumps(
                        result.get(
                            "failed_records",
                            []
                        )
                    )
                ]
            ],
            column_names=[
                "run_id",
                "rule_id",
                "rule_name",
                "layer",
                "severity",
                "status",
                "metric",
                "metric_value",
                "threshold",
                "failed_records"
            ]
        )