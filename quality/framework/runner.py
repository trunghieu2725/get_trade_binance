from pathlib import Path
import uuid
from quality.framework.registry import RuleRegistry
from quality.framework.executor import ClickHouseExecutor


def evaluate(result, metric, operator, threshold):

    value = result.get(metric)
    if value is None:
        raise Exception(
            f"Metric {metric} not found in SQL result"
        )

    if operator == "<=":
        return value <= threshold

    if operator == ">=":
        return value >= threshold

    if operator == "=":
        return value == threshold

    raise Exception(f"Unsupported operator {operator}")


def main():

    run_id = str(uuid.uuid4())

    project_root = Path(__file__).resolve().parents[2]

    rules_file = project_root / "quality/config/rules.yml"

    registry = RuleRegistry(rules_file)

    rules = registry.load_rules()


    executor = ClickHouseExecutor(
        project_root / "quality/config/settings.yml"
    )


    report = []


    for rule in rules:

        if not rule.enabled:
            continue


        sql_file = (
            project_root
            / "quality/rules"
            / rule.query
        )


        result = executor.execute_sql_file(sql_file)
        print(rule.id, result)

        passed = evaluate(
            result,
            rule.metric,
            rule.operator,
            rule.threshold
        )


        report.append({
            "id": rule.id,
            "name": rule.name,
            "severity": rule.severity,
            "status": "PASS" if passed else "FAIL",
            "metric": rule.metric,
            "value": result.get(rule.metric),
            "details": result.get("failed_records")
        })
        executor.save_quality_result(
            {
                "run_id": run_id,
                "rule_id": rule.id,
                "rule_name": rule.name,
                "layer": rule.layer,
                "severity": rule.severity,
                "status": "PASS" if passed else "FAIL",
                "metric": rule.metric,
                "value": result[rule.metric],
                "threshold": rule.threshold,
                "failed_records": result.get(
                    "failed_records",
                    []
                )
            }
        )

    print("=" * 60)
    print("DATA QUALITY REPORT")
    print("=" * 60)


    for r in report:

        print(
        f"{r['id']} | "
        f"{r['name']} | "
        f"{r['status']} | "
        f"{r['metric']}={r['value']}"
        )
        if r["details"]:
            print("Failed records:")
            print(r["details"])


if __name__ == "__main__":
    main()
