docker exec -i clickhouse clickhouse-client --query="TRUNCATE TABLE raw.log_tracking"

Get-Content .\clickhouse_data\date_input\log_tracking.csv | docker exec -i clickhouse clickhouse-client `
  --date_time_input_format=best_effort `
  --query="INSERT INTO raw.log_tracking FORMAT CSVWithNames"