docker exec -i clickhouse clickhouse-client --query="TRUNCATE TABLE raw.purchase_behavious"

Get-Content .\clickhouse_data\date_input\purchase_behavious.csv | docker exec -i clickhouse clickhouse-client `
  --date_time_input_format=best_effort `
  --query="INSERT INTO raw.purchase_behavious FORMAT CSVWithNames"