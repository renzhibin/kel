-- bss_kingbase_extract：从 kel 库 bss schema 卸载到 exchange。
-- 执行: PGPASSWORD=kel psql -h 127.0.0.1 -p 5432 -U kel -d kel -f doc/update_bss_kingbase_extract_config.sql
INSERT INTO kel.job_config (config_key, content_yaml)
VALUES ('bss_kingbase_extract', $yaml$
job:
  type: "EXTRACT_KINGBASE"
  name: "bss_kingbase_extract"
  description: "BSS 金仓卸载（bss schema 表导出）"
extract_database:
  host: "127.0.0.1"
  port: 5432
  name: "kel"
  user: "kel"
  password: "kel"
workDir: "work"
exchangeDir: "exchange"
extract_tasks:
  - type: "FULL"
    tables:
      - "bss.orders"
      - "bss.customers"
runtime:
  table_concurrency: 1
$yaml$)
ON CONFLICT (config_key) DO UPDATE SET content_yaml = EXCLUDED.content_yaml;
