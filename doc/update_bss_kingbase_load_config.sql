-- bss_kingbase_load：从 exchange/bss_kingbase_extract 解包后加载到 kel 库 bss_load schema（目标端）。
-- 不填 sourceBatch 时按本作业 input_directory 取最新批次；填则加载指定批次。
-- 执行: PGPASSWORD=kel psql -h 127.0.0.1 -p 5432 -U kel -d kel -f doc/update_bss_kingbase_load_config.sql
INSERT INTO kel.job_config (config_key, content_yaml)
VALUES ('bss_kingbase_load', $yaml$
job:
  type: "KINGBASE_LOAD"
  name: "bss_kingbase_load"
  description: "BSS 金仓加载（解包后 COPY 入 bss_load schema）"
workDir: "work"
exchangeDir: "exchange"
input_directory: "exchange/bss_kingbase_extract"
target_database:
  host: "127.0.0.1"
  port: 5432
  name: "kel"
  user: "kel"
  password: "kel"
load_tasks:
  - type: "TRUNCATE_LOAD"
    interfaceMapping:
      "bss.orders": "bss_load.orders"
      "bss.customers": "bss_load.customers"
    enableTransaction: true
runtime:
  table_concurrency: 1
$yaml$)
ON CONFLICT (config_key) DO UPDATE SET content_yaml = EXCLUDED.content_yaml;
