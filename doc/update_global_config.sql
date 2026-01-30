-- 更新或插入全局配置 __global__（仅通过 DB 维护，不再使用本地 global.yaml）
-- 一致性加密：卸载与加载均从 DB 读取 __global__，使用同一 security.sm4_key 与 enable_encryption，请勿单独改其一。
-- 执行: PGPASSWORD=kel psql -h 127.0.0.1 -p 5432 -U kel -d kel -f doc/update_global_config.sql
INSERT INTO kel.job_config (config_key, content_yaml)
VALUES ('__global__', $yaml$
concurrency:
  default_table_concurrency: 2
retry:
  max_retries: 1
  retry_interval_sec: 60
compression:
  algorithm: "gzip"
  split_threshold_gb: 3.5
enable_compression: true
security:
  sm4_key: "demo_key"
  enable_encryption: true
extract:
  work_dir: "work"
  batch_number_format: "{yyyyMMddHHmmss}_{seq}"
  encoding: "UTF-8"
  database_version: "V8R6"
  cleanup_work_dir: false
$yaml$)
ON CONFLICT (config_key) DO UPDATE SET content_yaml = EXCLUDED.content_yaml;
