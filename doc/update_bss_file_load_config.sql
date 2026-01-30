-- bss_file_load：加载侧独立配置，与卸载侧不共享配置/数据目录。input_directory 必填（包所在根目录）。
-- 路径 = input_directory + "/" + 批次号；批次号由调用方显式传入（sourceBatch）。
-- 执行: PGPASSWORD=kel psql -h 127.0.0.1 -p 5432 -U kel -d kel -f doc/update_bss_file_load_config.sql
INSERT INTO kel.job_config (config_key, content_yaml)
VALUES ('bss_file_load', $yaml$
job:
  type: "FILE_LOAD"
  name: "bss_file_load"
  description: "BSS 文件加载（input_directory 为包根目录，路径=input_directory/批次号）"
workDir: "work"
exchangeDir: "exchange"
# 包所在根目录（必填）。加载侧路径，与卸载侧无共享
input_directory: "exchange/bss_file_extract"
targetDirectory: "data/bss_file_load/output"
runtime:
  table_concurrency: 1
$yaml$)
ON CONFLICT (config_key) DO UPDATE SET content_yaml = EXCLUDED.content_yaml;
