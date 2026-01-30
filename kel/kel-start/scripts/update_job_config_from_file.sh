#!/usr/bin/env bash
# 用 psql 把本地 YAML 文件内容写入 kel.job_config 的 content_yaml。
# 用法: ./scripts/update_job_config_from_file.sh <job_name> <yaml_file>
# 示例: ./scripts/update_job_config_from_file.sh daily_sync src/main/resources/conf/job.yaml
#
# 注意：job_name 必须与 YAML 里 job.name 一致；若库中无该 job_name 会 INSERT，有则 UPDATE。

set -e
JOB_NAME="$1"
YAML_FILE="$2"
if [ -z "$JOB_NAME" ] || [ -z "$YAML_FILE" ]; then
  echo "用法: $0 <job_name> <yaml_file>"
  exit 1
fi
if [ ! -f "$YAML_FILE" ]; then
  echo "文件不存在: $YAML_FILE"
  exit 1
fi

# 使用临时文件 + 美元引用，避免 YAML 中的单引号/反斜杠问题
TMP_SQL=$(mktemp)
trap "rm -f $TMP_SQL" EXIT

{
  echo "SET search_path TO kel;"
  echo "INSERT INTO job_config (job_name, content_yaml) VALUES ('$JOB_NAME', \$yaml\$"
  cat "$YAML_FILE"
  echo "\$yaml\$) ON CONFLICT (job_name) DO UPDATE SET content_yaml = EXCLUDED.content_yaml, updated_at = CURRENT_TIMESTAMP;"
  echo "SELECT job_name, length(content_yaml), updated_at FROM job_config WHERE job_name = '$JOB_NAME';"
} > "$TMP_SQL"

PGPASSWORD=kel psql -h 127.0.0.1 -p 5432 -U kel -d kel -f "$TMP_SQL"
