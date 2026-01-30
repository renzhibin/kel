# 作业配置：旧格式 → 新格式对照

库表 `kel.job_config` 的 `content_yaml` 从旧扁平结构改为新层级（job / settings / resources / tasks）时，字段对应关系如下。

## 层级与变量名对照

| 旧（库里原样） | 新（层级 + 变量名） |
|----------------|---------------------|
| `job.type` / `job.name` / `job.description` | `job.*`（不变） |
| `workDir` | `settings.work_dir` |
| `exchangeDir` | `resources.extract_dir.exchange_dir` |
| `extract_database`（顶层） | `resources.extract_database` |
| `target_database`（顶层） | `resources.target_database` |
| `input_directory` | `resources.load_dir.input_dir` |
| 无对应（文件加载目标目录） | `resources.load_dir.target_dir` 或 tasks[].`target_dir` |
| `extract_tasks`（列表） | `tasks` 中 `mode: "FULL"` / `"INCREMENTAL"` 的项 |
| `extract_tasks[].type: "FULL"` | `tasks[].mode: "FULL"` |
| `extract_tasks[].type: "INCREMENTAL"` | `tasks[].mode: "INCREMENTAL"` |
| `extract_tasks[].tables` | `tasks[].tables` |
| `extract_tasks[].attribute.filePattern` | `tasks[].files.pattern` |
| `extract_tasks[].attribute.timeRange` | `tasks[].files.time_range` |
| `extract_tasks[].attribute.fileSizeLimitMb` | `tasks[].files.size_limit_mb` |
| `load_tasks`（列表） | `tasks` 中 `mode: "TRUNCATE_LOAD"` / `"MERGE"` / `"APPEND"` 的项 |
| `load_tasks[].type` | `tasks[].mode` |
| `load_tasks[].interfaceMapping` | `tasks[].mappings` |
| `load_tasks[].enableTransaction` | `tasks[].transaction` |
| `runtime.table_concurrency` | `settings.runtime.table_concurrency` |

## 用 psql 更新库内配置

- 新格式 YAML 放在 `scripts/job_configs/<job_name>.yaml`。
- 在项目根或 `kel-start` 下执行：
  `./kel-start/scripts/update_job_config_from_file.sh <job_name> <yaml文件路径>`
- 脚本内部会调用 psql，对 `kel.job_config` 做 INSERT ... ON CONFLICT DO UPDATE，写入 `content_yaml`。
