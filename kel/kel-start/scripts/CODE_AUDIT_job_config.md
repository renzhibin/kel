# 作业配置取值排查（新格式兼容）

所有从 JobConfig 读配置的代码均通过**兼容 getter** 取值，新格式（settings/resources/tasks）与旧格式（顶层字段）均能正确解析。

## 取值路径（JobConfig 兼容 getter）

| 调用处用的方法 | 新格式来源 | 旧格式回退 |
|----------------|------------|------------|
| `getJob()` | 不变 | 不变 |
| `getExtractDatabase()` | `resources.extract_database` | 顶层 `extract_database` |
| `getTargetDatabase()` | `resources.target_database` | 顶层 `target_database` |
| `getWorkDir()` | `settings.work_dir` | 顶层 `workDir` |
| `getExchangeDir()` | `resources.extract_dir.exchange_dir` | 顶层 `exchangeDir` |
| `getRuntime()` | `settings.runtime` | 顶层 `runtime` |
| `getInputDirectory()` | `resources.load_dir.input_dir` | 顶层 `input_directory` |
| `getExtractDirectory()` | `resources.extract_dir.extract_dir` | 顶层 `extractDirectory` |
| `getTargetDirectory()` | `resources.load_dir.target_dir` | 顶层 `target_directory` |
| `getExtractTasks()` | 从 `tasks` 筛 mode=FULL/INCREMENTAL 转成 ExtractTaskConfig | 顶层 `extract_tasks` |
| `getLoadTasks()` | 从 `tasks` 筛 mode=TRUNCATE_LOAD/MERGE/APPEND 转成 LoadTaskConfig | 顶层 `load_tasks` |

## 调用处排查结果

| 模块 | 使用的 getter | 结论 |
|------|----------------|------|
| **TaskExecutionService** | getWorkDir, getExchangeDir, getInputDirectory；fallback 用 GlobalConfig.getExtract().getWorkDir() | 取的是作业/全局 work_dir、exchange、input_dir，正确 |
| **KingbaseExtractPlugin** | getJob, getExtractDatabase, getWorkDir, getRuntime, getExtractTasks | 正确 |
| **KingbaseLoadPlugin** | getJob, getTargetDatabase, getWorkDir, getLoadTasks | 正确 |
| **FileExtractPlugin** | getJob, getExtractDirectory, getWorkDir, getExtractTasks（含 task.getAttribute()） | 正确；attribute 由 tasks[].files 转换 |
| **FileLoadPlugin** | getJob, getTargetDirectory, getWorkDir | 正确 |
| **ManualExportService** | getExtractTasks, getLoadTasks, getExtractDatabase, getRuntime 等；并 set 到 copy 的**旧字段** | 正确；copy 只设旧字段，后续 getter 仍从旧字段读 |
| **JobConfigService** | getJob, getExtractDatabase, getTargetDatabase, getRuntime（合并时补 database_version、table_concurrency） | 正确 |
| **KelXxlJobHandler / KelApplication** | getJob().getType() | 正确 |
| **DiskSpaceChecker** | 入参为 GlobalConfig，不用 JobConfig | 无影响 |
| **FileNamingService** | 入参为 FileNamingConfig（来自 GlobalConfig），不用 JobConfig | 无影响 |

## ManualExportService 的 copy 逻辑

`buildSingleTableJobConfig` / `buildSingleTableLoadConfig` 对 copy 只做 `setExtractDatabase(...)`、`setWorkDir(...)`、`setExtractTasks(...)` 等，即只写**旧字段**。copy 未设置 `settings`/`resources`/`tasks`，因此后续 `getExtractDatabase()` 等会走“新结构为空则用旧字段”的分支，取到已 set 的值，行为正确。

## 测试建议

- 运行 `kel-server` 下插件与 TaskExecutionService 相关单测。
- 启动应用，用库内已更新为新格式的 4 条作业执行一次卸载/加载，确认 work_dir、exchange、input_directory、表与映射均按配置生效。
