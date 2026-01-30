# KEL 配置文件说明

本文档说明全局配置与作业配置的**逻辑顺序、必填项与可复用全局项**，覆盖四种作业类型。同一语义的配置项在全局与作业中**同名**，作业不填时可复用全局默认值。

---

## 一、全局配置（global.yaml）

全局配置提供环境级默认值，所有作业共享。以下按**阅读与定义顺序**列出各块。

### 1.1 extract（与卸载/加载流程相关的默认值）

| 参数名 | 含义 | 必填 | 作业可复用 |
|--------|------|------|------------|
| work_dir | 工作目录根路径 | 否 | 是，作业不填则用此值 |
| batch_number_format | 批次号格式，如 `{yyyyMMdd}_{seq}` | 否 | 否，作业一般不覆盖 |
| encoding | 文件编码，如 UTF-8 | 否 | 否 |
| database_version | 数据库版本默认值，如 V8R6 | 否 | 是，作业连接块不填则用此值 |
| cleanup_work_dir | 完成后是否清理工作目录 | 否 | 否 |

### 1.2 concurrency

| 参数名 | 含义 | 必填 | 作业可复用 |
|--------|------|------|------------|
| default_table_concurrency | 默认表级并发数 | 否 | 是，作业 runtime.table_concurrency 不填则用此值 |

### 1.3 retry

| 参数名 | 含义 | 必填 | 作业可复用 |
|--------|------|------|------------|
| max_retries | 最大重试次数 | 否 | 否 |
| retry_interval_sec | 重试间隔（秒） | 否 | 否 |

### 1.4 compression / security / file_naming / disk_protection

- **compression**：algorithm、split_threshold_gb 等，作业不单独覆盖。
- **security**：sm4_key、enable_encryption，作业不单独覆盖。
- **file_naming**：文件命名规则（系统标识、接口映射等），作业不单独覆盖。
- **disk_protection**：enabled、min_free_space_gb 等，作业不单独覆盖。

---

## 二、作业配置（按定义顺序）

作业 YAML 建议按以下顺序书写：先身份，再连接，再路径，再任务，最后运行时。

### 2.1 作业元信息（job）

| 参数名 | 含义 | 必填 | 可复用全局 |
|--------|------|------|------------|
| job.type | 作业类型：EXTRACT_KINGBASE / FILE_EXTRACT / KINGBASE_LOAD / FILE_LOAD | **是** | 否 |
| job.name | 作业名称，用于日志与目录 | **是** | 否 |
| job.description | 作业描述 | 否 | 否 |

### 2.2 连接信息（extract_database / target_database）

连接块内同一语义与全局**同名**，如数据库版本用 `database_version`（不填则用全局 extract.database_version）。

**extract_database**（卸载侧库连接，仅 Kingbase 卸载作业使用）

| 参数名 | 含义 | 必填 | 可复用全局 |
|--------|------|------|------------|
| host | 主机 | **是**（当使用该块时） | 否 |
| port | 端口 | **是** | 否 |
| name | 数据库名 | **是** | 否 |
| user | 用户名 | **是** | 否 |
| password | 密码 | **是** | 否 |
| database_version | 数据库版本，如 V8R6 | 否 | **是**，不填则用 global.extract.database_version |

**target_database**（加载侧库连接，仅 Kingbase 加载作业使用）

| 参数名 | 含义 | 必填 | 可复用全局 |
|--------|------|------|------------|
| host / port / name / user / password | 同上 | **是**（当使用该块时） | 否 |
| database_version | 同上 | 否 | **是**，不填则用 global.extract.database_version |

### 2.3 路径

| 参数名 | 含义 | 必填 | 可复用全局 | 适用作业类型 |
|--------|------|------|------------|--------------|
| work_dir | 本作业工作目录 | 否 | **是**，不填则用 global.extract.work_dir | 所有 |
| exchange_dir | 交换目录（若使用） | 否 | 否 | 按需 |
| extract_directory | 文件采集源目录 | **是**（FILE_EXTRACT） | 否 | 仅 FILE_EXTRACT |
| input_directory | 加载时输入根目录（用于扫描批次） | **是**（加载作业） | 否 | KINGBASE_LOAD、FILE_LOAD |
| target_directory | 文件加载目标目录 | **是**（FILE_LOAD） | 否 | 仅 FILE_LOAD |

### 2.4 任务（extract_tasks / load_tasks）

**extract_tasks**（卸载任务，仅卸载类作业使用）

| 项 | 含义 | 必填 | 适用类型 |
|----|------|------|----------|
| type | FULL 全量 / INCREMENTAL 增量 | 否，默认 FULL | EXTRACT_KINGBASE |
| tables | 全量表名列表 | 全量时必填 | EXTRACT_KINGBASE |
| sql_list | 增量 SQL 配置列表 | 增量时必填 | EXTRACT_KINGBASE |
| attribute | 文件采集属性（file_pattern、time_type、time_range、file_size_limit_mb 等） | 按需 | FILE_EXTRACT |

**load_tasks**（加载任务，仅加载类作业使用）

| 项 | 含义 | 必填 | 适用类型 |
|----|------|------|----------|
| type | TRUNCATE_LOAD / APPEND / MERGE | 否，默认 APPEND | KINGBASE_LOAD |
| interface_mapping | 源接口/表名到目标表名的映射 | **是** | KINGBASE_LOAD |
| enable_transaction | 是否启用事务 | 否 | KINGBASE_LOAD |

### 2.5 运行时（runtime）

| 参数名 | 含义 | 必填 | 可复用全局 |
|--------|------|------|------------|
| runtime.table_concurrency | 本作业表级并发数 | 否 | **是**，不填则用 global.concurrency.default_table_concurrency |

---

## 三、按作业类型的必填与可复用速查

| 类型 | 作业必填 | 可复用全局 | 不适用（无需配置） |
|------|----------|------------|--------------------|
| **EXTRACT_KINGBASE** | job.type、job.name；extract_database（host、port、name、user、password）；extract_tasks | work_dir、extract_database.database_version、runtime.table_concurrency | target_database、load_tasks、input_directory、target_directory、extract_directory |
| **FILE_EXTRACT** | job.type、job.name；extract_directory；extract_tasks | work_dir、runtime.table_concurrency | extract_database、target_database、load_tasks、input_directory、target_directory |
| **KINGBASE_LOAD** | job.type、job.name；target_database（host、port、name、user、password）；load_tasks；input_directory | work_dir、target_database.database_version、runtime.table_concurrency | extract_database、extract_tasks、extract_directory、target_directory |
| **FILE_LOAD** | job.type、job.name；target_directory；input_directory | work_dir、runtime.table_concurrency | extract_database、target_database、extract_tasks、extract_directory |

---

## 四、合并规则小结

- **work_dir**：作业不填则用 global.extract.work_dir。
- **database_version**：作业连接块（extract_database / target_database）不填则用 global.extract.database_version；全局与作业键名同名。
- **table_concurrency**：作业 runtime.table_concurrency 不填则用 global.concurrency.default_table_concurrency。
- 其余项：全局与作业各读各的，不做自动合并。

---

## 五、YAML 命名与风格

- 键名统一 **snake_case**（如 batch_number_format、extract_database、input_directory、database_version）。
- 同一概念在全局与作业中**同名**（如 database_version），便于合并与理解。
