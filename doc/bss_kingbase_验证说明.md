# BSS 金仓卸载/加载验证（PostgreSQL）

在 kel 库中创建 bss schema，用 bss_kingbase_extract / bss_kingbase_load 验证金仓（当前用 PostgreSQL 驱动）卸载与加载流程。

## 1. 执行 SQL（按顺序）

```bash
cd <项目根目录>

# 创建 bss schema 及表、初始数据
PGPASSWORD=kel psql -h 127.0.0.1 -p 5432 -U kel -d kel -f doc/init_bss_schema.sql

# 写入卸载/加载作业配置到 kel.job_config
PGPASSWORD=kel psql -h 127.0.0.1 -p 5432 -U kel -d kel -f doc/update_bss_kingbase_extract_config.sql
PGPASSWORD=kel psql -h 127.0.0.1 -p 5432 -U kel -d kel -f doc/update_bss_kingbase_load_config.sql
```

## 2. 验证步骤

1. **启动 Kel**（若未启动）：  
   `cd kel && java -jar kel-start/target/kel-start-0.0.1-SNAPSHOT.jar`

2. **卸载**：打开管控台 → 作业执行 → 选择 `bss_kingbase_extract` → 执行。  
   成功后在「任务管理」中该任务状态为 SUCCESS，批次号形如 `20260130_NNN`。

3. **加载**：作业执行 → 选择 `bss_kingbase_load` → 源批次号可不填（自动取 `exchange/bss_kingbase_extract` 下最新批次）或填写上一步的批次号 → 执行。  
   成功后在任务管理中为 SUCCESS。

## 3. 说明

- **bss schema**：含 `bss.orders`、`bss.customers` 两张表及少量初始数据；卸载导出为 COPY 文本（分隔符 `\x1E`），打包到 `exchange/bss_kingbase_extract/<批次号>/`。
- **卸载配置**：`bss_kingbase_extract` 连接 kel 库，导出 `bss.orders`、`bss.customers`；`runtime.table_concurrency: 1` 为串行导出（避免并发导出失败）。
- **加载配置**：`bss_kingbase_load` 从 `input_directory: exchange/bss_kingbase_extract` 解包后 COPY 入 kel 库的 bss 表；`load_tasks` 为 TRUNCATE_LOAD，先清表再加载。
- **JobConfig**：`target_database` 已用 `@JsonProperty("target_database")` 绑定 YAML 的 `target_database` 键。

若卸载报「并发导出失败」，确认 DB 中 `bss_kingbase_extract` 的 `runtime.table_concurrency` 为 1 后重试。
