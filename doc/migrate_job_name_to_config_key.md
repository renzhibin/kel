# 存量配置迁移：job.name 与 config_key 对齐

## 背景

约定：`job.name` 全局唯一，作为作业配置的唯一标识；数据库 `job_config.config_key` 与 YAML 内 `job.name` 一致。

若存量数据中存在 `config_key` 与 YAML 内 `job.name` 不一致的记录，需执行一次迁移。

## 迁移策略

对每条作业配置（不含 `__global__`）：

- 若 YAML 内 `job.name` 为空或与当前 `config_key` 不同，则将 `job.name` 设为当前 `config_key` 并回写 YAML。
- 不修改 `config_key`，仅统一 YAML 中的 `job.name`，避免主键冲突与重复 key 风险。

## 执行方式

**HTTP 接口**（应用运行中）：

```http
POST /api/jobs/admin/migrate-job-name
```

响应示例：

```json
{ "message": "迁移完成", "updated": 2 }
```

`updated` 为被更新的配置条数。

## 执行时机

- 升级到「job.name 即 key」版本后，若库中已有历史作业配置，建议执行一次。
- 执行一次即可；迁移为幂等（已一致的记录不会重复写入）。
