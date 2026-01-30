-- Kel 元数据库 schema：实例名 kel，所有表置于 kel schema 下。
CREATE SCHEMA IF NOT EXISTS kel;

-- 任务执行表（含执行日志：execution_log 合并原 task_execution_log）
CREATE TABLE IF NOT EXISTS kel.task_execution (
    id BIGSERIAL PRIMARY KEY,
    job_name VARCHAR(100) NOT NULL,
    batch_number VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL,
    node_name VARCHAR(100),
    config_snapshot TEXT,
    progress INTEGER DEFAULT 0,
    current_stage VARCHAR(50),
    error_message TEXT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    statistics TEXT,
    execution_log JSONB DEFAULT '[]',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_task_execution_job_name ON kel.task_execution(job_name);
CREATE INDEX IF NOT EXISTS idx_task_execution_batch_number ON kel.task_execution(batch_number);
CREATE INDEX IF NOT EXISTS idx_task_execution_status ON kel.task_execution(status);
CREATE INDEX IF NOT EXISTS idx_task_execution_created_at ON kel.task_execution(created_at);

-- 任务执行统计表
CREATE TABLE IF NOT EXISTS kel.task_execution_stats (
    id BIGSERIAL PRIMARY KEY,
    task_id BIGINT NOT NULL,
    stat_type VARCHAR(50) NOT NULL,
    stat_name VARCHAR(100) NOT NULL,
    stat_value BIGINT,
    stat_value_str VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (task_id) REFERENCES kel.task_execution(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_task_execution_stats_task_id ON kel.task_execution_stats(task_id);
CREATE INDEX IF NOT EXISTS idx_task_execution_stats_type ON kel.task_execution_stats(stat_type);

-- 配置表（全局与作业 YAML，配置仅从 DB 加载）
CREATE TABLE IF NOT EXISTS kel.job_config (
    id BIGSERIAL PRIMARY KEY,
    job_name VARCHAR(100) NOT NULL UNIQUE,
    content_yaml TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_job_config_job_name ON kel.job_config(job_name);

-- 人工表级导出记录
CREATE TABLE IF NOT EXISTS kel.manual_export (
    id BIGSERIAL PRIMARY KEY,
    job_name VARCHAR(100) NOT NULL,
    table_name VARCHAR(200) NOT NULL,
    mode VARCHAR(20) NOT NULL,
    status VARCHAR(20) NOT NULL,
    task_id BIGINT,
    requested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    requested_by VARCHAR(100),
    FOREIGN KEY (task_id) REFERENCES kel.task_execution(id) ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS idx_manual_export_job_name ON kel.manual_export(job_name);
CREATE INDEX IF NOT EXISTS idx_manual_export_table_name ON kel.manual_export(table_name);
CREATE INDEX IF NOT EXISTS idx_manual_export_status ON kel.manual_export(status);
CREATE INDEX IF NOT EXISTS idx_manual_export_requested_at ON kel.manual_export(requested_at);

-- 更新时间触发器函数（放在 public 以便复用）
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

DROP TRIGGER IF EXISTS update_task_execution_updated_at ON kel.task_execution;
CREATE TRIGGER update_task_execution_updated_at
    BEFORE UPDATE ON kel.task_execution
    FOR EACH ROW
    EXECUTE PROCEDURE update_updated_at_column();

DROP TRIGGER IF EXISTS update_job_config_updated_at ON kel.job_config;
CREATE TRIGGER update_job_config_updated_at
    BEFORE UPDATE ON kel.job_config
    FOR EACH ROW
    EXECUTE PROCEDURE update_updated_at_column();
