-- 任务执行表
CREATE TABLE IF NOT EXISTS task_execution (
    id BIGSERIAL PRIMARY KEY,
    job_code VARCHAR(100) NOT NULL,
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
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_task_execution_job_code ON task_execution(job_code);
CREATE INDEX IF NOT EXISTS idx_task_execution_batch_number ON task_execution(batch_number);
CREATE INDEX IF NOT EXISTS idx_task_execution_status ON task_execution(status);
CREATE INDEX IF NOT EXISTS idx_task_execution_created_at ON task_execution(created_at);

-- 任务执行日志表
CREATE TABLE IF NOT EXISTS task_execution_log (
    id BIGSERIAL PRIMARY KEY,
    task_id BIGINT NOT NULL,
    log_level VARCHAR(10) NOT NULL,
    stage VARCHAR(50),
    message TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (task_id) REFERENCES task_execution(id) ON DELETE CASCADE
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_task_execution_log_task_id ON task_execution_log(task_id);
CREATE INDEX IF NOT EXISTS idx_task_execution_log_created_at ON task_execution_log(created_at);

-- 任务执行统计表
CREATE TABLE IF NOT EXISTS task_execution_stats (
    id BIGSERIAL PRIMARY KEY,
    task_id BIGINT NOT NULL,
    stat_type VARCHAR(50) NOT NULL,
    stat_name VARCHAR(100) NOT NULL,
    stat_value BIGINT,
    stat_value_str VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (task_id) REFERENCES task_execution(id) ON DELETE CASCADE
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_task_execution_stats_task_id ON task_execution_stats(task_id);
CREATE INDEX IF NOT EXISTS idx_task_execution_stats_type ON task_execution_stats(stat_type);

-- 更新时间触发器函数
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- 创建触发器
DROP TRIGGER IF EXISTS update_task_execution_updated_at ON task_execution;
CREATE TRIGGER update_task_execution_updated_at
    BEFORE UPDATE ON task_execution
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
