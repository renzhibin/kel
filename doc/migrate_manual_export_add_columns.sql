-- 为已有 kel.manual_export 表增加 type、source_batch 列（表级导出/加载功能需要）。
-- 若库是新建的且已执行 schema.sql，可跳过。仅对升级前已存在的 manual_export 表执行一次即可。
-- PostgreSQL 9.5+ 支持 ADD COLUMN IF NOT EXISTS。

ALTER TABLE kel.manual_export ADD COLUMN IF NOT EXISTS type VARCHAR(20) NOT NULL DEFAULT 'EXPORT';
ALTER TABLE kel.manual_export ADD COLUMN IF NOT EXISTS source_batch VARCHAR(50);

CREATE INDEX IF NOT EXISTS idx_manual_export_type ON kel.manual_export(type);
