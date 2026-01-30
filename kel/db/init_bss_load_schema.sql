-- 在 kel 库中创建 bss_load schema 及空表，作为加载目标端（数据由 bss_kingbase_load 从包导入）。
-- 执行: PGPASSWORD=kel psql -h 127.0.0.1 -p 5432 -U kel -d kel -f doc/init_bss_load_schema.sql

CREATE SCHEMA IF NOT EXISTS bss_load;

-- 订单表（与 bss.orders 结构一致，供 COPY 导入）
DROP TABLE IF EXISTS bss_load.orders;
CREATE TABLE bss_load.orders (
    id     INTEGER PRIMARY KEY,
    name   TEXT,
    amount NUMERIC
);

-- 客户表（与 bss.customers 结构一致，供 COPY 导入）
DROP TABLE IF EXISTS bss_load.customers;
CREATE TABLE bss_load.customers (
    id         INTEGER PRIMARY KEY,
    code       TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
