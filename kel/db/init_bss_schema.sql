-- 在 kel 库中创建 bss schema 及示例表，用于金仓卸载/加载验证。
-- 执行: PGPASSWORD=kel psql -h 127.0.0.1 -p 5432 -U kel -d kel -f doc/init_bss_schema.sql

CREATE SCHEMA IF NOT EXISTS bss;

-- 订单表（与 COPY 导出格式一致：列顺序与类型）
DROP TABLE IF EXISTS bss.orders;
CREATE TABLE bss.orders (
    id     INTEGER PRIMARY KEY,
    name   TEXT,
    amount NUMERIC
);

-- 客户表
DROP TABLE IF EXISTS bss.customers;
CREATE TABLE bss.customers (
    id         INTEGER PRIMARY KEY,
    code       TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 初始数据
INSERT INTO bss.orders (id, name, amount) VALUES
    (1, '订单A', 100.50),
    (2, '订单B', 200.00),
    (3, '订单C', 300.25);

INSERT INTO bss.customers (id, code, created_at) VALUES
    (1, 'C001', CURRENT_TIMESTAMP),
    (2, 'C002', CURRENT_TIMESTAMP),
    (3, 'C003', CURRENT_TIMESTAMP);
