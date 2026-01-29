-- 本地 PostgreSQL 测试数据初始化脚本
-- 使用方法（在本机已安装 psql 且有 postgres 用户的前提下）：
--   cd kel
--   psql -h 127.0.0.1 -U postgres -d postgres -f testdata/pg/demo_kingbase.sql

DROP TABLE IF EXISTS public.demo_kingbase;

CREATE TABLE public.demo_kingbase (
    id   SERIAL PRIMARY KEY,
    name TEXT,
    amount NUMERIC
);

INSERT INTO public.demo_kingbase (name, amount) VALUES
    ('Alice', 100.00),
    ('Bob',   200.50),
    ('Carol', 300.75);

