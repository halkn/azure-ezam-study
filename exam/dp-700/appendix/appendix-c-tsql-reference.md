# 付録 C：T-SQL クイックリファレンス（Fabric Warehouse）

> Fabric Warehouse 固有の制約と Snowflake SQL との差異に注目

---

## C.1 DDL

```sql
-- テーブル作成
CREATE TABLE dbo.dim_customer (
    customer_sk   BIGINT IDENTITY(1,1) NOT NULL,   -- 自動採番（SEQUENCE は非サポート）
    customer_id   INT NOT NULL,
    customer_name NVARCHAR(200),
    email         NVARCHAR(200) MASKED WITH (FUNCTION = 'email()'),  -- DDM
    region        NVARCHAR(50),
    is_current    BIT DEFAULT 1,
    valid_from    DATETIME2 DEFAULT GETDATE(),
    valid_to      DATETIME2
);

-- ビュー
CREATE VIEW dbo.vw_active_customers AS
SELECT customer_id, customer_name, region
FROM dbo.dim_customer WHERE is_current = 1;

-- テーブルのクローン（ゼロコピー）
CREATE TABLE dbo.dim_customer_backup AS CLONE OF dbo.dim_customer;
CREATE TABLE dbo.dim_customer_snapshot AS CLONE OF dbo.dim_customer AT '2024-06-15T10:00:00Z';

-- スキーマ作成
CREATE SCHEMA staging;
CREATE SCHEMA gold;

-- テーブル削除 / 切り捨て
DROP TABLE IF EXISTS dbo.stg_temp;
TRUNCATE TABLE dbo.stg_temp;

-- 列追加 / 名前変更
ALTER TABLE dbo.dim_customer ADD phone NVARCHAR(20);
EXEC sp_rename 'dbo.dim_customer.phone', 'phone_number', 'COLUMN';
```

---

## C.2 DML

```sql
-- INSERT
INSERT INTO dbo.dim_customer (customer_id, customer_name, email, region)
VALUES (1001, 'Tanaka Taro', 'tanaka@example.com', 'East');

-- INSERT SELECT
INSERT INTO dbo.silver_orders
SELECT order_id, customer_id, amount, order_date
FROM dbo.bronze_orders
WHERE order_date >= '2024-01-01';

-- UPDATE
UPDATE dbo.dim_customer
SET email = 'new@example.com', region = 'West'
WHERE customer_id = 1001;

-- DELETE
DELETE FROM dbo.stg_orders WHERE load_date < '2024-01-01';

-- MERGE（upsert + 削除）
MERGE INTO dbo.dim_customer AS tgt
USING dbo.stg_customer AS src ON tgt.customer_id = src.customer_id
WHEN MATCHED AND (tgt.customer_name <> src.customer_name OR tgt.email <> src.email) THEN
    UPDATE SET tgt.customer_name = src.customer_name, tgt.email = src.email
WHEN NOT MATCHED BY TARGET THEN
    INSERT (customer_id, customer_name, email, region)
    VALUES (src.customer_id, src.customer_name, src.email, src.region)
WHEN NOT MATCHED BY SOURCE THEN
    DELETE;

-- COPY INTO
COPY INTO dbo.fact_orders
FROM 'abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>/Files/raw/orders/*.parquet'
WITH (FILE_TYPE = 'PARQUET');

COPY INTO dbo.staging_data
FROM 'abfss://.../<lakehouse>/Files/raw/data.csv'
WITH (FILE_TYPE = 'CSV', FIRSTROW = 2, FIELDTERMINATOR = ',', FIELDQUOTE = '"', MAXERRORS = 100);
```

---

## C.3 クエリ

```sql
-- CTE
WITH monthly AS (
    SELECT YEAR(order_date) AS yr, MONTH(order_date) AS mo, SUM(amount) AS total
    FROM dbo.fact_orders GROUP BY YEAR(order_date), MONTH(order_date)
),
ranked AS (
    SELECT *, RANK() OVER (PARTITION BY yr ORDER BY total DESC) AS rnk FROM monthly
)
SELECT * FROM ranked WHERE rnk <= 3;

-- ウィンドウ関数
SELECT order_id, order_date, amount,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS rn,
    LAG(amount, 1) OVER (ORDER BY order_date) AS prev_amount,
    SUM(amount) OVER (ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total,
    AVG(amount) OVER (ORDER BY order_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS moving_avg_7d,
    NTILE(4) OVER (ORDER BY amount DESC) AS quartile
FROM dbo.fact_orders;

-- 重複排除（QUALIFY 非サポートのため CTE で代替）
WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY modified_date DESC) AS rn
    FROM dbo.stg_orders
)
SELECT * FROM ranked WHERE rn = 1;

-- GROUPING SETS / ROLLUP / PIVOT
SELECT COALESCE(region,'ALL') AS region, COALESCE(category,'ALL') AS category, SUM(amount) AS total
FROM dbo.fact_orders
GROUP BY GROUPING SETS ((region,category),(region),(category),());

SELECT * FROM (SELECT region, category, amount FROM dbo.fact_orders) AS src
PIVOT (SUM(amount) FOR category IN ([Electronics],[Clothing],[Food])) AS pvt;

-- STRING_AGG
SELECT customer_id, STRING_AGG(product_name, ', ') WITHIN GROUP (ORDER BY order_date) AS products
FROM dbo.fact_orders f JOIN dbo.dim_product p ON f.product_id = p.product_id
GROUP BY customer_id;

-- クロスデータベースクエリ（同一ワークスペース内）
SELECT w.*, l.category
FROM [MyWarehouse].[dbo].[fact_orders] AS w
JOIN [MyLakehouse].[dbo].[dim_product] AS l ON w.product_id = l.product_id;
```

---

## C.4 セキュリティ

```sql
-- RLS
CREATE FUNCTION dbo.fn_rls(@region NVARCHAR(50))
RETURNS TABLE WITH SCHEMABINDING
AS RETURN SELECT 1 AS r WHERE @region = USER_NAME() OR USER_NAME() = 'admin@contoso.com';
CREATE SECURITY POLICY dbo.RegionPolicy
ADD FILTER PREDICATE dbo.fn_rls(region) ON dbo.fact_orders WITH (STATE = ON);

-- CLS
GRANT SELECT ON dbo.dim_customer (customer_id, customer_name, region) TO [analyst@contoso.com];
DENY SELECT ON dbo.dim_customer (ssn, phone) TO [analyst@contoso.com];

-- DDM
ALTER TABLE dbo.employees ALTER COLUMN email ADD MASKED WITH (FUNCTION = 'email()');
ALTER TABLE dbo.employees ALTER COLUMN phone ADD MASKED WITH (FUNCTION = 'partial(0,"XXX-",4)');
ALTER TABLE dbo.employees ALTER COLUMN salary ADD MASKED WITH (FUNCTION = 'default()');
GRANT UNMASK ON dbo.employees TO [hr@contoso.com];
REVOKE UNMASK ON dbo.employees FROM [hr@contoso.com];

-- オブジェクトレベル
GRANT SELECT ON SCHEMA::gold TO [bi_team@contoso.com];
DENY SELECT ON SCHEMA::staging TO [bi_team@contoso.com];
```

---

## C.5 ストアドプロシージャ

```sql
CREATE PROCEDURE dbo.usp_incremental_load
    @load_date DATE, @load_type NVARCHAR(20) = 'incremental'
AS BEGIN
    IF @load_type = 'full'
        BEGIN TRUNCATE TABLE dbo.silver_orders;
              INSERT INTO dbo.silver_orders SELECT * FROM dbo.bronze_orders; END
    ELSE
        BEGIN
            MERGE INTO dbo.silver_orders AS t
            USING (SELECT * FROM dbo.bronze_orders WHERE modified_date >= @load_date) AS s
            ON t.order_id = s.order_id
            WHEN MATCHED THEN UPDATE SET t.amount=s.amount, t.modified_date=s.modified_date
            WHEN NOT MATCHED THEN INSERT (order_id,customer_id,amount,order_date,modified_date)
                VALUES (s.order_id,s.customer_id,s.amount,s.order_date,s.modified_date);
        END
END;
-- パイプラインから: Stored Procedure Activity → dbo.usp_incremental_load → @load_date = @pipeline().TriggerTime
```

---

## C.6 DMV（監視）

```sql
SELECT * FROM sys.dm_exec_requests WHERE status = 'running';            -- 実行中クエリ
SELECT * FROM sys.dm_exec_requests_history ORDER BY submit_time DESC;   -- クエリ履歴
SELECT * FROM sys.dm_exec_sessions WHERE is_user_process = 1;           -- セッション
SELECT * FROM sys.dm_exec_connections;                                   -- 接続
```

---

## C.7 Fabric Warehouse の未サポート構文

| 未サポート | 代替 |
|-----------|------|
| `CREATE INDEX` | 不要（自動最適化） |
| `CURSOR` | セットベースの操作に書き換え |
| `TRIGGER` | パイプライン / Activator で代替 |
| `SEQUENCE` | `IDENTITY` |
| `QUALIFY` | CTE + `ROW_NUMBER` + `WHERE` |
| `EXECUTE AS` | Entra ID ベースの認証 |
| `SYNONYM` | クロスデータベースクエリで直接参照 |
| `VARIANT` / `PARSE_JSON` | Lakehouse (Spark) で処理 |
| `ILIKE` | `LOWER(col) LIKE '%text%'` |
| `LATERAL FLATTEN` | Spark の `explode` |
