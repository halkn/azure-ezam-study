# 第6章 Data Warehouse

> 📘 対応 MS Learn モジュール:
> - Get started with data warehouses in Microsoft Fabric
> - Load data into a Microsoft Fabric data warehouse
> - Query a data warehouse in Microsoft Fabric
> - Monitor a Microsoft Fabric data warehouse
> - Secure a Microsoft Fabric data warehouse
>
> 試験タグ: `[取込変換]` `[監視最適化]` `[実装管理]`

---

## 6.1 Fabric Warehouse の特性

### 6.1.1 Warehouse vs Lakehouse SQL 分析エンドポイント

| 観点 | Warehouse | Lakehouse SQL 分析エンドポイント |
|------|----------|-------------------------------|
| DML (INSERT/UPDATE/DELETE/MERGE) | **○** | × (読み取り専用) |
| DDL (CREATE TABLE/VIEW/SP) | **○** | △ (VIEW, SP は作成可) |
| ストアドプロシージャ | **○** | ○ |
| 関数 (TVF) | **○** | ○ |
| IDENTITY 列 | **○** | × |
| RLS / CLS / DDM | **○ (ネイティブ)** | △ (SQL ポリシー経由) |
| トランザクション | **○ (ACID)** | × |
| #temp テーブル | **○ (セッションスコープ)** | × |
| データ格納形式 | Delta Parquet (OneLake) | Delta Parquet (OneLake) |
| クロスデータベースクエリ | **○ (同一ワークスペース内)** | ○ |

### 6.1.2 Snowflake Warehouse との対比

| 観点 | Snowflake Warehouse | Fabric Warehouse |
|------|-------------------|-----------------|
| コンピュートモデル | Warehouse ごとに独立クラスタ | 容量（CU）プールを全ワークロードで共有 |
| スケーリング | サイズ変更 + マルチクラスタ | CU の自動バースト + スロットリング |
| 課金 | クレジット（コンピュート時間） | CU 秒（全ワークロード共有） |
| トランザクション | ACID | ACID (Delta Lake ベース) |
| Time Travel | UNDROP / AT (TIMESTAMP) | Delta Lake のタイムトラベル |
| CLONE | ゼロコピークローン | ゼロコピークローン（`CREATE TABLE ... AS CLONE OF`） |
| セミ構造化データ | VARIANT 型 | 非サポート（Lakehouse で処理） |
| QUALIFY 句 | **○** | **×** (CTE + ROW_NUMBER で代替) |

**試験ポイント**: Fabric Warehouse は QUALIFY をサポートしない。Snowflake で `QUALIFY ROW_NUMBER() OVER (...) = 1` としていた重複排除は CTE で書き換える必要がある。

### 6.1.3 未サポートの T-SQL 機能

試験でも問われる主な未サポート構文:

- `CREATE INDEX` / インデックス全般
- カーソル（`DECLARE CURSOR`）
- `EXECUTE AS` / セキュリティコンテキストの切り替え
- `TRIGGER`
- `SYNONYM`
- `SEQUENCE`（IDENTITY で代替）
- ネストされた CTE（プレビュー機能）
- 一部のシステム関数

---

## 6.2 データロード `[取込変換]`

### 6.2.1 COPY INTO

大量データの高速ロードに最適。OneLake / ADLS Gen2 / S3 からロード可能。

```sql
-- 基本的な COPY INTO
COPY INTO dbo.fact_orders
FROM 'https://onelake.dfs.fabric.microsoft.com/<workspace>/<lakehouse>/Files/raw/orders/'
WITH (
    FILE_TYPE = 'PARQUET'
);

-- CSV からのロード（オプション付き）
COPY INTO dbo.staging_customers
FROM 'https://onelake.dfs.fabric.microsoft.com/<workspace>/<lakehouse>/Files/raw/customers.csv'
WITH (
    FILE_TYPE = 'CSV',
    FIRSTROW = 2,                -- ヘッダースキップ
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    FIELDQUOTE = '"',
    ENCODING = 'UTF8',
    MAXERRORS = 100              -- 許容エラー行数
);

-- OneLake パスを使った Lakehouse からの直接ロード
COPY INTO dbo.fact_orders
FROM 'abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>/Tables/silver_orders/'
WITH (
    FILE_TYPE = 'PARQUET'
);

-- ワイルドカードで複数ファイルをロード
COPY INTO dbo.fact_orders
FROM 'https://onelake.dfs.fabric.microsoft.com/.../raw/orders/2024/01/*.parquet'
WITH (
    FILE_TYPE = 'PARQUET'
);
```

**Snowflake の COPY INTO との主な差異**:

| 観点 | Snowflake | Fabric Warehouse |
|------|----------|-----------------|
| ステージ | 内部/外部ステージを指定 | OneLake パスまたは URL を直接指定 |
| FILE_FORMAT | 名前付きフォーマット | WITH 句で個別指定 |
| ON_ERROR | CONTINUE / SKIP_FILE / ABORT | MAXERRORS で許容数指定 |
| VALIDATION_MODE | ○ | × |
| PATTERN | ○ | ワイルドカード（`*.parquet`） |

### 6.2.2 パイプラインからのロード

Copy アクティビティで直接 Warehouse テーブルに書き込む:

```
Pipeline:
  Copy Activity:
    Source: Azure SQL DB / Lakehouse / Blob Storage
    Sink: Warehouse テーブル
    Write behavior: Insert / Upsert (MERGE)
    Pre-copy script: TRUNCATE TABLE dbo.staging_customers; (全量洗い替え時)
```

Dataflow Gen2 でも Warehouse をデスティネーションに指定可能（第4章参照）。

### 6.2.3 増分ロード（MERGE）

```sql
-- ウォーターマークパターンによる増分ロード
-- Step 1: 前回の最大値を取得
DECLARE @last_watermark DATETIME2;
SELECT @last_watermark = COALESCE(MAX(modified_date), '1900-01-01')
FROM dbo.fact_orders;

-- Step 2: ステージングテーブルに新規/変更データをロード
COPY INTO dbo.stg_orders
FROM 'abfss://.../<lakehouse>/Tables/bronze_orders/'
WITH (FILE_TYPE = 'PARQUET');

-- Step 3: MERGE で upsert
MERGE INTO dbo.fact_orders AS tgt
USING (
    SELECT * FROM dbo.stg_orders
    WHERE modified_date > @last_watermark
) AS src
ON tgt.order_id = src.order_id
WHEN MATCHED AND tgt.modified_date < src.modified_date THEN
    UPDATE SET
        tgt.amount = src.amount,
        tgt.quantity = src.quantity,
        tgt.status = src.status,
        tgt.modified_date = src.modified_date
WHEN NOT MATCHED THEN
    INSERT (order_id, customer_id, amount, quantity, status, order_date, modified_date)
    VALUES (src.order_id, src.customer_id, src.amount, src.quantity, src.status,
            src.order_date, src.modified_date);

-- Step 4: ステージングテーブルをクリア
TRUNCATE TABLE dbo.stg_orders;
```

**MERGE 構文のポイント**:

```sql
MERGE INTO <target> AS tgt
USING <source> AS src
ON <join_condition>
WHEN MATCHED [AND <condition>] THEN
    UPDATE SET ...
WHEN NOT MATCHED [BY TARGET] THEN
    INSERT (...) VALUES (...)
WHEN NOT MATCHED BY SOURCE [AND <condition>] THEN
    DELETE;  -- ソースにないレコードを削除（オプション）
```

- `WHEN MATCHED`: 両方に存在 → 更新
- `WHEN NOT MATCHED [BY TARGET]`: ソースにのみ存在 → 挿入
- `WHEN NOT MATCHED BY SOURCE`: ターゲットにのみ存在 → 削除（SCD Type 4 等で使用）

---

## 6.3 T-SQL によるデータ変換 `[取込変換]`

### 6.3.1 CTE（共通テーブル式）

```sql
-- CTE の連鎖（Snowflake の WITH 句と同じ）
WITH daily_totals AS (
    SELECT
        order_date,
        region,
        SUM(amount) AS daily_revenue,
        COUNT(DISTINCT customer_id) AS unique_customers
    FROM dbo.fact_orders
    WHERE order_date >= '2024-01-01'
    GROUP BY order_date, region
),
ranked_regions AS (
    SELECT
        order_date,
        region,
        daily_revenue,
        unique_customers,
        RANK() OVER (PARTITION BY order_date ORDER BY daily_revenue DESC) AS revenue_rank
    FROM daily_totals
)
SELECT *
FROM ranked_regions
WHERE revenue_rank <= 3
ORDER BY order_date, revenue_rank;
```

### 6.3.2 ウィンドウ関数

```sql
-- ROW_NUMBER: 重複排除（QUALIFY の代替）
WITH ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY modified_date DESC
        ) AS rn
    FROM dbo.stg_orders
)
SELECT * FROM ranked WHERE rn = 1;

-- LAG / LEAD: 前後行の比較
SELECT
    order_date,
    daily_revenue,
    LAG(daily_revenue, 1) OVER (ORDER BY order_date) AS prev_day_revenue,
    daily_revenue - LAG(daily_revenue, 1) OVER (ORDER BY order_date) AS day_over_day_change,
    LEAD(daily_revenue, 1) OVER (ORDER BY order_date) AS next_day_revenue
FROM dbo.daily_sales;

-- 累積合計
SELECT
    order_date,
    amount,
    SUM(amount) OVER (
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total
FROM dbo.fact_orders;

-- 移動平均（直近7日間）
SELECT
    order_date,
    daily_revenue,
    AVG(daily_revenue) OVER (
        ORDER BY order_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS moving_avg_7d
FROM dbo.daily_sales;

-- NTILE: データの等分割
SELECT
    customer_id,
    total_spend,
    NTILE(4) OVER (ORDER BY total_spend DESC) AS spend_quartile
FROM dbo.customer_summary;

-- FIRST_VALUE / LAST_VALUE
SELECT
    region,
    order_date,
    amount,
    FIRST_VALUE(amount) OVER (
        PARTITION BY region ORDER BY order_date
    ) AS first_order_amount,
    LAST_VALUE(amount) OVER (
        PARTITION BY region ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS last_order_amount
FROM dbo.fact_orders;
```

### 6.3.3 ストアドプロシージャと関数

```sql
-- ストアドプロシージャの作成
CREATE PROCEDURE dbo.usp_load_silver_orders
    @load_date DATE,
    @load_type NVARCHAR(20) = 'incremental'
AS
BEGIN
    IF @load_type = 'full'
    BEGIN
        TRUNCATE TABLE dbo.silver_orders;

        INSERT INTO dbo.silver_orders
        SELECT * FROM dbo.bronze_orders;
    END
    ELSE
    BEGIN
        MERGE INTO dbo.silver_orders AS tgt
        USING (
            SELECT * FROM dbo.bronze_orders
            WHERE modified_date >= @load_date
        ) AS src
        ON tgt.order_id = src.order_id
        WHEN MATCHED THEN
            UPDATE SET
                tgt.amount = src.amount,
                tgt.status = src.status,
                tgt.modified_date = src.modified_date
        WHEN NOT MATCHED THEN
            INSERT (order_id, customer_id, amount, status, order_date, modified_date)
            VALUES (src.order_id, src.customer_id, src.amount, src.status,
                    src.order_date, src.modified_date);
    END
END;

-- パイプラインからの呼び出し
-- Stored Procedure アクティビティ → usp_load_silver_orders
--   パラメータ: @load_date = @formatDateTime(pipeline().TriggerTime, 'yyyy-MM-dd')
--              @load_type = @pipeline().parameters.load_type
```

```sql
-- テーブル値関数
CREATE FUNCTION dbo.fn_get_customer_orders(@customer_id INT)
RETURNS TABLE
AS
RETURN
(
    SELECT order_id, order_date, amount, status
    FROM dbo.fact_orders
    WHERE customer_id = @customer_id
);

-- 使用例
SELECT * FROM dbo.fn_get_customer_orders(1001);
```

### 6.3.4 データのグループ化と集計

```sql
-- GROUPING SETS（複数レベルの集計を一度に）
SELECT
    COALESCE(region, 'ALL') AS region,
    COALESCE(category, 'ALL') AS category,
    SUM(amount) AS total_revenue,
    COUNT(*) AS order_count
FROM dbo.fact_orders
GROUP BY GROUPING SETS (
    (region, category),     -- region × category
    (region),               -- region 小計
    (category),             -- category 小計
    ()                      -- 総計
);

-- ROLLUP（階層的な小計 + 総計）
SELECT region, category, SUM(amount) AS total_revenue
FROM dbo.fact_orders
GROUP BY ROLLUP (region, category);

-- PIVOT
SELECT *
FROM (
    SELECT region, category, amount
    FROM dbo.fact_orders
) AS src
PIVOT (
    SUM(amount)
    FOR category IN ([Electronics], [Clothing], [Food])
) AS pvt;

-- STRING_AGG（集約）
SELECT
    customer_id,
    STRING_AGG(product_name, ', ') WITHIN GROUP (ORDER BY order_date) AS ordered_products
FROM dbo.fact_orders o
JOIN dbo.dim_product p ON o.product_id = p.product_id
GROUP BY customer_id;
```

---

## 6.4 ミラーリング `[取込変換]`

### 6.4.1 概要

ミラーリングはソースデータベースの変更を CDC（Change Data Capture）で自動的に OneLake に複製する仕組み。ETL パイプラインが不要。

```
ソース DB                    Fabric
───────                    ──────
Azure SQL DB ──── CDC ────→ Mirrored Database (OneLake)
Cosmos DB    ──── CDC ────→   ├── Delta テーブル
Snowflake    ──── CDC ────→   └── SQL 分析エンドポイント（読み取り専用）
SQL Server   ──── CDC ────→
PostgreSQL   ──── CDC ────→
```

### 6.4.2 対応ソース（GA）

| ソース | CDC 方式 | 前提条件 |
|-------|---------|---------|
| **Azure SQL Database** | トランザクションログベース | vCore モデル推奨。DTU Basic/Standard <100 は非対応 |
| **Azure Cosmos DB** | 継続的バックアップ（Change Feed） | NoSQL API のみ。7日 or 30日の継続的バックアップ有効化 |
| **Snowflake** | Snowflake Streams (CDC) | Snowflake アカウントへの接続情報が必要 |
| **SQL Server 2016–2025** | トランザクションログベース | オンプレミス/Azure VM。OPDG が必要な場合あり |
| **PostgreSQL** | 論理レプリケーション | Azure Database for PostgreSQL Flexible Server |

### 6.4.3 ミラーリングの構成

```
1. Fabric ワークスペースで「Mirrored Database」を新規作成
2. ソースの種類を選択（Azure SQL DB, Cosmos DB 等）
3. 接続情報を入力（サーバー名, DB名, 認証）
4. ミラーリング対象を選択（全テーブル or 個別テーブル）
5. ミラーリング開始 → 初回スナップショット → CDC による継続同期
```

### 6.4.4 ミラーリング後のデータアクセス

ミラーリングされたデータは以下の方法でアクセス:

```sql
-- SQL 分析エンドポイント（読み取り専用）
SELECT TOP 100 * FROM dbo.orders;

-- クロスデータベースクエリ（同一ワークスペース内の Warehouse と結合）
SELECT m.order_id, m.amount, w.customer_name
FROM [MirroredDB].[dbo].[orders] AS m
JOIN [MyWarehouse].[dbo].[dim_customer] AS w
    ON m.customer_id = w.customer_id;
```

```python
# Spark ノートブックからミラーリングデータを読み取り
# ショートカット経由または直接パス指定で OneLake の Delta テーブルにアクセス
df = spark.read.format("delta").load(
    "abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<mirrored_db>/Tables/dbo/orders"
)
```

### 6.4.5 ミラーリング vs コピー vs ショートカット

| 観点 | ミラーリング | Pipeline コピー | ショートカット |
|------|-----------|----------------|-------------|
| データの鮮度 | **ほぼリアルタイム** | スケジュール依存 | リアルタイム |
| データの格納場所 | **OneLake** | OneLake | ソース側 |
| 対象ソース | RDBMS / NoSQL | ほぼ全て | オブジェクトストレージ |
| 変換 | なし（スキーマ変換のみ） | 可能 | なし |
| コンピュートコスト | **無料**（レプリケーション分） | CU 消費 | なし |
| ストレージコスト | SKU に応じた無料枠あり | OneLake ストレージ | ソース側のみ |

### 6.4.6 OneLake からの継続的インテグレーション

ミラーリングされた Delta テーブルを下流の Lakehouse / Warehouse から参照することで、Bronze レイヤーとしてメダリオンアーキテクチャに組み込める:

```
ミラーリング DB (自動 CDC) → ショートカット → Bronze Lakehouse
  → Notebook (変換) → Silver Lakehouse
    → Gold Warehouse / Lakehouse
```

---

## 6.5 Warehouse の最適化 `[監視最適化]`

### 6.5.1 統計情報

Fabric Warehouse は自動統計を管理するが、手動作成も可能。

```sql
-- 統計情報の手動作成（頻繁にクエリされる列に対して）
CREATE STATISTICS stat_orders_date ON dbo.fact_orders (order_date);
CREATE STATISTICS stat_orders_customer ON dbo.fact_orders (customer_id);

-- 複数列の統計
CREATE STATISTICS stat_orders_region_date ON dbo.fact_orders (region, order_date);
```

自動統計:
- **ヒストグラム統計**: 列値の分布
- **平均列長統計**: 列のサイズ
- **テーブルカーディナリティ統計**: 行数

### 6.5.2 データ圧縮と自動コンパクション

Fabric Warehouse はバックグラウンドで自動コンパクション（小さな Parquet ファイルの統合）を実行する。

- 書き込み後に自動的にトリガー
- ユーザークエリとの衝突を回避（プリエンプション機能）
- 手動での OPTIMIZE は不要（Lakehouse と異なる点）

### 6.5.3 ゼロコピークローン

```sql
-- テーブルのクローン（メタデータのみコピー、データは共有）
CREATE TABLE dbo.fact_orders_backup
AS CLONE OF dbo.fact_orders;

-- タイムトラベルでの特定時点のクローン
CREATE TABLE dbo.fact_orders_snapshot
AS CLONE OF dbo.fact_orders
AT '2024-06-15T10:00:00Z';
```

クローンはセキュリティ設定（RLS, CLS, DDM）も継承する。

### 6.5.4 Direct Lake モード（Power BI）

Warehouse のデータを Power BI で Direct Lake モードで利用すると、OneLake の Delta ファイルを直接メモリにロードし、Import モードに匹敵するパフォーマンスが得られる。

**注意**: RLS / CLS / DDM が設定されている場合、Direct Lake は **DirectQuery にフォールバック**する（パフォーマンスが低下）。

### 6.5.5 DMV によるクエリ分析

```sql
-- 実行中のクエリの確認
SELECT * FROM sys.dm_exec_requests;

-- クエリの実行履歴
SELECT * FROM sys.dm_exec_requests_history
ORDER BY start_time DESC;

-- セッション情報
SELECT * FROM sys.dm_exec_sessions;

-- 接続情報
SELECT * FROM sys.dm_exec_connections;
```

---

## 6.6 Warehouse のセキュリティ `[実装管理]`

### 6.6.1 行レベルセキュリティ（RLS）

```sql
-- Step 1: フィルタ関数を作成
CREATE FUNCTION dbo.fn_rls_region_filter(@region NVARCHAR(50))
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN
    SELECT 1 AS fn_result
    WHERE @region = USER_NAME()
       OR USER_NAME() = 'admin@contoso.com';

-- Step 2: セキュリティポリシーを作成
CREATE SECURITY POLICY dbo.RegionFilter
ADD FILTER PREDICATE dbo.fn_rls_region_filter(region)
ON dbo.fact_orders
WITH (STATE = ON);
```

### 6.6.2 列レベルセキュリティ（CLS）

```sql
-- 特定のユーザーに特定列へのアクセスを制限
GRANT SELECT ON dbo.dim_customer (customer_id, customer_name, region) TO [analyst@contoso.com];
DENY SELECT ON dbo.dim_customer (email, phone, ssn) TO [analyst@contoso.com];
```

### 6.6.3 動的データマスク（DDM）

```sql
-- テーブル作成時にマスク定義
CREATE TABLE dbo.employees (
    employee_id   INT,
    full_name     NVARCHAR(200),
    email         NVARCHAR(200)  MASKED WITH (FUNCTION = 'email()'),
    phone         NVARCHAR(20)   MASKED WITH (FUNCTION = 'partial(0,"XXX-XXX-",4)'),
    salary        DECIMAL(18,2)  MASKED WITH (FUNCTION = 'default()'),
    ssn           NVARCHAR(11)   MASKED WITH (FUNCTION = 'partial(0,"***-**-",4)')
);

-- 既存テーブルにマスクを追加
ALTER TABLE dbo.employees
ALTER COLUMN email ADD MASKED WITH (FUNCTION = 'email()');

-- マスク解除権限の付与
GRANT UNMASK ON dbo.employees TO [hr_manager@contoso.com];

-- マスク解除権限の取り消し
REVOKE UNMASK ON dbo.employees FROM [hr_manager@contoso.com];
```

**マスク関数の種類**:

| 関数 | 動作 | 例 |
|------|------|---|
| `default()` | 型に応じたデフォルトマスク | 数値→0, 文字列→XXXX |
| `email()` | メール形式のマスク | `aXXX@XXXX.com` |
| `random(start, end)` | 数値のランダムマスク | `random(1, 100)` |
| `partial(prefix, padding, suffix)` | 部分マスク | `partial(2, "XXX", 4)` → `toXXX6789` |

### 6.6.4 オブジェクトレベルセキュリティ

```sql
-- テーブルへのアクセスを制限
GRANT SELECT ON dbo.fact_orders TO [analyst@contoso.com];
DENY SELECT ON dbo.stg_orders TO [analyst@contoso.com];

-- スキーマレベルの権限
GRANT SELECT ON SCHEMA::gold TO [bi_team@contoso.com];
DENY SELECT ON SCHEMA::staging TO [bi_team@contoso.com];
```

### 6.6.5 セキュリティの組み合わせ

| セキュリティ機能 | 制御対象 | 適用場面 |
|---------------|---------|---------|
| RLS | どの**行**が見えるか | 部門別データフィルタリング |
| CLS | どの**列**が見えるか | PII（個人情報）の列を非表示 |
| DDM | データが**どう見えるか** | 部分的な情報の表示（メール、電話等） |
| オブジェクト | どの**テーブル/ビュー**にアクセスできるか | ステージングテーブルへのアクセス制限 |

**試験ポイント**: DDM は「表示の制御」であり、悪意あるクエリでマスクされたデータを推測することが可能。DDM だけでは完全なセキュリティにならない。RLS / CLS と組み合わせて使用する。

---

## 6.7 T-SQL エラーの特定と解決 `[監視最適化]`

### 6.7.1 一般的なエラー

| エラー | 原因 | 対処 |
|-------|------|------|
| 未サポート構文エラー | INDEX, CURSOR, TRIGGER 等の使用 | Fabric 対応構文に書き換え。公式 T-SQL Surface Area を参照 |
| 型不一致 | INSERT / MERGE で型が合わない | CAST / CONVERT で明示的に型変換 |
| NULL 制約違反 | NOT NULL 列に NULL を挿入 | COALESCE / ISNULL でデフォルト値を設定 |
| MERGE のアンビギュイティ | MERGE の結合条件で複数行がマッチ | 事前に重複排除。ソース側で ROW_NUMBER + CTE |
| クロスDB クエリエラー | 異なるワークスペースのアイテムを参照 | クロスDB は同一ワークスペース内のみ。ショートカットで対応 |
| QUALIFY 未サポート | Snowflake からの移行コード | CTE + ROW_NUMBER + WHERE で代替 |
| 書き込み競合 | 自動コンパクションとの衝突 | 通常は自動回避。明示的トランザクション + リトライ |

### 6.7.2 Snowflake SQL からの移行でよくある書き換え

```sql
-- Snowflake: QUALIFY
SELECT * FROM orders
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY modified_date DESC) = 1;

-- Fabric Warehouse: CTE + WHERE
WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY modified_date DESC) AS rn
    FROM orders
)
SELECT * FROM ranked WHERE rn = 1;

-- Snowflake: VARIANT / PARSE_JSON
SELECT raw_data:customer.name::STRING FROM events;

-- Fabric Warehouse: 非サポート → Lakehouse (Spark) で処理
-- PySpark: from_json / get_json_object を使用

-- Snowflake: FLATTEN (JSON 配列の展開)
SELECT value:item_name FROM events, LATERAL FLATTEN(input => raw_data:items);

-- Fabric Warehouse: 非サポート → Spark の explode / PySpark で処理

-- Snowflake: ILIKE (大文字小文字を無視した LIKE)
SELECT * FROM customers WHERE name ILIKE '%john%';

-- Fabric Warehouse: LOWER を使用
SELECT * FROM customers WHERE LOWER(name) LIKE '%john%';
```

---

## 6.8 確認問題

### Q1. COPY INTO

Lakehouse の Files フォルダにある CSV ファイルを Warehouse テーブルにロードする COPY INTO 文で正しいものはどれか？

A) `COPY INTO dbo.orders FROM 'Tables/orders.csv' WITH (FILE_TYPE = 'CSV')`
B) `COPY INTO dbo.orders FROM 'abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>/Files/orders.csv' WITH (FILE_TYPE = 'CSV', FIRSTROW = 2)`
C) `INSERT INTO dbo.orders SELECT * FROM OPENROWSET(BULK 'orders.csv')`
D) `LOAD DATA INFILE 'orders.csv' INTO TABLE dbo.orders`

<details>
<summary>解答</summary>

**B)**

COPY INTO はフルパス（OneLake URL）を指定する必要がある。`FIRSTROW = 2` でヘッダー行をスキップ。A は相対パスで不正確。C の OPENROWSET(BULK) も使用可能だが、ここでは COPY INTO が最適。D は MySQL 構文で Fabric では無効。
</details>

### Q2. MERGE

MERGE 文で、ソースにあるがターゲットにないレコードを挿入し、両方に存在するレコードを更新する。ターゲットにあるがソースにないレコードは削除したい。正しい MERGE 句の組み合わせはどれか？

A) `WHEN MATCHED THEN UPDATE` + `WHEN NOT MATCHED THEN INSERT`
B) `WHEN MATCHED THEN UPDATE` + `WHEN NOT MATCHED THEN INSERT` + `WHEN NOT MATCHED BY SOURCE THEN DELETE`
C) `WHEN MATCHED THEN UPDATE` + `WHEN NOT MATCHED BY TARGET THEN INSERT` + `WHEN NOT MATCHED BY SOURCE THEN UPDATE`
D) `WHEN MATCHED THEN DELETE` + `WHEN NOT MATCHED THEN INSERT`

<details>
<summary>解答</summary>

**B)**

- `WHEN MATCHED` → 両方に存在 → UPDATE
- `WHEN NOT MATCHED [BY TARGET]` → ソースのみ → INSERT
- `WHEN NOT MATCHED BY SOURCE` → ターゲットのみ → DELETE

これで完全同期（ソースの状態をターゲットに反映）が実現できる。
</details>

### Q3. ミラーリング

Azure SQL Database のミラーリングについて正しいものはどれか？

A) ミラーリングはレプリケーション用の Fabric コンピュート費用が発生する
B) ミラーリングされたデータは Warehouse テーブルとして DML 操作が可能
C) ミラーリングされたデータは Delta Parquet 形式で OneLake に格納される
D) ミラーリングは DTU Basic プランの Azure SQL DB でも使用できる

<details>
<summary>解答</summary>

**C) ミラーリングされたデータは Delta Parquet 形式で OneLake に格納される**

- A は不正解: レプリケーションのコンピュートは**無料**
- B は不正解: SQL 分析エンドポイントは**読み取り専用**。DML 不可
- D は不正解: DTU Basic / Standard（<100 DTU）は非サポート
</details>

### Q4. DDM

動的データマスクについて正しいものはどれか？

A) DDM はテーブル内の実データを物理的に変更する
B) UNMASK 権限を持つユーザーはマスクされていないデータを参照できる
C) DDM だけで機密データの完全な保護が保証される
D) DDM は Lakehouse の SQL 分析エンドポイントでは使用できない

<details>
<summary>解答</summary>

**B) UNMASK 権限を持つユーザーはマスクされていないデータを参照できる**

- A は不正解: DDM は表示時のマスクで、実データは変更しない
- C は不正解: DDM は推測攻撃に脆弱。RLS / CLS との併用が推奨
- D は不正解: DDM は SQL 分析エンドポイントでも使用可能
</details>

### Q5. Direct Lake とセキュリティ

Warehouse テーブルに RLS を設定した後、Power BI の Direct Lake セマンティックモデルでデータを参照した場合、何が起こるか？

A) RLS が無視され、全データが表示される
B) Direct Lake が DirectQuery にフォールバックし、RLS が適用される
C) Direct Lake モードのまま RLS が適用される
D) セマンティックモデルの更新がエラーになる

<details>
<summary>解答</summary>

**B) Direct Lake が DirectQuery にフォールバックし、RLS が適用される**

RLS / CLS / DDM が設定されているテーブルを Direct Lake で参照すると、セキュリティフィルタリングの適用のため自動的に DirectQuery モードにフォールバックする。パフォーマンスは低下するがセキュリティは確保される。
</details>

---

> **次章**: 第7章 Real-Time Intelligence（Eventhouse / KQL DB / Eventstream、KQL 構文、ストリーミングエンジンの選択）
