# 第2章 Lakehouse と Delta Lake

> 📘 対応 MS Learn モジュール:
> - Get started with lakehouses in Microsoft Fabric
> - Work with Delta Lake tables in Microsoft Fabric
> - Organize a Fabric lakehouse using medallion architecture design
>
> 試験タグ: `[取込変換]` `[監視最適化]`

---

## 2.1 Lakehouse の作成と構造

### 2.1.1 Lakehouse の作成

Lakehouse はワークスペース内で作成する Fabric アイテムの1つであり、Apache Spark エンジンと SQL 分析エンドポイントの両方でデータにアクセスできる。

作成時に自動的に以下が生成される:
- **Tables/ フォルダ**: マネージド Delta テーブルの格納場所
- **Files/ フォルダ**: 任意ファイル（CSV, JSON, Parquet, 画像等）の格納場所
- **SQL 分析エンドポイント**: T-SQL で読み取り専用クエリを実行するエンドポイント
- **デフォルトセマンティックモデル**: Power BI 用のセマンティックモデル（自動生成）

### 2.1.2 Tables/ と Files/ の違い

| 観点 | Tables/ | Files/ |
|------|---------|--------|
| フォーマット | Delta（Parquet + トランザクションログ） | 任意（CSV, JSON, Parquet, AVRO 等） |
| メタストア登録 | 自動的にテーブルとして登録 | 登録されない（Spark で直接読み取り可能） |
| SQL 分析エンドポイント | SELECT 可能 | アクセス不可 |
| ショートカット | トップレベルのみ作成可 | 任意階層で作成可 |
| 用途 | 分析用のクリーンデータ | 生データの着地、ステージング |

### 2.1.3 マネージドテーブル vs 外部テーブル

```python
# マネージドテーブル: メタストアがデータライフサイクルを管理
# DROP TABLE するとデータも削除される
df.write.format("delta").mode("overwrite").saveAsTable("dim_customer")

# 外部テーブル: メタストアにメタデータのみ登録、データは指定パスに格納
# DROP TABLE してもデータは残る
df.write.format("delta").mode("overwrite").saveAsTable(
    "ext_dim_customer",
    path="Files/external/dim_customer"
)
```

**試験ポイント**: Lakehouse Explorer に表示されるのは Tables/ 配下のマネージドテーブルのみ。Files/ に保存した Delta データはテーブルとしては表示されない（Spark からは直接パス指定で読める）。Files/ のデータを SQL 分析エンドポイントで読みたい場合は Tables/ にショートカットを作成する。

### 2.1.4 SQL 分析エンドポイント

SQL 分析エンドポイントは Lakehouse に自動生成される**読み取り専用**の T-SQL インターフェースである。

```sql
-- SQL 分析エンドポイントから実行可能
SELECT
    c.customer_name,
    SUM(o.amount) AS total_amount
FROM silver.fact_orders o
INNER JOIN silver.dim_customer c ON o.customer_id = c.customer_id
GROUP BY c.customer_name
ORDER BY total_amount DESC;

-- 以下は実行不可（読み取り専用）
-- INSERT INTO ...
-- UPDATE ...
-- DELETE ...
-- MERGE ...
-- CREATE TABLE ...
```

**Warehouse との使い分け**:
- 読み取りのみで十分 → SQL 分析エンドポイント
- DML（INSERT / UPDATE / DELETE / MERGE）が必要 → Warehouse
- ストアドプロシージャが必要 → Warehouse

### 2.1.5 スキーマ（Lakehouse スキーマ）

Lakehouse ではスキーマを有効化することで、テーブルを名前空間で整理できる。

```sql
-- スキーマの作成（Spark SQL）
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- スキーマ付きテーブルの作成
CREATE TABLE silver.dim_customer (
    customer_id INT,
    customer_name STRING,
    email STRING
) USING DELTA;
```

```python
# PySpark からスキーマ付きテーブルへの書き込み
df.write.format("delta").mode("overwrite").saveAsTable("silver.dim_customer")
```

---

## 2.2 Delta Lake 基礎

### 2.2.1 Delta Lake とは

Delta Lake は Apache Parquet の上に ACID トランザクション機能を追加するオープンソースのストレージレイヤーである。Fabric の Lakehouse では Delta Lake がデフォルトフォーマット。

| 機能 | Parquet | Delta Lake |
|------|---------|-----------|
| 列指向圧縮 | ○ | ○（Parquet ベース） |
| スキーマ強制 | × | ○ |
| ACID トランザクション | × | ○ |
| タイムトラベル | × | ○ |
| MERGE (upsert) | × | ○ |
| スキーマ進化 | × | ○ |
| VACUUM (不要ファイル削除) | × | ○ |

### 2.2.2 トランザクションログ（_delta_log）

Delta テーブルのディレクトリ構造:

```
Tables/fact_orders/
  ├── _delta_log/                          ← トランザクションログ
  │     ├── 00000000000000000000.json      ← バージョン 0 のコミット
  │     ├── 00000000000000000001.json      ← バージョン 1 のコミット
  │     ├── ...
  │     └── 00000000000000000010.checkpoint.parquet  ← チェックポイント（10コミットごと）
  ├── part-00000-xxx.snappy.parquet        ← 実データファイル
  ├── part-00001-xxx.snappy.parquet
  └── ...
```

各 JSON ログファイルには以下が記録される:
- **add**: 新しい Parquet ファイルの追加
- **remove**: 論理削除されたファイル（物理ファイルは VACUUM まで残る）
- **metaData**: スキーマ変更
- **commitInfo**: コミットのメタデータ（タイムスタンプ、操作種別等）

**試験ポイント**: Parquet ファイルはイミュータブル（不変）。UPDATE や DELETE 実行時は、変更データを含む新しい Parquet ファイルが追加され、古いファイルは論理的に remove される。VACUUM を実行するまで古いファイルは物理的に残る。

### 2.2.3 テーブル作成の方法

```python
# 方法1: DataFrame からの作成
df.write.format("delta").mode("overwrite").saveAsTable("products")

# 方法2: DeltaTable API
from delta.tables import DeltaTable

DeltaTable.create(spark) \
    .tableName("products") \
    .addColumn("product_id", "INT") \
    .addColumn("product_name", "STRING") \
    .addColumn("category", "STRING") \
    .addColumn("price", "DECIMAL(10,2)") \
    .execute()

# 方法3: Spark SQL
spark.sql("""
    CREATE TABLE IF NOT EXISTS products (
        product_id INT,
        product_name STRING,
        category STRING,
        price DECIMAL(10,2)
    ) USING DELTA
""")
```

---

## 2.3 Delta テーブル管理

### 2.3.1 V-Order 最適化

V-Order は Fabric 固有の書き込み時最適化で、Parquet ファイルに VertiPaq 互換のソート・エンコーディング・圧縮を適用する。

**効果**:
| 消費エンジン | 効果 |
|------------|------|
| Power BI Direct Lake | **40–60%** のコールドキャッシュクエリ改善 |
| SQL 分析エンドポイント / Warehouse | 約 **10%** の読み取り改善 |
| Spark | 読み取り改善なし。書き込みが **15–33% 遅くなる** |

**V-Order のデフォルト**: 新しいワークスペースでは**無効**（書き込み重視のエンジニアリングワークロード向け）。

```python
# セッションレベルで V-Order を有効化
spark.conf.set("spark.sql.parquet.vorder.default", "true")

# テーブルレベルで V-Order を有効化
spark.sql("""
    ALTER TABLE gold.fact_daily_sales
    SET TBLPROPERTIES ('parquet.vorder.enabled' = 'true')
""")

# 書き込み時にオプションで指定
df.write.format("delta") \
    .mode("overwrite") \
    .option("parquet.vorder.enabled", "true") \
    .saveAsTable("gold.fact_daily_sales")
```

**使い分け**:
- **有効にすべき**: Gold レイヤー（Power BI / SQL で頻繁に読み取られるテーブル）
- **無効のまま**: Bronze / Silver レイヤー（書き込みが主で、読み取りは次の変換ステップのみ）
- **ステージングテーブル**: 一度書いて一度読むだけなら無効が効率的

### 2.3.2 OPTIMIZE（ファイル圧縮）

OPTIMIZE は小さな Parquet ファイルを統合して大きなファイルにする（bin-compaction）。

```sql
-- 基本的な OPTIMIZE
OPTIMIZE silver.fact_orders;

-- Z-ORDER 付き OPTIMIZE（特定列でのクエリが高速化）
OPTIMIZE silver.fact_orders ZORDER BY (order_date, region);

-- WHERE 句で対象パーティションを絞る
OPTIMIZE silver.fact_orders
WHERE order_date >= '2024-01-01'
ZORDER BY (customer_id);
```

**最適なファイルサイズ**: 128 MB 以上、理想的には **1 GB 前後**。

**Optimize Write（自動最適化書き込み）**:

```python
# セッションレベルで有効化（書き込み時に自動でファイルサイズを最適化）
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")

# テーブルレベルで有効化
spark.sql("""
    ALTER TABLE silver.fact_orders
    SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
""")
```

**Auto Compaction（自動圧縮）**:

```python
# 書き込みごとに自動でファイル断片化を検出して圧縮
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

# テーブルレベル
spark.sql("""
    ALTER TABLE silver.fact_orders
    SET TBLPROPERTIES ('delta.autoOptimize.autoCompact' = 'true')
""")
```

**試験ポイント**: OPTIMIZE / VACUUM は **Spark SQL コマンド**であり、ノートブックまたは Spark ジョブ定義でのみ実行可能。SQL 分析エンドポイントや Warehouse の SQL クエリエディタでは実行不可。GUI からは Lakehouse Explorer の右クリック → Maintenance で実行可能。

### 2.3.3 VACUUM（不要ファイルの物理削除）

```sql
-- デフォルト保持期間（7日）で VACUUM
VACUUM silver.fact_orders;

-- 保持期間を明示的に指定
VACUUM silver.fact_orders RETAIN 168 HOURS;  -- 7日 = 168時間

-- 保持期間を短くする場合（7日未満は安全チェックを無効化する必要あり）
-- ⚠️ 本番環境では推奨されない
SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM silver.fact_orders RETAIN 24 HOURS;
```

**VACUUM の重要ルール**:
- デフォルト保持期間は **7日**
- 保持期間未満のファイルは削除されない（並行リーダー/ライターの保護）
- **VACUUM 後はタイムトラベルで削除済みバージョンにアクセス不可**
- 7日未満の保持期間を指定すると、UIとAPIではデフォルトで拒否される
- Power BI Direct Lake のセマンティックモデルがフレーミング中に参照しているバージョンを VACUUM で削除しないよう注意

### 2.3.4 スキーマ進化

```python
# 列追加の自動マージ（既存テーブルに新しい列を持つデータを書き込む）
df_with_new_column.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("silver.fact_orders")

# スキーマの完全上書き（既存スキーマを新スキーマで置換）
df_new_schema.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver.fact_orders")
```

| オプション | 動作 | ユースケース |
|-----------|------|------------|
| `mergeSchema=true` | 新しい列を追加。既存列はそのまま | ソースに列が追加された場合 |
| `overwriteSchema=true` | スキーマ全体を置換 | スキーマの大幅な変更（列名変更、型変更等） |
| なし（デフォルト） | スキーマ不一致でエラー | スキーマの一貫性を強制したい場合 |

### 2.3.5 タイムトラベル

```python
# バージョン指定で読み取り
df_v3 = spark.read.format("delta").option("versionAsOf", 3).load("Tables/fact_orders")

# タイムスタンプ指定で読み取り
df_past = spark.read.format("delta") \
    .option("timestampAsOf", "2024-06-15T10:00:00Z") \
    .load("Tables/fact_orders")
```

```sql
-- Spark SQL でのタイムトラベル
SELECT * FROM fact_orders VERSION AS OF 3;
SELECT * FROM fact_orders TIMESTAMP AS OF '2024-06-15T10:00:00';

-- テーブル履歴の確認
DESCRIBE HISTORY fact_orders;

-- 特定バージョンへの復元
RESTORE TABLE fact_orders TO VERSION AS OF 5;
RESTORE TABLE fact_orders TO TIMESTAMP AS OF '2024-06-15T10:00:00';
```

**試験ポイント**:
- `DESCRIBE HISTORY` でコミット履歴（バージョン、タイムスタンプ、操作種別、ユーザー）を確認可能
- VACUUM で物理ファイルが削除されたバージョンにはタイムトラベルできない
- RESTORE はテーブルを指定バージョンに巻き戻す（新しいコミットとして記録）
- Snowflake の `AT (TIMESTAMP => ...)` / `AT (OFFSET => ...)` に相当

### 2.3.6 Liquid Clustering（プレビュー）

従来のパーティション分割に代わるデータ配置の仕組み。

```sql
-- テーブル作成時に Liquid Clustering を指定
CREATE TABLE gold.fact_sales (
    order_id INT,
    customer_id INT,
    order_date DATE,
    amount DECIMAL(18,2)
) USING DELTA
CLUSTER BY (order_date, customer_id);

-- 既存テーブルに Liquid Clustering を追加
ALTER TABLE gold.fact_sales CLUSTER BY (order_date, customer_id);

-- クラスタリングを解除
ALTER TABLE gold.fact_sales CLUSTER BY NONE;
```

**Liquid Clustering vs パーティション vs Z-Order**:

| 機能 | パーティション | Z-Order | Liquid Clustering |
|------|-------------|---------|------------------|
| データ配置 | フォルダベース | ファイル内のソート | 動的なファイルレベル配置 |
| キー変更 | テーブル再作成が必要 | OPTIMIZE 時に再適用 | ALTER TABLE で変更可能 |
| 小カーディナリティ列 | ○（推奨） | ○ | ○ |
| 高カーディナリティ列 | ×（過剰パーティション） | ○ | ○ |
| パーティションとの共存 | - | ○ | ×（パーティションテーブルには不可） |

---

## 2.4 メダリオンアーキテクチャ設計

### 2.4.1 レイヤーの概要（復習と深掘り）

| レイヤー | 目的 | 変換例 | 推奨フォーマット |
|---------|------|-------|---------------|
| **Bronze** | 生データの保存（ソースの忠実なコピー） | スキーマ付与、監査列追加、append-only | Delta（DB系）/ 生ファイル（CSV, JSON） |
| **Silver** | クレンジング・統合 | 型変換、重複排除、NULL処理、JOIN | Delta |
| **Gold** | ビジネスレベルの集計・モデル | 集計、非正規化、ディメンションモデル | Delta |

### 2.4.2 Fabric でのメダリオン実装パターン

**パターン1: レイヤーごとに Lakehouse を分離（推奨）**

```
ワークスペース: Sales-Bronze
  └── Lakehouse: lh_bronze_sales

ワークスペース: Sales-Silver
  └── Lakehouse: lh_silver_sales

ワークスペース: Sales-Gold
  ├── Lakehouse: lh_gold_sales      ← Spark / Direct Lake 向き
  └── Warehouse: wh_gold_sales      ← T-SQL 高頻度クエリ向き
```

メリット: ワークスペース単位でアクセス制御・容量割り当て・Git連携を分離できる。

**パターン2: 1つの Lakehouse 内でスキーマ分離**

```
ワークスペース: Sales
  └── Lakehouse: lh_sales
        └── Tables/
              ├── bronze.raw_orders
              ├── bronze.raw_customers
              ├── silver.fact_orders
              ├── silver.dim_customer
              ├── gold.daily_sales_summary
              └── gold.monthly_kpi
```

メリット: シンプル。小規模プロジェクト向き。
デメリット: セキュリティ粒度がワークスペース単位になるため、Bronze の生データと Gold の分析データに異なるアクセス制御を設けにくい。

**パターン3: Bronze/Silver は Lakehouse、Gold は Warehouse**

```
ワークスペース: Sales-Engineering
  ├── Lakehouse: lh_bronze
  └── Lakehouse: lh_silver

ワークスペース: Sales-Analytics
  └── Warehouse: wh_gold
```

Gold を Warehouse にするメリット:
- フル T-SQL（MERGE, ストアドプロシージャ, ビュー）
- RLS / CLS / DDM のネイティブサポート
- 50人以上の同時ユーザーに適した高並行性

### 2.4.3 Bronze レイヤーの実装

```python
from pyspark.sql.functions import current_timestamp, lit, input_file_name

# Bronze: 生データの取り込み（メタデータ列を追加して append）
df_raw = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("Files/raw/orders/2024/01/")
)

df_bronze = (
    df_raw
    .withColumn("_ingestion_ts", current_timestamp())
    .withColumn("_source_file", input_file_name())
    .withColumn("_batch_id", lit("batch_20240115_001"))
)

# append モード（Bronze は append-only が原則）
df_bronze.write.format("delta").mode("append").saveAsTable("bronze.raw_orders")
```

**Bronze 設計のポイント**:
- **append-only**: 過去データを上書きしない。ソースの忠実なコピーを保持
- **監査列**: `_ingestion_ts`, `_source_file`, `_batch_id` 等を追加して追跡可能にする
- **スキーマ**: inferSchema または全列 STRING で着地し、Silver で型変換
- **パーティション**: 取り込み日でのパーティション（`_ingestion_date`）がライフサイクル管理に有効
- **V-Order**: 不要（一度書いて一度読むだけ）

### 2.4.4 Silver レイヤーの実装

```python
from pyspark.sql.functions import col, to_date, trim, upper, row_number
from pyspark.sql.window import Window

# Silver: クレンジング・型変換・重複排除
df_bronze = spark.read.format("delta").table("bronze.raw_orders")

# 型変換とクレンジング
df_cleaned = (
    df_bronze
    .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
    .withColumn("amount", col("amount").cast("decimal(18,2)"))
    .withColumn("quantity", col("quantity").cast("int"))
    .withColumn("customer_name", trim(upper(col("customer_name"))))
    .filter(col("order_date").isNotNull())
    .filter(col("amount") > 0)
)

# 重複排除（同一 order_id の最新レコードを保持）
window_spec = Window.partitionBy("order_id").orderBy(col("_ingestion_ts").desc())

df_deduped = (
    df_cleaned
    .withColumn("_row_num", row_number().over(window_spec))
    .filter(col("_row_num") == 1)
    .drop("_row_num")
)

# overwrite モード（Silver は最新の正規化されたビュー）
df_deduped.write.format("delta").mode("overwrite").saveAsTable("silver.fact_orders")
```

**Silver 設計のポイント**:
- **型変換**: 全列を適切な型に変換（STRING → DATE, DECIMAL, INT 等）
- **重複排除**: ビジネスキー + タイムスタンプで最新レコードを選択
- **NULL 処理**: ビジネスルールに基づいて fillna / filter
- **結合**: ソース間の統合（異なるシステムの顧客データを customer_id で結合等）
- **V-Order**: Gold で読まれる場合は検討。Silver のみ中間ステップなら不要

### 2.4.5 Gold レイヤーの実装

```python
from pyspark.sql.functions import sum, count, avg

# Gold: ビジネスレベルの集計
df_silver = spark.read.format("delta").table("silver.fact_orders")
df_customer = spark.read.format("delta").table("silver.dim_customer")

# 非正規化 + 集計
df_gold = (
    df_silver
    .join(df_customer, "customer_id", "inner")
    .groupBy("region", "product_category", "order_date")
    .agg(
        count("order_id").alias("order_count"),
        sum("amount").alias("total_revenue"),
        avg("amount").alias("avg_order_value")
    )
)

df_gold.write.format("delta") \
    .mode("overwrite") \
    .option("parquet.vorder.enabled", "true") \
    .saveAsTable("gold.daily_sales_summary")
```

**Gold 設計のポイント**:
- **V-Order**: 有効にする（Power BI / SQL での読み取りが主用途）
- **ディメンションモデル**: スタースキーマを構築（次節で詳述）
- **非正規化**: 分析クエリのパフォーマンスのために意図的に冗長データを含める
- **集計**: ビジネスKPI に合わせた粒度で事前集計

---

## 2.5 ディメンションモデルの準備 `[取込変換]`

### 2.5.1 スタースキーマ設計

```
           ┌──────────────┐
           │ dim_date     │
           └──────┬───────┘
                  │
┌──────────────┐  │  ┌──────────────┐
│ dim_customer ├──┼──┤ fact_orders  │
└──────────────┘  │  └──────┬───────┘
                  │         │
           ┌──────┴───────┐ │
           │ dim_product  ├─┘
           └──────────────┘
```

```python
# ファクトテーブルの構成例
fact_orders_schema = """
    order_sk        BIGINT,         -- サロゲートキー
    order_id        INT,            -- ビジネスキー（ソースの元ID）
    customer_sk     BIGINT,         -- dim_customer へのFK
    product_sk      BIGINT,         -- dim_product へのFK
    order_date_key  INT,            -- dim_date へのFK (YYYYMMDD)
    quantity        INT,
    unit_price      DECIMAL(18,2),
    amount          DECIMAL(18,2),
    _load_ts        TIMESTAMP
"""
```

### 2.5.2 サロゲートキーの生成

```python
from pyspark.sql.functions import monotonically_increasing_id, row_number
from pyspark.sql.window import Window

# 方法1: monotonically_increasing_id（ユニークだが連番ではない）
df = df.withColumn("customer_sk", monotonically_increasing_id())

# 方法2: row_number()（連番、ただし Window が必要）
window_spec = Window.orderBy("customer_id")
df = df.withColumn("customer_sk", row_number().over(window_spec))

# 方法3: 既存の最大値からオフセット（増分ロード時）
max_sk = spark.sql("SELECT COALESCE(MAX(customer_sk), 0) FROM gold.dim_customer").collect()[0][0]
window_spec = Window.orderBy("customer_id")
df_new = df_new.withColumn("customer_sk", row_number().over(window_spec) + max_sk)
```

```sql
-- Warehouse では IDENTITY 列が使用可能
CREATE TABLE gold.dim_customer (
    customer_sk  BIGINT IDENTITY(1,1) NOT NULL,
    customer_id  INT NOT NULL,
    customer_name NVARCHAR(200),
    email        NVARCHAR(200)
);
```

### 2.5.3 SCD（Slowly Changing Dimensions）

試験では SCD Type 1 と Type 2 が最も重要。

#### SCD Type 1（上書き）

現在値のみ保持。変更があれば既存レコードを上書き。

```python
from delta.tables import DeltaTable

delta_target = DeltaTable.forName(spark, "gold.dim_customer")

delta_target.alias("tgt").merge(
    df_source.alias("src"),
    "tgt.customer_id = src.customer_id"
).whenMatchedUpdate(
    condition="tgt.customer_name <> src.customer_name OR tgt.email <> src.email",
    set={
        "customer_name": "src.customer_name",
        "email": "src.email",
        "_load_ts": "current_timestamp()"
    }
).whenNotMatchedInsert(
    values={
        "customer_id": "src.customer_id",
        "customer_name": "src.customer_name",
        "email": "src.email",
        "_load_ts": "current_timestamp()"
    }
).execute()
```

```sql
-- T-SQL（Warehouse）での SCD Type 1
MERGE INTO gold.dim_customer AS tgt
USING staging.stg_customer AS src
    ON tgt.customer_id = src.customer_id
WHEN MATCHED AND (
    tgt.customer_name <> src.customer_name
    OR tgt.email <> src.email
) THEN
    UPDATE SET
        tgt.customer_name = src.customer_name,
        tgt.email = src.email
WHEN NOT MATCHED THEN
    INSERT (customer_id, customer_name, email)
    VALUES (src.customer_id, src.customer_name, src.email);
```

#### SCD Type 2（履歴保持）

変更があると新しい行を追加し、古い行を無効化。

```python
from pyspark.sql.functions import current_timestamp, lit, when

target = DeltaTable.forName(spark, "gold.dim_customer_scd2")
df_updates = spark.read.format("delta").table("silver.stg_customer")

# Step 1: 既存の変更レコードを無効化（is_current = false, valid_to = 現在時刻）
target.alias("tgt").merge(
    df_updates.alias("src"),
    "tgt.customer_id = src.customer_id AND tgt.is_current = true"
).whenMatchedUpdate(
    condition="tgt.customer_name <> src.customer_name OR tgt.email <> src.email",
    set={
        "is_current": lit(False),
        "valid_to": current_timestamp()
    }
).execute()

# Step 2: 変更された顧客の新しいレコードを挿入
# （変更があった顧客のみ抽出）
df_changed = (
    df_updates.alias("src")
    .join(
        spark.read.format("delta").table("gold.dim_customer_scd2")
            .filter("is_current = false")
            .alias("closed"),
        (col("src.customer_id") == col("closed.customer_id"))
        & (col("closed.valid_to").isNotNull()),
        "inner"
    )
    .select("src.*")
    .distinct()
)

# 新規 + 変更顧客を挿入
df_new_and_changed = (
    df_updates.alias("src")
    .join(
        spark.read.format("delta").table("gold.dim_customer_scd2")
            .filter("is_current = true")
            .alias("current"),
        "customer_id",
        "left_anti"  # 現在有効なレコードがないもの = 新規 or 先ほど閉じたもの
    )
)

df_insert = (
    df_new_and_changed
    .withColumn("is_current", lit(True))
    .withColumn("valid_from", current_timestamp())
    .withColumn("valid_to", lit(None).cast("timestamp"))
)

df_insert.write.format("delta").mode("append").saveAsTable("gold.dim_customer_scd2")
```

**SCD Type 2 のテーブル構造**:

```sql
CREATE TABLE gold.dim_customer_scd2 (
    customer_sk    BIGINT,         -- サロゲートキー（行ごとにユニーク）
    customer_id    INT,            -- ビジネスキー（顧客ごとに同じ）
    customer_name  STRING,
    email          STRING,
    is_current     BOOLEAN,
    valid_from     TIMESTAMP,
    valid_to       TIMESTAMP       -- NULL = 現在有効
);
```

#### SCD の種類まとめ

| Type | 動作 | 履歴 | 実装複雑度 | ユースケース |
|------|------|------|-----------|------------|
| 0 | 変更しない | なし | 最低 | 生年月日等の不変属性 |
| 1 | 上書き | なし | 低 | 修正（typo修正等） |
| 2 | 新行追加 + 旧行無効化 | 完全 | 高 | 住所変更、ステータス変更等 |
| 3 | 追加カラムに前回値 | 1世代前のみ | 中 | 直前値だけ必要な場合 |
| 4 | 履歴テーブル分離 | 完全（別テーブル） | 高 | 変更頻度が高いディメンション |

### 2.5.4 データの非正規化 `[取込変換]`

```python
# ファクトテーブルにディメンション属性を結合して非正規化
df_denormalized = (
    spark.read.format("delta").table("silver.fact_orders").alias("f")
    .join(
        spark.read.format("delta").table("silver.dim_customer").alias("c"),
        col("f.customer_id") == col("c.customer_id"),
        "inner"
    )
    .join(
        spark.read.format("delta").table("silver.dim_product").alias("p"),
        col("f.product_id") == col("p.product_id"),
        "inner"
    )
    .select(
        col("f.order_id"),
        col("f.order_date"),
        col("c.customer_name"),
        col("c.region"),
        col("p.product_name"),
        col("p.category"),
        col("f.quantity"),
        col("f.amount")
    )
)

df_denormalized.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold.wide_orders")
```

---

## 2.6 データの重複・欠落・遅延の処理 `[取込変換]`

### 2.6.1 重複排除

```python
# 方法1: dropDuplicates（完全一致の重複排除）
df_deduped = df.dropDuplicates(["order_id"])

# 方法2: row_number + filter（最新レコードを残す）
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

w = Window.partitionBy("order_id").orderBy(col("_ingestion_ts").desc())
df_deduped = df.withColumn("rn", row_number().over(w)).filter("rn = 1").drop("rn")
```

```sql
-- T-SQL での重複排除（QUALIFY がないため CTE + ROW_NUMBER）
WITH ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _ingestion_ts DESC) AS rn
    FROM staging.raw_orders
)
SELECT * FROM ranked WHERE rn = 1;

-- Snowflake なら QUALIFY が使えるが、Fabric Warehouse では CTE で代替
```

### 2.6.2 欠落データの処理

```python
from pyspark.sql.functions import coalesce, lit, when

# NULL の補完
df = df.fillna({
    "quantity": 0,
    "customer_name": "Unknown",
    "email": "noemail@unknown.com"
})

# 条件付き補完
df = df.withColumn(
    "amount",
    when(col("amount").isNull(), col("quantity") * col("unit_price"))
    .otherwise(col("amount"))
)

# COALESCE で複数列からフォールバック
df = df.withColumn(
    "contact_email",
    coalesce(col("primary_email"), col("secondary_email"), lit("no-email"))
)
```

### 2.6.3 到着遅延データの処理

到着遅延（Late-arriving data）は、イベント発生後にデータが到着するケース。

```python
# パターン1: MERGE による遅延データの取り込み（ファクトテーブルの場合）
delta_target = DeltaTable.forName(spark, "silver.fact_orders")

delta_target.alias("tgt").merge(
    df_late_arriving.alias("src"),
    "tgt.order_id = src.order_id"
).whenMatchedUpdateAll(  # 既にある場合は更新
).whenNotMatchedInsertAll(  # ない場合は挿入
).execute()

# パターン2: 遅延ディメンションキーの処理
# ファクトが先に到着し、対応するディメンションがまだない場合
# → ディメンションに「不明」レコードを用意しておく
spark.sql("""
    INSERT INTO gold.dim_customer (customer_sk, customer_id, customer_name, email)
    VALUES (-1, -1, 'Unknown', 'unknown@unknown.com')
""")

# ファクトロード時に結合失敗した場合は SK = -1 を使用
df_fact = df_fact.withColumn(
    "customer_sk",
    coalesce(col("customer_sk"), lit(-1))
)
```

---

## 2.7 Lakehouse テーブルの最適化 `[監視最適化]`

### 2.7.1 メンテナンス戦略のまとめ

| レイヤー | OPTIMIZE 頻度 | V-Order | VACUUM 保持 |
|---------|-------------|---------|------------|
| Bronze | 大量バッチロード後 | 無効 | 7日以上 |
| Silver | 日次（アクティブなテーブル） | Gold で読まれるなら検討 | 7日 |
| Gold | 日次〜週次 | **有効**（読み取り重視） | 7日 |
| ステージング | 不要（短命テーブル） | 無効 | 最小限 |

### 2.7.2 パーティション戦略

```python
# パーティション分割の例（日付でパーティション）
df.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("order_date") \
    .saveAsTable("silver.fact_orders")
```

**パーティション設計の原則**:
- パーティション内のファイルサイズが **1 GB 前後** になるカーディナリティを選ぶ
- **過剰パーティション**の回避: パーティションが小さすぎる（数 MB）と小さなファイルが大量にでき、パフォーマンスが劣化
- 日次のデータ量が少ない場合は、月単位のパーティション or パーティションなしが適切
- Liquid Clustering が使える場合はパーティションよりそちらを推奨

### 2.7.3 自動化

```python
# パイプラインから呼び出されるメンテナンスノートブック
tables_to_optimize = ["silver.fact_orders", "silver.dim_customer", "gold.daily_sales"]

for table_name in tables_to_optimize:
    spark.sql(f"OPTIMIZE {table_name}")
    spark.sql(f"VACUUM {table_name} RETAIN 168 HOURS")
    print(f"Maintenance completed for {table_name}")
```

Lakehouse REST API による自動化:

```
POST /v1/workspaces/{workspaceId}/lakehouses/{lakehouseId}/jobs/TableMaintenance/instances
{
    "executionData": {
        "tableName": "fact_orders",
        "schemaName": "silver",
        "optimizeSettings": { "vOrder": true },
        "vacuumSettings": { "retentionPeriod": "7.01:00:00" }
    }
}
```

---

## 2.8 確認問題

### Q1. Delta Lake の特性

以下のうち Delta Lake の特性として**誤っている**ものはどれか？

A) ACID トランザクションをサポートする
B) タイムトラベルにより過去バージョンのデータを読み取れる
C) VACUUM を実行すると、すべての過去バージョンのデータが物理削除される
D) MERGE コマンドによる upsert をサポートする

<details>
<summary>解答</summary>

**C) VACUUM を実行すると、すべての過去バージョンのデータが物理削除される**

VACUUM は保持期間（デフォルト7日）を超えた不要ファイルのみを削除する。保持期間内のファイルは削除されない。「すべての過去バージョン」ではない。
</details>

### Q2. V-Order の使い分け

以下のシナリオのうち、V-Order を有効にすべき**ではない**ものはどれか？

A) Power BI Direct Lake で頻繁に参照される Gold テーブル
B) SQL 分析エンドポイントから日次ダッシュボード用にクエリされるテーブル
C) ストリーミング取り込みで毎秒書き込みが発生する Bronze テーブル
D) 週次のバッチレポート用に集計される Gold 集計テーブル

<details>
<summary>解答</summary>

**C) ストリーミング取り込みで毎秒書き込みが発生する Bronze テーブル**

V-Order は書き込み時に15–33%のオーバーヘッドを追加する。高頻度書き込みの Bronze テーブルでは、読み取り改善の恩恵よりも書き込みコストが上回る。V-Order は読み取り重視の Gold テーブルで最も効果的。
</details>

### Q3. SCD Type 2

SCD Type 2 を実装する dim_customer テーブルで、顧客の住所が変更された場合の正しい処理はどれか？

A) 既存レコードの住所を上書きする
B) 既存レコードの is_current を false、valid_to を現在時刻に更新し、新しい住所で新レコードを挿入する
C) 既存レコードに previous_address 列を追加して前回住所を保存する
D) 既存レコードを削除し、新しい住所で新レコードを挿入する

<details>
<summary>解答</summary>

**B) 既存レコードの is_current を false、valid_to を現在時刻に更新し、新しい住所で新レコードを挿入する**

SCD Type 2 は完全な変更履歴を保持する。旧レコードを論理的に閉じ（is_current=false, valid_to=現在）、新レコードを有効状態で挿入する。A は Type 1、C は Type 3、D はどの SCD にも該当しない（データ損失のリスク）。
</details>

### Q4. OPTIMIZE と VACUUM

以下のうち正しいものはどれか？

A) OPTIMIZE は SQL 分析エンドポイントから実行できる
B) VACUUM のデフォルト保持期間は30日である
C) OPTIMIZE 実行後にファイルが統合されても、タイムトラベルには影響しない
D) VACUUM を7日未満の保持期間で実行するとデフォルトでエラーになる

<details>
<summary>解答</summary>

**D) VACUUM を7日未満の保持期間で実行するとデフォルトでエラーになる**

- A は不正解: OPTIMIZE は Spark SQL コマンド。SQL 分析エンドポイント（T-SQL 専用）では実行不可
- B は不正解: デフォルト保持期間は7日
- C は部分的に正解だが不正確: OPTIMIZE 自体はタイムトラベルに影響しないが、統合前の古いファイルは VACUUM で削除可能になる
- D は正解: 安全チェック（`retentionDurationCheck`）によりデフォルトで拒否される
</details>

### Q5. メダリオンアーキテクチャ

Gold レイヤーに 50人以上のビジネスアナリストが同時にアクセスし、複雑な T-SQL クエリを実行する要件がある。Gold レイヤーに最適な Fabric アイテムはどれか？

A) Lakehouse（SQL 分析エンドポイント経由）
B) Warehouse
C) Eventhouse
D) Notebook

<details>
<summary>解答</summary>

**B) Warehouse**

50人以上の同時ユーザーで複雑な T-SQL クエリを実行する要件には、高並行性をサポートする Warehouse が最適。Lakehouse の SQL 分析エンドポイントは読み取り専用で同時接続数が限定的。Eventhouse は時系列/ストリーミング向き。
</details>

---

> **次章**: 第3章 Apache Spark とノートブック（Spark プール、PySpark DataFrame 操作、mssparkutils、構造化ストリーミング、パフォーマンス最適化）
