# 第3章 Apache Spark とノートブック

> 📘 対応 MS Learn モジュール: Use Apache Spark in Microsoft Fabric
> 試験タグ: `[実装管理]` `[取込変換]` `[監視最適化]`

---

## 3.1 Fabric における Spark の仕組み

### 3.1.1 Spark プール

Fabric では Spark のコンピュートリソースを**プール**として管理する。プールは2種類。

| 種類 | ノードサイズ | 起動時間 | カスタマイズ性 | ユースケース |
|------|-----------|---------|-------------|------------|
| **Starter Pool** | Medium（固定） | **5–10秒**（常時起動クラスタ） | 低（ノードサイズ変更不可。Autoscale/動的アロケーションの調整のみ） | 開発・探索・小中規模ジョブ |
| **Custom Pool** | Small〜XX-Large（選択可） | **約3分** | 高（ノードサイズ、ノード数、Autoscale ON/OFF 等） | 本番・大規模ジョブ・特定要件 |

**ノードサイズとCU の関係**:

| ノードサイズ | コア数 | メモリ | 必要 CU |
|------------|-------|-------|--------|
| Small | 4 | 32 GB | 4 CU |
| Medium | 8 | 64 GB | 8 CU |
| Large | 16 | 128 GB | 16 CU |
| X-Large | 32 | 256 GB | 32 CU |
| XX-Large | 64 | 512 GB | 64 CU |

**Custom Pool の作成**（ワークスペース管理者のみ）:

```
ワークスペース設定 → Data Engineering/Science → Spark settings → Pool タブ
  → New Pool
    - 名前: etl-pool-large
    - ノードファミリ: Memory Optimized
    - ノードサイズ: Large
    - Autoscale: 有効（Min: 1, Max: 5）
    - Dynamic Executor Allocation: 有効（Min: 1, Max: 4）
```

**試験ポイント**:
- Starter Pool は**常時起動**のため、起動が速い（5–10秒）。Custom Pool は起動に約3分
- Starter Pool のノードサイズは **Medium 固定**で変更不可
- Custom Pool の作成には、容量管理者がテナント設定で「Customized workspace pools」を有効にする必要がある
- 単一ノードクラスタ（Min Node = 1）がサポートされており、ドライバーとエグゼキューターが同一ノードで動作

### 3.1.2 Spark ワークスペース設定 `[実装管理]`

ワークスペース設定の「Data Engineering/Science → Spark settings」で構成する項目:

| タブ | 設定内容 |
|-----|---------|
| **Pool** | デフォルトプールの選択（Starter or Custom）、Custom Pool の作成・編集 |
| **Environment** | デフォルト環境の選択、Runtime バージョンの指定 |
| **High Concurrency** | 高並行性モード（同一セッションで複数ノートブックを実行） |
| **Automatic Log** | Spark イベントログの自動保存 |

**Customize compute configuration for items**: この設定が有効（デフォルト）だと、個別のノートブックや Spark ジョブ定義が環境アイテムを通じて独自のコンピュート構成を使用できる。無効にすると全ジョブがワークスペースのデフォルトプール設定を使う。

### 3.1.3 環境アイテム（Environment）

環境はライブラリ・Spark プロパティ・Runtime バージョン・コンピュート構成をバンドルした Fabric アイテムである。

```
環境アイテムの構成要素:

┌─────────────────────────────────┐
│ 環境: env-etl-production        │
├─────────────────────────────────┤
│ Runtime: 1.3 (Spark 3.5,       │
│          Delta 3.2)             │
├─────────────────────────────────┤
│ Compute:                        │
│   Pool: etl-pool-large          │
│   Driver: 16 cores, 128 GB     │
│   Executor: 16 cores, 128 GB   │
├─────────────────────────────────┤
│ Public Libraries:               │
│   - great_expectations==0.18.8  │
│   - azure-identity==1.15.0     │
├─────────────────────────────────┤
│ Custom Libraries:               │
│   - company_utils-1.0.0.whl    │
├─────────────────────────────────┤
│ Spark Properties:               │
│   spark.sql.shuffle.partitions  │
│   = 200                         │
│   spark.sql.parquet.vorder      │
│   .default = true               │
└─────────────────────────────────┘
```

**ライブラリ管理**:

| 方法 | 対象 | 反映タイミング |
|------|------|-------------|
| 環境アイテム → Public Libraries | PyPI / Conda パッケージ | Publish 後の次のセッション |
| 環境アイテム → Custom Libraries | .whl / .jar / .tar.gz | Publish 後の次のセッション |
| ノートブック内 `%pip install` | PyPI パッケージ | 現在のセッションのみ（一時的） |
| `%%configure` マジック | Spark プロパティ | 現在のセッションのみ |

```python
# ノートブック内でのインラインインストール（セッション限り）
%pip install great_expectations==0.18.8

# Spark プロパティの一時変更
%%configure
{
    "conf": {
        "spark.sql.shuffle.partitions": "400",
        "spark.sql.parquet.vorder.default": "true"
    }
}
```

**Runtime バージョン**:

| Runtime | Spark | Delta Lake | 状態 |
|---------|-------|-----------|------|
| 1.2 | 3.4 | 2.4 | GA |
| **1.3** | **3.5** | **3.2** | **GA（推奨）** |
| 2.0 | 4.0 | 4.0 | Experimental Preview |

**試験ポイント**: 本番ワークロードには GA の最新 Runtime（現時点では 1.3）を使用する。Runtime 2.0 は実験的プレビューで、V-Order やスキーマ進化などの一部機能が未サポート。

---

## 3.2 PySpark DataFrame 操作

### 3.2.1 読み込みと書き込み

```python
# ── Delta テーブルの読み込み ──
df = spark.read.format("delta").table("silver.fact_orders")
# または
df = spark.read.format("delta").load("Tables/silver/fact_orders")

# ── CSV の読み込み ──
df_csv = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("multiLine", "true")        # 複数行にまたがるフィールド
    .option("dateFormat", "yyyy-MM-dd")
    .load("Files/raw/orders.csv")
)

# ── JSON の読み込み ──
df_json = (
    spark.read.format("json")
    .option("multiLine", "true")
    .load("Files/raw/events.json")
)

# ── Parquet の読み込み ──
df_parquet = spark.read.format("parquet").load("Files/raw/products.parquet")

# ── スキーマ明示指定（inferSchema より高速・確実）──
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, DateType

schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("order_date", DateType(), True),
    StructField("amount", DecimalType(18, 2), True),
    StructField("product_name", StringType(), True)
])
df = spark.read.format("csv").option("header", "true").schema(schema).load("Files/raw/orders.csv")

# ── 書き込み ──
# マネージドテーブルとして保存
df.write.format("delta").mode("overwrite").saveAsTable("silver.fact_orders")

# 外部パスに保存
df.write.format("delta").mode("append").save("Files/staging/orders_delta")

# パーティション付き書き込み
df.write.format("delta").mode("overwrite").partitionBy("order_date").saveAsTable("silver.fact_orders")
```

**mode の種類**:

| mode | 動作 |
|------|------|
| `overwrite` | テーブル全体を置換 |
| `append` | 既存データに追加 |
| `ignore` | テーブルが存在すれば何もしない |
| `errorIfExists` | テーブルが存在すればエラー（デフォルト） |

### 3.2.2 基本変換

```python
from pyspark.sql.functions import col, lit, when, upper, trim, to_date, current_timestamp, regexp_replace

# 列選択
df = df.select("order_id", "customer_id", "amount")

# 列の追加・変換
df = (
    df
    .withColumn("amount_jpy", col("amount") * lit(150))
    .withColumn("order_date", to_date(col("order_date_str"), "yyyy-MM-dd"))
    .withColumn("customer_name", trim(upper(col("customer_name"))))
    .withColumn("load_ts", current_timestamp())
)

# 条件分岐
df = df.withColumn(
    "order_size",
    when(col("amount") >= 10000, "large")
    .when(col("amount") >= 1000, "medium")
    .otherwise("small")
)

# 列の削除
df = df.drop("temp_column", "debug_flag")

# 型変換
df = df.withColumn("quantity", col("quantity").cast("int"))
df = df.withColumn("price", col("price").cast("decimal(18,2)"))

# フィルタリング
df = df.filter(col("amount") > 0)
df = df.filter((col("order_date") >= "2024-01-01") & (col("status") != "cancelled"))

# 正規表現による変換
df = df.withColumn("phone_clean", regexp_replace(col("phone"), "[^0-9]", ""))

# 列名変更
df = df.withColumnRenamed("old_name", "new_name")
```

### 3.2.3 結合と集計

```python
from pyspark.sql.functions import count, sum, avg, min, max, collect_list, countDistinct

# ── JOIN ──
df_result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), col("o.customer_id") == col("c.customer_id"), "inner")
    .join(df_products.alias("p"), col("o.product_id") == col("p.product_id"), "left")
    .select("o.order_id", "c.customer_name", "p.product_name", "o.amount")
)
```

| JOIN 種別 | PySpark | SQL相当 | 動作 |
|----------|---------|--------|------|
| `inner` | 両方に存在する行 | INNER JOIN | |
| `left` / `left_outer` | 左テーブルの全行 | LEFT JOIN | |
| `right` / `right_outer` | 右テーブルの全行 | RIGHT JOIN | |
| `full` / `full_outer` | 両方の全行 | FULL OUTER JOIN | |
| `left_anti` | 右に存在**しない**左の行 | WHERE NOT EXISTS | |
| `left_semi` | 右に存在**する**左の行（右の列は含まない） | WHERE EXISTS | |
| `cross` | デカルト積 | CROSS JOIN | |

```python
# ── 集計 ──
df_summary = (
    df_orders
    .groupBy("region", "product_category")
    .agg(
        count("order_id").alias("order_count"),
        sum("amount").alias("total_revenue"),
        avg("amount").alias("avg_order_value"),
        min("order_date").alias("first_order"),
        max("order_date").alias("last_order"),
        countDistinct("customer_id").alias("unique_customers")
    )
)

# ── PIVOT ──
df_pivot = (
    df_orders
    .groupBy("region")
    .pivot("product_category", ["Electronics", "Clothing", "Food"])
    .agg(sum("amount"))
)
# 結果: region | Electronics | Clothing | Food
```

### 3.2.4 ウィンドウ関数

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, lag, lead, sum as _sum

# ── ウィンドウ定義 ──
w_customer = Window.partitionBy("customer_id").orderBy(col("order_date").desc())
w_region = Window.partitionBy("region").orderBy("order_date")

# ── ランキング ──
df = df.withColumn("rn", row_number().over(w_customer))       # 1,2,3（タイなし）
df = df.withColumn("rnk", rank().over(w_customer))             # 1,2,2,4（タイあり、欠番）
df = df.withColumn("drnk", dense_rank().over(w_customer))      # 1,2,2,3（タイあり、欠番なし）

# ── 前後行の参照 ──
df = df.withColumn("prev_amount", lag("amount", 1).over(w_region))
df = df.withColumn("next_amount", lead("amount", 1).over(w_region))

# ── 累積集計 ──
w_running = Window.partitionBy("region").orderBy("order_date").rowsBetween(
    Window.unboundedPreceding, Window.currentRow
)
df = df.withColumn("running_total", _sum("amount").over(w_running))

# ── 移動平均（直近7日間）──
w_moving = Window.partitionBy("region").orderBy("order_date").rangeBetween(-6, 0)
df = df.withColumn("moving_avg_7d", avg("amount").over(w_moving))

# ── 重複排除（最新1件を残す）──
w_dedup = Window.partitionBy("order_id").orderBy(col("_ingestion_ts").desc())
df_deduped = df.withColumn("rn", row_number().over(w_dedup)).filter("rn = 1").drop("rn")
```

**試験での頻出パターン**: 重複排除（row_number + filter）、前後比較（lag/lead）、累積合計（running total）

### 3.2.5 UDF（ユーザー定義関数）

```python
from pyspark.sql.functions import udf, pandas_udf
from pyspark.sql.types import StringType
import pandas as pd

# ── 通常の UDF（行ごとに Python 関数を呼び出し → 遅い）──
@udf(returnType=StringType())
def mask_email(email: str) -> str:
    if email is None:
        return None
    parts = email.split("@")
    return parts[0][:2] + "***@" + parts[1]

df = df.withColumn("masked_email", mask_email(col("email")))

# ── Pandas UDF（Vectorized UDF → 高速）──
@pandas_udf(StringType())
def mask_email_vectorized(emails: pd.Series) -> pd.Series:
    return emails.apply(lambda e: e.split("@")[0][:2] + "***@" + e.split("@")[1] if e else None)

df = df.withColumn("masked_email", mask_email_vectorized(col("email")))
```

**UDF の使い分け**:
- 通常 UDF: 行ごとに Python 関数呼び出し。JVM ↔ Python のシリアライゼーションオーバーヘッドが大きい
- **Pandas UDF（推奨）**: Apache Arrow でバッチ処理。通常 UDF の **数倍〜数十倍高速**
- **組み込み関数が最速**: 可能な限り `pyspark.sql.functions` の組み込み関数を使い、UDF は最後の手段

---

## 3.3 NotebookUtils（旧 mssparkutils）

`mssparkutils` は `notebookutils` にリネームされた。既存コードは後方互換だが、新規コードでは `notebookutils` を使う。

### 3.3.1 ファイル操作（fs）

```python
from notebookutils import mssparkutils

# ファイル一覧
files = mssparkutils.fs.ls("Files/raw/")
for f in files:
    print(f"{f.name} | {f.size} bytes | isDir={f.isDir}")

# ディレクトリ作成
mssparkutils.fs.mkdirs("Files/staging/2024/01/")

# ファイルコピー
mssparkutils.fs.cp("Files/raw/data.csv", "Files/staging/data.csv")
# 再帰コピー
mssparkutils.fs.cp("Files/raw/", "Files/backup/raw/", recurse=True)

# ファイル移動
mssparkutils.fs.mv("Files/staging/data.csv", "Files/archive/data.csv")

# ファイル先頭の読み取り（最大 maxBytes）
head_content = mssparkutils.fs.head("Files/raw/data.csv", maxBytes=1024)
print(head_content)

# ファイル書き込み
mssparkutils.fs.put("Files/output/result.txt", "処理完了", overwrite=True)

# ファイル/ディレクトリ削除
mssparkutils.fs.rm("Files/staging/temp/", recurse=True)

# 高速コピー（大量ファイル向け）
mssparkutils.fs.fastcp("Files/raw/", "Files/backup/", recurse=True)
```

### 3.3.2 ノートブック連携（notebook）

```python
# ── 単一ノートブックの実行（同期）──
result = mssparkutils.notebook.run(
    "transform_silver",                    # ノートブック名
    timeout_seconds=600,                   # タイムアウト
    arguments={                            # パラメータ
        "source_table": "bronze.raw_orders",
        "target_table": "silver.fact_orders",
        "load_date": "2024-01-15"
    }
)
print(f"Exit value: {result}")  # 呼び出し先で mssparkutils.notebook.exit("success") した値

# ── 複数ノートブックの並列実行 ──
mssparkutils.notebook.runMultiple(["nb_silver_orders", "nb_silver_customers", "nb_silver_products"])

# ── DAG 定義による依存関係付き並列実行 ──
DAG = {
    "activities": [
        {
            "name": "silver_orders",
            "path": "nb_silver_orders",
            "timeoutPerCellInSeconds": 90,
            "args": {"load_date": "2024-01-15"},
            "retry": 1,
            "retryIntervalInSeconds": 30,
            "dependencies": []  # 依存なし → 最初に実行
        },
        {
            "name": "silver_customers",
            "path": "nb_silver_customers",
            "args": {"load_date": "2024-01-15"},
            "dependencies": []  # 依存なし → silver_orders と並列
        },
        {
            "name": "gold_summary",
            "path": "nb_gold_summary",
            "args": {"load_date": "2024-01-15"},
            "dependencies": ["silver_orders", "silver_customers"]  # 両方完了後に実行
        }
    ],
    "timeoutInSeconds": 1800,
    "concurrency": 3
}
mssparkutils.notebook.runMultiple(DAG)

# ── ノートブックの終了値の設定（呼び出し先で実行）──
mssparkutils.notebook.exit("success")  # 文字列のみ
```

**`%run` vs `mssparkutils.notebook.run`**:

| 項目 | `%run` | `mssparkutils.notebook.run` |
|------|--------|---------------------------|
| 実行方式 | 同期（インライン） | 同期（別スレッド） |
| 変数共有 | 共有される（同一セッション） | **共有されない**（カプセル化） |
| パラメータ | × | ○（arguments で渡す） |
| 戻り値 | × | ○（exit value） |
| 並列実行 | × | ○（runMultiple） |
| SparkSession | 共有 | 共有（同一セッション内） |

### 3.3.3 資格情報管理（credentials）

```python
# Azure Key Vault からシークレットを取得
secret_value = mssparkutils.credentials.getSecret(
    "https://my-keyvault.vault.azure.net/",  # Key Vault URI
    "my-secret-name"                          # シークレット名
)

# アクセストークンの取得
token = mssparkutils.credentials.getToken("https://storage.azure.com/")

# Fabric API 用トークン
fabric_token = mssparkutils.credentials.getToken("https://api.fabric.microsoft.com/")
```

### 3.3.4 ランタイムコンテキスト

```python
# 実行コンテキスト情報の取得（旧 mssparkutils.env は非推奨）
import notebookutils

context = notebookutils.runtime.context
print(f"Workspace ID: {context['currentWorkspaceId']}")
print(f"Lakehouse ID: {context['defaultLakehouseId']}")
print(f"Notebook Name: {context.get('currentNotebookName', 'N/A')}")
```

---

## 3.4 ノートブック間の連携パターン

### 3.4.1 パイプラインからのノートブック呼び出し

パイプラインの Notebook アクティビティからノートブックを呼び出す場合、パラメータセルで定義した変数をパイプラインから上書きできる。

```python
# ノートブック側: パラメータセル（セルにタグ「parameters」を設定）
load_date = "2024-01-01"    # デフォルト値
source_schema = "bronze"
target_schema = "silver"

# パイプラインからの呼び出し時に load_date = "@pipeline().TriggerTime" 等で上書き
```

```json
// パイプラインの Notebook アクティビティ設定
{
    "type": "TridentNotebook",
    "notebook": "nb_transform_silver",
    "parameters": {
        "load_date": {
            "value": "@formatDateTime(pipeline().TriggerTime, 'yyyy-MM-dd')",
            "type": "string"
        },
        "source_schema": {
            "value": "bronze",
            "type": "string"
        }
    }
}
```

### 3.4.2 参照ノートブックパターン

共通ユーティリティ関数を1つのノートブックに定義し、他ノートブックから `%run` で参照する。

```python
# ── utils_notebook（共通関数定義）──
def get_max_date(table_name: str) -> str:
    """テーブルの最大日付を取得（増分ロードのウォーターマーク）"""
    result = spark.sql(f"SELECT MAX(order_date) FROM {table_name}").collect()[0][0]
    return str(result) if result else "1900-01-01"

def log_execution(notebook_name: str, status: str, row_count: int) -> None:
    """実行ログをテーブルに記録"""
    spark.sql(f"""
        INSERT INTO audit.execution_log
        VALUES ('{notebook_name}', '{status}', {row_count}, current_timestamp())
    """)
```

```python
# ── main_notebook（呼び出し側）──
%run utils_notebook

# utils_notebook の関数が使える
watermark = get_max_date("silver.fact_orders")
print(f"Watermark: {watermark}")

# ... 処理 ...

log_execution("main_notebook", "success", df.count())
```

---

## 3.5 Spark 構造化ストリーミング `[取込変換]`

### 3.5.1 基本パターン

```python
# ── Delta テーブルをストリーミングソースとして読み取り ──
df_stream = (
    spark.readStream
    .format("delta")
    .option("ignoreChanges", "true")  # UPDATE/DELETE を無視（append のみ処理）
    .table("bronze.raw_events")
)

# ── 変換 ──
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

df_transformed = (
    df_stream
    .filter(col("event_type") == "purchase")
    .withColumn("event_ts", to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ss"))
    .select("event_id", "user_id", "event_ts", "amount")
)

# ── Delta テーブルにストリーミング書き込み ──
query = (
    df_transformed.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "Files/checkpoints/silver_events/")
    .toTable("silver.events")
)
```

### 3.5.2 トリガーの種類

```python
from pyspark.sql.streaming import Trigger

# ── マイクロバッチ（指定間隔で処理）──
query = df_stream.writeStream \
    .trigger(processingTime="30 seconds") \
    .format("delta") \
    .option("checkpointLocation", "Files/checkpoints/events/") \
    .toTable("silver.events")

# ── once（1回だけ実行して停止。バッチ的に使う）──
query = df_stream.writeStream \
    .trigger(once=True) \
    .format("delta") \
    .option("checkpointLocation", "Files/checkpoints/events/") \
    .toTable("silver.events")

# ── availableNow（現在利用可能な全データを処理して停止）──
query = df_stream.writeStream \
    .trigger(availableNow=True) \
    .format("delta") \
    .option("checkpointLocation", "Files/checkpoints/events/") \
    .toTable("silver.events")
```

| トリガー | 動作 | ユースケース |
|---------|------|------------|
| `processingTime` | 指定間隔でマイクロバッチ | 継続的なストリーム処理 |
| `once=True` | 1バッチだけ処理して停止 | スケジュール実行の増分ロード |
| `availableNow=True` | 現在の全未処理データを処理して停止 | 確実に全データを処理して停止 |

### 3.5.3 checkpointLocation

チェックポイントはストリーム処理の進捗状態（どのオフセットまで処理済みか）を保存する。

- **必須**: writeStream に `checkpointLocation` を指定しないとエラー
- **再開**: 同じチェックポイントパスで再起動すると、前回の続きから処理
- **リセット**: チェックポイントを削除して再起動すると、最初から再処理
- Snowflake の Streams のオフセット管理に概念的に対応

### 3.5.4 Spark Job Definition でのバックグラウンド実行

長時間実行のストリーミングジョブはノートブックではなく **Spark Job Definition** で実行するのが推奨。ノートブックはインタラクティブ開発向きで、セッションタイムアウトがある。

```python
# Spark Job Definition 用の Python スクリプト例
# spark_streaming_job.py
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df_stream = spark.readStream.format("delta").table("bronze.raw_events")
df_transformed = df_stream.filter("event_type = 'purchase'")

query = (
    df_transformed.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "Files/checkpoints/events/")
    .trigger(processingTime="60 seconds")
    .toTable("silver.events")
)

query.awaitTermination()
```

### 3.5.5 Delta テーブルソースの制約

- **append のみ**: デフォルトでは Delta テーブルのストリーミングソースは append 操作のみ
- `ignoreChanges=true`: UPDATE/DELETE があってもエラーにせず、変更された行を再読み取り
- `ignoreDeletes=true`: DELETE のみ無視
- **ignoreChanges を指定しないと UPDATE/DELETE 時にエラー**になる（試験ポイント）

---

## 3.6 Spark パフォーマンス最適化 `[監視最適化]`

### 3.6.1 パーティション数の調整

```python
# シャッフルパーティション数（デフォルト 200）
spark.conf.set("spark.sql.shuffle.partitions", "100")  # データ量に応じて調整

# DataFrame のパーティション数確認
print(df.rdd.getNumPartitions())

# repartition: パーティション数を増やす（フルシャッフル発生）
df = df.repartition(8, "customer_id")

# coalesce: パーティション数を減らす（シャッフルなし、高速）
df = df.coalesce(4)
```

**判断基準**:
- パーティションが少なすぎ → 並列度が下がり遅い。各パーティションが大きすぎてメモリ不足のリスク
- パーティションが多すぎ → タスク起動のオーバーヘッド増。小さなファイル問題
- 目安: 各パーティション **128 MB〜1 GB** 程度になるよう調整

### 3.6.2 ブロードキャスト結合

```python
from pyspark.sql.functions import broadcast

# 小さいテーブル（ディメンション等）をブロードキャストして結合高速化
df_result = df_orders.join(
    broadcast(df_products),  # 各エグゼキューターにテーブル全体をコピー
    "product_id",
    "inner"
)
```

**ブロードキャスト結合の条件**:
- 小さい側のテーブルが `spark.sql.autoBroadcastJoinThreshold`（デフォルト 10MB）以下なら自動適用
- 明示的に `broadcast()` ヒントを指定可能
- ブロードキャストするテーブルが大きすぎるとドライバーの OOM リスク

### 3.6.3 キャッシュ戦略

```python
# キャッシュ（メモリに保持、複数回使うデータに有効）
df_customers = spark.read.format("delta").table("silver.dim_customer")
df_customers.cache()   # .persist(StorageLevel.MEMORY_AND_DISK) と同等
df_customers.count()   # キャッシュのトリガー（遅延評価のため明示的にアクション実行）

# 複数の join で使用
df_result1 = df_orders.join(df_customers, "customer_id")
df_result2 = df_returns.join(df_customers, "customer_id")

# キャッシュの解放
df_customers.unpersist()
```

**キャッシュすべき場面**: 同一 DataFrame を**2回以上**使う場合（JOIN、集計の繰り返し等）
**キャッシュすべきでない場面**: 一度しか使わないデータ、メモリが逼迫している場合

### 3.6.4 AQE（Adaptive Query Execution）

Fabric Runtime 1.2+ ではデフォルトで有効。実行時にクエリプランを動的に最適化する。

主な機能:
- **Coalesce shuffle partitions**: シャッフル後の小さなパーティションを自動統合
- **Skew join optimization**: 偏りのあるキーでの結合を自動最適化
- **Dynamic partition pruning**: 実行時にパーティションをプルーニング

```python
# AQE の確認（デフォルト有効）
print(spark.conf.get("spark.sql.adaptive.enabled"))  # true

# 無効化が必要な場面はほぼないが、テスト目的で
spark.conf.set("spark.sql.adaptive.enabled", "false")
```

### 3.6.5 Spark UI の読み方

Monitoring Hub → ノートブックの実行 → View Spark UI で確認。

| タブ | 確認内容 |
|------|---------|
| **Jobs** | ジョブ一覧と所要時間。失敗ジョブの特定 |
| **Stages** | 各ステージのタスク数、シャッフル量、データスキュー |
| **Storage** | キャッシュされた RDD/DataFrame のサイズ |
| **SQL/DataFrame** | クエリ実行プラン（Physical Plan） |
| **Environment** | Spark プロパティ、Runtime バージョン |

**ボトルネック特定のチェックリスト**:
1. **シャッフルが多い** → パーティションキーの見直し、ブロードキャスト結合の検討
2. **タスク間のデータスキュー** → AQE の skew join 確認、salting テクニック
3. **GC（ガベージコレクション）時間が長い** → メモリ不足。ノードサイズの拡大 or キャッシュ削減
4. **小さなタスクが大量** → coalesce でパーティション削減、Optimize Write 有効化

---

## 3.7 ノートブックエラーの特定と解決 `[監視最適化]`

### 3.7.1 一般的なエラーパターン

| エラー | 原因 | 対処 |
|-------|------|------|
| `OutOfMemoryError` (OOM) | データがメモリに収まらない | ノードサイズ拡大、パーティション数増加、不要キャッシュの解放、処理対象データの絞り込み |
| `AnalysisException: Table not found` | テーブル名の誤り、Lakehouse 未アタッチ | テーブル名確認、Lakehouse がノートブックにアタッチされているか確認 |
| `AnalysisException: cannot resolve column` | 列名の誤り、スキーマ変更 | `df.printSchema()` でスキーマ確認 |
| `Py4JJavaError` | JVM 側のエラー（シリアライゼーション失敗等） | スタックトレースの Java 部分を確認。UDF 内のエラーが多い |
| `StreamingQueryException` | ストリーミングジョブの失敗 | チェックポイントの状態確認、ソーステーブルのスキーマ変更確認 |
| セッションタイムアウト | 長時間の非アクティブ | Spark Job Definition での実行に切り替え |
| ライブラリ依存エラー | 環境にインストールされていないパッケージ | 環境アイテムにライブラリを追加して Publish |

### 3.7.2 デバッグ手順

```
1. エラーメッセージを確認
   ├── セル出力のエラーメッセージ（Python トレースバック）
   └── Spark UI → Stages → Failed タブ → エラー詳細

2. Spark UI で実行プランを確認
   ├── SQL/DataFrame タブ → Physical Plan で結合戦略を確認
   └── Stages タブ → タスクごとのデータ量・所要時間を確認

3. ログを確認
   ├── ドライバーログ: Spark UI → Executors → Driver → stderr
   └── エグゼキューターログ: 各エグゼキューターの stderr

4. データを確認
   ├── df.printSchema() → スキーマの確認
   ├── df.show(5) → サンプルデータの確認
   └── df.count() → 行数の確認（想定外に多い/少ないか）
```

### 3.7.3 冪等性の確保

パイプラインからノートブックを呼び出す場合、リトライ時に重複データが発生しないよう冪等性を確保する。

```python
# パターン1: overwrite モード（テーブル全体を置換）
df.write.format("delta").mode("overwrite").saveAsTable("silver.fact_orders")

# パターン2: MERGE による upsert（ビジネスキーで重複排除）
from delta.tables import DeltaTable

target = DeltaTable.forName(spark, "silver.fact_orders")
target.alias("tgt").merge(
    df.alias("src"),
    "tgt.order_id = src.order_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

# パターン3: replaceWhere（パーティション単位の上書き）
df.write.format("delta") \
    .mode("overwrite") \
    .option("replaceWhere", "order_date = '2024-01-15'") \
    .saveAsTable("silver.fact_orders")
```

---

## 3.8 確認問題

### Q1. Spark プール

Starter Pool と Custom Pool について正しいものはどれか？

A) Starter Pool のノードサイズは Small に固定されている
B) Custom Pool の作成にはワークスペース管理者ロールが必要で、容量管理者の設定も必要
C) Starter Pool は Custom Pool よりも起動が遅い
D) Custom Pool は Autoscale をサポートしない

<details>
<summary>解答</summary>

**B) Custom Pool の作成にはワークスペース管理者ロールが必要で、容量管理者の設定も必要**

- A は不正解: Starter Pool は **Medium** 固定
- C は不正解: Starter Pool は常時起動で **5–10秒**。Custom Pool は約3分
- D は不正解: Custom Pool は Autoscale をサポートする
</details>

### Q2. ノートブック連携

`%run` と `mssparkutils.notebook.run` の違いについて正しいものはどれか？

A) `%run` はパラメータを渡せるが、`mssparkutils.notebook.run` は渡せない
B) `mssparkutils.notebook.run` は呼び出し先の Python 変数を呼び出し元と共有する
C) `mssparkutils.notebook.run` は exit value を返すことができる
D) `%run` は非同期実行される

<details>
<summary>解答</summary>

**C) `mssparkutils.notebook.run` は exit value を返すことができる**

- A は逆: `%run` はパラメータ不可、`mssparkutils.notebook.run` はパラメータ可
- B は不正解: `mssparkutils.notebook.run` はカプセル化されており Python 変数は共有されない（SparkSession は共有）
- D は不正解: `%run` は同期実行
</details>

### Q3. 構造化ストリーミング

Delta テーブルをストリーミングソースとして使用する場合、ソーステーブルに UPDATE が実行されるとどうなるか？

A) 自動的に変更行が処理される
B) `ignoreChanges` オプションを指定していなければエラーになる
C) UPDATE は無視され、INSERT のみ処理される
D) ストリーミングクエリが自動的に再起動される

<details>
<summary>解答</summary>

**B) `ignoreChanges` オプションを指定していなければエラーになる**

Delta テーブルのストリーミングソースはデフォルトで append のみをサポート。UPDATE や DELETE があると `StreamingQueryException` が発生する。`ignoreChanges=true` を指定すると、変更されたファイル内の行が再読み取りされる。
</details>

### Q4. パフォーマンス最適化

10 GB のファクトテーブルと 50 MB のディメンションテーブルを結合する。最も効果的な最適化はどれか？

A) ファクトテーブルを repartition で再分散する
B) ディメンションテーブルに broadcast ヒントを適用する
C) 両テーブルをキャッシュする
D) シャッフルパーティション数を増やす

<details>
<summary>解答</summary>

**B) ディメンションテーブルに broadcast ヒントを適用する**

50 MB のテーブルは broadcast に適したサイズ。各エグゼキューターにディメンションテーブル全体をコピーすることで、シャッフルを完全に回避できる。これが大小テーブル結合で最も効果的な最適化。
</details>

### Q5. 環境アイテム

ノートブックでのみ一時的にライブラリを使用したい場合、最適な方法はどれか？

A) 環境アイテムに Public Library として追加し、Publish する
B) ワークスペース設定の Spark properties にライブラリパスを追加する
C) ノートブック内で `%pip install` を実行する
D) Custom Library として .whl ファイルをアップロードする

<details>
<summary>解答</summary>

**C) ノートブック内で `%pip install` を実行する**

`%pip install` はセッション限りのインストールで、一時的な使用に最適。環境アイテムへの追加は Publish が必要で全セッションに影響する。一時的な利用には不適切。
</details>

---

> **次章**: 第4章 Dataflow Gen2 と Power Query M（Power Query M 言語、デスティネーション設定、増分更新、エラー特定）
