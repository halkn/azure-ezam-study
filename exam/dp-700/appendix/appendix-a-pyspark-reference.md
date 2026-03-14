# 付録 A：PySpark クイックリファレンス

> 試験中に MS Learn で構文を探す時間を短縮するためのチートシート

---

## A.1 読み込み / 書き込み

```python
# ── Delta テーブル ──
df = spark.read.format("delta").table("schema_name.table_name")
df = spark.read.format("delta").load("Tables/table_name")
df = spark.read.format("delta").option("versionAsOf", 3).load("Tables/table_name")
df = spark.read.format("delta").option("timestampAsOf", "2024-06-15").load("Tables/table_name")

# ── CSV ──
df = spark.read.format("csv").option("header","true").option("inferSchema","true").load("Files/raw/data.csv")

# ── JSON ──
df = spark.read.format("json").option("multiLine","true").load("Files/raw/data.json")

# ── Parquet ──
df = spark.read.format("parquet").load("Files/raw/data.parquet")

# ── 書き込み ──
df.write.format("delta").mode("overwrite").saveAsTable("schema_name.table_name")        # マネージド
df.write.format("delta").mode("append").saveAsTable("schema_name.table_name")            # 追加
df.write.format("delta").mode("overwrite").option("replaceWhere","date='2024-01-15'").saveAsTable("t")  # 条件付き上書き
df.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("t")  # スキーマ上書き
df.write.format("delta").mode("append").option("mergeSchema","true").saveAsTable("t")         # スキーママージ
df.write.format("delta").mode("overwrite").partitionBy("order_date").saveAsTable("t")         # パーティション付き
df.write.format("delta").mode("overwrite").option("parquet.vorder.enabled","true").saveAsTable("t")  # V-Order
```

**mode**: `overwrite` | `append` | `ignore` | `errorIfExists`

---

## A.2 基本変換

```python
from pyspark.sql.functions import (
    col, lit, when, otherwise, coalesce,
    upper, lower, trim, regexp_replace, substring, concat, concat_ws,
    to_date, to_timestamp, date_format, datediff, date_add, current_timestamp,
    cast, round as _round,
    count, sum as _sum, avg, min as _min, max as _max, countDistinct, collect_list, collect_set,
    row_number, rank, dense_rank, lag, lead,
    explode, from_json, get_json_object,
    input_file_name, monotonically_increasing_id,
    broadcast
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType, TimestampType

# 列選択 / 追加 / 削除
df.select("col1", "col2")
df.select(col("col1").alias("new_name"))
df.withColumn("new_col", col("amount") * lit(1.1))
df.withColumnRenamed("old", "new")
df.drop("temp_col")

# フィルタ
df.filter(col("amount") > 0)
df.filter((col("status") == "active") & (col("region").isin("East","West")))
df.filter(col("name").isNotNull())

# 型変換
df.withColumn("amount", col("amount").cast("decimal(18,2)"))
df.withColumn("order_date", to_date(col("date_str"), "yyyy-MM-dd"))

# 条件分岐
df.withColumn("tier", when(col("amount")>=10000,"gold").when(col("amount")>=1000,"silver").otherwise("bronze"))

# NULL 処理
df.fillna({"amount": 0, "name": "Unknown"})
df.withColumn("email", coalesce(col("primary_email"), col("backup_email"), lit("none")))

# 文字列操作
df.withColumn("name_upper", upper(trim(col("name"))))
df.withColumn("phone_clean", regexp_replace(col("phone"), "[^0-9]", ""))
```

---

## A.3 結合 / 集計 / ウィンドウ

```python
# ── JOIN ──
df_result = df_a.join(df_b, "key_col", "inner")                # inner
df_result = df_a.join(df_b, col("a.id")==col("b.id"), "left")  # left
df_result = df_a.join(df_b, "key_col", "left_anti")            # NOT EXISTS
df_result = df_a.join(broadcast(df_small), "key_col", "inner") # broadcast

# ── 集計 ──
df.groupBy("region","category").agg(
    count("*").alias("cnt"),
    _sum("amount").alias("total"),
    avg("amount").alias("avg_val"),
    countDistinct("customer_id").alias("uniq_cust")
)

# ── PIVOT ──
df.groupBy("region").pivot("category", ["A","B","C"]).agg(_sum("amount"))

# ── ウィンドウ関数 ──
w = Window.partitionBy("customer_id").orderBy(col("order_date").desc())
df.withColumn("rn", row_number().over(w))           # 重複排除
df.withColumn("prev_amt", lag("amount",1).over(w))   # 前行
df.withColumn("running", _sum("amount").over(
    Window.partitionBy("region").orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
))
```

---

## A.4 Delta 固有操作

```python
from delta.tables import DeltaTable

# ── MERGE (upsert) ──
target = DeltaTable.forName(spark, "schema.table_name")
target.alias("t").merge(
    df_source.alias("s"), "t.id = s.id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

# ── テーブル作成 ──
DeltaTable.create(spark).tableName("products") \
    .addColumn("id","INT").addColumn("name","STRING").addColumn("price","DECIMAL(10,2)") \
    .execute()

# ── 履歴 / 復元 ──
# spark.sql("DESCRIBE HISTORY schema.table_name")
# spark.sql("RESTORE TABLE schema.table_name TO VERSION AS OF 5")
```

```sql
-- Spark SQL
OPTIMIZE schema.table_name;
OPTIMIZE schema.table_name ZORDER BY (col1, col2);
VACUUM schema.table_name RETAIN 168 HOURS;
```

---

## A.5 NotebookUtils (旧 mssparkutils)

```python
from notebookutils import mssparkutils

# ファイル操作
mssparkutils.fs.ls("Files/raw/")
mssparkutils.fs.cp("Files/a.csv", "Files/b.csv")
mssparkutils.fs.cp("Files/src/", "Files/dst/", recurse=True)
mssparkutils.fs.mv("Files/old.csv", "Files/archive/old.csv")
mssparkutils.fs.rm("Files/temp/", recurse=True)
mssparkutils.fs.mkdirs("Files/output/2024/01/")
mssparkutils.fs.put("Files/log.txt", "done", overwrite=True)
mssparkutils.fs.head("Files/data.csv", maxBytes=1024)

# ノートブック連携
result = mssparkutils.notebook.run("nb_name", timeout_seconds=600, arguments={"key":"val"})
mssparkutils.notebook.runMultiple(["nb1","nb2","nb3"])  # 並列実行
mssparkutils.notebook.exit("success")                    # 終了値の設定

# 資格情報
secret = mssparkutils.credentials.getSecret("https://kv.vault.azure.net/", "secret-name")
token = mssparkutils.credentials.getToken("https://storage.azure.com/")

# ランタイムコンテキスト
import notebookutils
ctx = notebookutils.runtime.context  # currentWorkspaceId, defaultLakehouseId 等
```

---

## A.6 構造化ストリーミング

```python
# 読み取り
df_stream = spark.readStream.format("delta").option("ignoreChanges","true").table("bronze.events")

# 書き込み
query = df_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "Files/checkpoints/silver_events/") \
    .trigger(availableNow=True) \
    .toTable("silver.events")
```

**トリガー**: `processingTime="30 seconds"` | `once=True` | `availableNow=True`

---

## A.7 Spark 設定

```python
# シャッフルパーティション
spark.conf.set("spark.sql.shuffle.partitions", "200")

# V-Order
spark.conf.set("spark.sql.parquet.vorder.default", "true")

# Optimize Write
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")

# Auto Compaction
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

# AQE（デフォルト有効）
spark.conf.get("spark.sql.adaptive.enabled")

# パーティション数確認
df.rdd.getNumPartitions()
df.repartition(8, "key_col")   # フルシャッフル
df.coalesce(4)                  # シャッフルなし（縮小のみ）
```
