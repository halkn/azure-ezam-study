# 第0章 Fabric アーキテクチャ概論

> 📘 対応 MS Learn モジュール: Introduction to end-to-end analytics using Microsoft Fabric
> 試験タグ: `[実装管理]` `[取込変換]`

---

## 0.1 Microsoft Fabric とは

Microsoft Fabric は、データの取り込みから変換・格納・分析・可視化までを単一の SaaS プラットフォーム上で提供する統合分析基盤である。従来 Azure 上で個別に構築・運用していた複数サービス（Azure Data Factory, Azure Synapse Analytics, Azure Data Lake Storage, Power BI）が、Fabric では1つの統合環境として提供される。

### 0.1.1 SaaS としての Fabric

Fabric は PaaS ではなく SaaS である。この違いは試験でも問われるポイントになる。

| 観点 | PaaS（Azure Synapse 等） | SaaS（Microsoft Fabric） |
|------|--------------------------|--------------------------|
| インフラ管理 | ユーザーがリソースプールやスケーリングを管理 | Microsoft が管理。容量単位で課金 |
| プロビジョニング | 明示的にサービスを作成・構成 | ワークスペース内でアイテムを作成するだけ |
| ストレージ | ストレージアカウントを個別作成 | OneLake が自動プロビジョニング |
| セキュリティ | ネットワーク・IAM を個別構成 | テナントレベルのガバナンスが自動適用 |
| 統合 | サービス間を手動で接続 | 同一プラットフォーム内で自動統合 |

Snowflake との対比で言えば、Fabric も Snowflake と同様にコンピュートとストレージが分離された SaaS モデルだが、Fabric はストレージ層（OneLake）が全ワークロードで共有される点が大きく異なる。Snowflake のウェアハウスごとに独立したコンピュートリソースを割り当てるモデルに対し、Fabric は1つの容量プール（Capacity Unit）を全ワークロードが共有する。

### 0.1.2 Fabric を構成するワークロード

Fabric は以下のワークロード（エクスペリエンス）で構成される。

| ワークロード | 主な役割 | 試験での出題頻度 |
|------------|---------|----------------|
| Data Factory | パイプライン、Dataflow Gen2 によるデータ取り込み・オーケストレーション | 高 |
| Data Engineering | Lakehouse、Notebook（PySpark）、Spark ジョブ定義 | 高 |
| Data Warehouse | フル T-SQL によるリレーショナルデータウェアハウス | 高 |
| Real-Time Intelligence | Eventhouse、KQL DB、Eventstream によるリアルタイム分析 | 高 |
| Power BI | セマンティックモデル、レポート、ダッシュボード | 低（DP-600 の範囲） |
| Data Science | ML モデル、実験 | 低（DP-700 の範囲外） |
| Data Activator | イベント駆動のアラートとアクション | 中（アラート構成として出題） |

DP-700 では Power BI のレポート作成や DAX は範囲外だが、セマンティックモデルの更新監視や Direct Lake モードは出題される。

---

## 0.2 OneLake

OneLake は Fabric のストレージ基盤であり、テナントに1つだけ存在する論理データレイクである。

### 0.2.1 アーキテクチャ

OneLake は ADLS Gen2（Azure Data Lake Storage Gen2）の上に構築されている。既存の ADLS Gen2 API・SDK との互換性があり、Azure Databricks や他のツールからも直接アクセスできる。

```
OneLake の階層構造:

テナント (= OneLake のルート)
  └── ワークスペース A (= ADLS のコンテナに相当)
  │     ├── Lakehouse_Sales
  │     │     ├── Tables/          ← マネージド Delta テーブル
  │     │     │     ├── dim_customer/
  │     │     │     ├── fact_orders/
  │     │     │     └── ...
  │     │     └── Files/           ← 非構造化ファイル（CSV, JSON, 画像等）
  │     │           ├── raw/
  │     │           └── staging/
  │     ├── Warehouse_DWH
  │     │     └── (内部的に Delta Parquet で格納)
  │     └── Eventhouse_RTI
  │           └── (KQL DB のデータ)
  └── ワークスペース B
        └── ...
```

重要なポイント:

- **1テナント = 1 OneLake**: 組織全体で単一のデータレイクを共有する。Snowflake のように DB ごとにストレージが分かれるモデルではない。
- **Delta Parquet が標準フォーマット**: Lakehouse の Tables/、Warehouse のデータはすべて Delta Parquet 形式で OneLake に格納される。これにより、Lakehouse に Spark で書き込んだデータを Warehouse の T-SQL から読み取る（逆も同様）ことが可能。
- **ADLS Gen2 互換**: `abfss://` プロトコルでアクセス可能。OneLake 全体を1つの巨大な ADLS ストレージアカウントとして扱える。

### 0.2.2 OneLake のアドレス指定

OneLake 上のデータは以下の形式でアドレス指定する。

```
# ADLS Gen2 互換の URI
abfss://<workspace-name>@onelake.dfs.fabric.microsoft.com/<item-name>/Tables/<table-name>/

# OneLake file explorer (Windows) からのパス
OneLake - <Tenant Name>/<Workspace Name>/<Item Name>/Tables/<Table Name>/
```

PySpark ノートブックからのアクセス例:

```python
# 同一ワークスペース内の Lakehouse テーブル読み込み（デフォルト Lakehouse がアタッチされている場合）
df = spark.read.format("delta").load("Tables/fact_orders")

# 別ワークスペースの Lakehouse テーブル読み込み（フルパス指定）
df = spark.read.format("delta").load(
    "abfss://workspace-b@onelake.dfs.fabric.microsoft.com/Lakehouse_Sales/Tables/dim_customer"
)

# Files フォルダからの CSV 読み込み
df = spark.read.format("csv").option("header", "true").load("Files/raw/sales_2024.csv")
```

### 0.2.3 OneLake ストレージの課金

OneLake ストレージは容量（F SKU）とは別に課金される。

- **計算式**: 格納データ量 × 単価（リージョンにより異なる）
- **ミラーリング無料枠**: 購入した SKU の数字分の TB が無料。例: F64 → 64 TB まで無料
- **容量を一時停止（Pause）した場合**: ストレージ課金は継続する（コンピュートのみ停止）

---

## 0.3 ワークスペース

ワークスペースは Fabric におけるアイテムのコンテナであり、コラボレーションとアクセス制御の基本単位である。

### 0.3.1 ワークスペースと容量の関係

```
テナント
  ├── 容量 A (F64, East US)
  │     ├── ワークスペース: Sales-Dev
  │     ├── ワークスペース: Sales-Test
  │     └── ワークスペース: Sales-Prod
  ├── 容量 B (F32, West Europe)
  │     └── ワークスペース: Marketing
  └── 共有容量 (Pro/PPU)
        └── My Workspace (各ユーザーの個人用)
```

- **容量（Capacity）**: コンピュートリソースのプール。F SKU で指定。1つの容量に複数のワークスペースを割り当てられる。
- **ワークスペース**: アイテム（Lakehouse, Warehouse, Pipeline 等）を格納するフォルダのような存在。
- **ワークスペースの割り当て**: 1ワークスペースは常に1つの容量に属する。別の容量への再割り当ても可能。

### 0.3.2 ワークスペースロール

ワークスペースには4つのロールがあり、試験では頻出。

| ロール | OneLake データアクセス | アイテム作成・編集 | アイテム削除 | 権限管理 | ワークスペース設定変更 |
|-------|---------------------|-----------------|-----------|---------|-------------------|
| Admin | 全データ読み書き | ○ | ○ | ○ | ○ |
| Member | 全データ読み書き | ○ | ○ | △（共有のみ） | × |
| Contributor | 全データ読み書き | ○ | × | × | × |
| Viewer | OneLake セキュリティロールに依存 | × | × | × | × |

**試験ポイント**:
- Admin / Member / Contributor は OneLake セキュリティロールの影響を受けない（全データ読み書き可能）
- Viewer のデータアクセスは OneLake セキュリティロールで制御する
- DefaultReader ロール（全 Lakehouse に自動作成）を削除・編集すると、ReadAll 権限を持つユーザーでもデータにアクセスできなくなる

### 0.3.3 ドメイン

ドメインはワークスペースを論理的にグルーピングする仕組みで、データメッシュアーキテクチャを実現する。

- テナント管理者またはドメイン管理者がドメインを作成
- 複数のワークスペースを1つのドメインに割り当てる
- ドメインレベルでの承認（Certification）ポリシーを設定可能
- OneLake カタログ上でドメイン別にアイテムを探索可能

```
ドメイン: Sales
  ├── ワークスペース: Sales-Raw
  ├── ワークスペース: Sales-Curated
  └── ワークスペース: Sales-BI

ドメイン: Marketing
  └── ワークスペース: Marketing-Analytics
```

---

## 0.4 Fabric の主要アイテムと選択基準

### 0.4.1 各アイテムの概要

#### Lakehouse

- Spark エンジン（PySpark / SQL / R）と SQL 分析エンドポイントの両方でデータにアクセス
- Tables/ フォルダにマネージド Delta テーブル、Files/ フォルダに非構造化ファイルを格納
- SQL 分析エンドポイントは読み取り専用（SELECT のみ。INSERT / UPDATE / DELETE 不可）
- メダリオンアーキテクチャの実装に最適

```python
# Lakehouse で Spark を使ってデータを書き込む例
df_cleaned = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("Files/raw/sales_2024.csv")
    .dropDuplicates(["order_id"])
    .withColumn("load_ts", current_timestamp())
)

# Delta テーブルとして保存（Tables/ 配下にマネージドテーブルとして作成）
df_cleaned.write.format("delta").mode("overwrite").saveAsTable("bronze_sales")
```

#### Warehouse

- フル T-SQL サポート（DDL, DML, ストアドプロシージャ, 関数, ビュー）
- INSERT, UPDATE, DELETE, MERGE が実行可能
- 内部的にはデータは Delta Parquet 形式で OneLake に格納
- RLS / CLS / DDM（動的データマスク）をネイティブサポート

```sql
-- Warehouse でのテーブル作成と MERGE による増分ロード
CREATE TABLE dbo.dim_customer (
    customer_sk  BIGINT IDENTITY(1,1) NOT NULL,
    customer_id  INT NOT NULL,
    customer_name NVARCHAR(200),
    email        NVARCHAR(200),
    is_current   BIT DEFAULT 1,
    valid_from   DATETIME2 DEFAULT GETDATE(),
    valid_to     DATETIME2 DEFAULT '9999-12-31'
);

-- MERGE による SCD Type 1 更新
MERGE INTO dbo.dim_customer AS tgt
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

#### Eventhouse / KQL DB

- 時系列データ・ストリーミングデータに最適化されたストア
- Kusto Query Language (KQL) でクエリ
- ミリ秒〜秒レベルのレイテンシでの取り込みが可能
- Eventstream からのリアルタイムデータ取り込みに対応

```kql
// KQL による時系列集計の例
SensorReadings
| where Timestamp > ago(1h)
| where DeviceType == "TemperatureSensor"
| summarize
    avg_temp = avg(Temperature),
    max_temp = max(Temperature),
    reading_count = count()
    by bin(Timestamp, 5m), Location
| order by Timestamp desc
| render timechart
```

#### Notebook

- PySpark, Spark SQL, R のインタラクティブ実行環境
- Lakehouse をデフォルトでアタッチ
- mssparkutils / notebookutils API で OneLake のファイル操作や他ノートブック呼び出しが可能
- パイプラインからパラメータ付きで呼び出し可能

```python
# ノートブックでのパラメータ受け取り（パイプラインから渡される）
# セルにタグ「parameters」を設定すると、パイプラインからのパラメータで上書きされる
load_date = "2024-01-15"  # デフォルト値。パイプラインから渡された場合は上書き

# mssparkutils によるファイル操作
from notebookutils import mssparkutils

# ファイル一覧の取得
files = mssparkutils.fs.ls("Files/raw/")
for f in files:
    print(f"{f.name} ({f.size} bytes)")

# ノートブック間呼び出し（戻り値を取得）
result = mssparkutils.notebook.run("transform_silver", timeout_seconds=600, arguments={
    "source_table": "bronze_sales",
    "target_table": "silver_sales"
})
print(f"Notebook returned: {result}")
```

#### Pipeline

- ADF 互換のオーケストレーションエンジン
- Copy, Dataflow, Notebook, Stored Procedure, KQL 等のアクティビティ
- パラメータ、動的式、制御フロー（ForEach, If Condition, Switch 等）
- スケジュールトリガーとイベントベーストリガー

```json
// パイプラインの動的式の例（ADF 式言語互換）
// ファイルパスに実行日を埋め込む
{
    "source": {
        "filePath": "@concat('raw/', formatDateTime(pipeline().TriggerTime, 'yyyy/MM/dd'), '/sales.csv')"
    }
}

// Notebook アクティビティにパラメータを渡す
{
    "activity": "RunNotebook",
    "type": "TridentNotebook",
    "parameters": {
        "load_date": {
            "value": "@formatDateTime(pipeline().TriggerTime, 'yyyy-MM-dd')",
            "type": "string"
        }
    }
}
```

#### Dataflow Gen2

- Power Query (M言語) ベースの GUI データ変換ツール
- デスティネーションとして Lakehouse テーブルまたは Warehouse テーブルを指定可能
- 増分更新に対応
- ノーコード/ローコードでデータ変換が可能

```m
// Power Query M 言語の例: CSV の読み込みと変換
let
    Source = Csv.Document(
        Web.Contents("https://example.com/sales.csv"),
        [Delimiter = ",", Encoding = 65001, QuoteStyle = QuoteStyle.Csv]
    ),
    PromotedHeaders = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
    ChangedTypes = Table.TransformColumnTypes(PromotedHeaders, {
        {"OrderDate", type date},
        {"Amount", type number},
        {"Quantity", Int64.Type}
    }),
    FilteredRows = Table.SelectRows(ChangedTypes, each [Amount] > 0),
    AddedColumn = Table.AddColumn(FilteredRows, "Revenue", each [Amount] * [Quantity], type number)
in
    AddedColumn
```

#### Data Workflow（Apache Airflow in Fabric）

- Apache Airflow のマネージド版
- DAG（Directed Acyclic Graph）による複雑なオーケストレーション
- Pipeline よりも柔軟な依存関係・条件分岐が必要な場合に使用
- Fabric 固有のオペレーター（Lakehouse, Notebook 等）が利用可能

### 0.4.2 アイテム選択の判断基準

試験では「適切なアイテムを選択する」シナリオ問題が頻出する。以下のフローチャートで判断する。

```
データの特性は？
├── バッチデータ
│   ├── 非構造化/半構造化データが多い → Lakehouse
│   ├── リレーショナルモデリングが必要 → Warehouse
│   ├── 非構造化 + リレーショナル両方 → Lakehouse（メダリオン → Gold で Warehouse に出力）
│   └── 外部 DB のレプリカが必要 → ミラーリング → Lakehouse/Warehouse
│
└── ストリーミングデータ
    ├── ミリ秒〜秒のレイテンシ要件 → Eventhouse + Eventstream
    ├── 分〜時間のレイテンシで良い → Spark 構造化ストリーミング → Lakehouse
    └── KQL でのアドホック分析が必要 → Eventhouse
```

変換ツールの選択:

| シナリオ | 推奨ツール |
|---------|-----------|
| ノーコードで GUI 操作したい | Dataflow Gen2 (Power Query M) |
| 大量データの複雑な変換 | Notebook (PySpark) |
| リレーショナルデータの変換（JOIN, MERGE, CTE） | Warehouse (T-SQL) |
| 時系列データの集計・分析 | Eventhouse (KQL) |
| 複数ツールの逐次実行 | Pipeline（オーケストレーション） |
| 複雑な依存関係・条件分岐のオーケストレーション | Data Workflow (Airflow) |
| 単純なコピー（A → B の移動） | Pipeline の Copy アクティビティ |

---

## 0.5 容量とライセンス

### 0.5.1 Capacity Unit (CU) と F SKU

Fabric のコンピュートリソースは **Capacity Unit (CU)** で測定される。CU は CPU・メモリ・I/O を統合した抽象単位で、すべてのワークロード（Spark, SQL, KQL, Dataflow 等）が同じ CU プールを共有する。

| SKU | CU数 | 旧 Power BI 対応 | 主な用途 |
|-----|------|-----------------|---------|
| F2 | 2 | - | 開発・PoC |
| F4 | 4 | - | 開発・小規模テスト |
| F8 | 8 | - | 小規模本番 |
| F16 | 16 | - | 小規模本番 |
| F32 | 32 | - | 中規模本番 |
| F64 | 64 | P1 相当 | 本番（Pro ライセンス不要の境界） |
| F128 | 128 | P2 相当 | 大規模本番 |
| F256 | 256 | P3 相当 | エンタープライズ |
| F512〜F2048 | 512〜2048 | - | 大規模エンタープライズ |

**試験ポイント**:
- **F64 は重要な境界**: F64 以上では Power BI の閲覧に Pro ライセンスが不要になる（Viewer ロールの場合）
- **CU はワークロード間で共有**: Spark ジョブが大量の CU を消費すると、同じ容量上のパイプラインや Power BI の更新に影響する
- **バースト**: 短時間の CU 超過は「スムージング」で吸収されるが、持続すると**スロットリング**（ジョブのキューイング/遅延）が発生する
- **一時停止（Pause）**: コンピュート課金は停止するが、OneLake ストレージ課金は継続する

### 0.5.2 ライセンスの種類

| ライセンス | 条件 | できること |
|-----------|------|----------|
| Fabric (Free) | テナント内で Fabric が有効なら自動付与 | F 容量上の非 Power BI アイテムの作成・利用 |
| Power BI Pro | 有償（月額/年額） | Power BI コンテンツの作成・共有（F64 未満では閲覧にも必要） |
| PPU (Premium Per User) | 有償 | Pro + Premium 機能（大規模データセット等）個人用 |
| 容量ライセンス (F SKU) | Azure サブスクリプションで購入 | Fabric 全機能へのアクセス（コンピュート+ストレージ） |

**DP-700 の試験ではライセンスの深い知識は問われにくい**が、「F64 以上なら Pro 不要」「容量の一時停止でストレージ課金は続く」程度は押さえておく。

### 0.5.3 Trial 容量

学習目的で Fabric を使う場合、60日間の無料トライアル（F64 相当）が利用可能。

- Microsoft Entra ID（旧 Azure AD）のアカウントが必要
- 個人の Microsoft アカウント（outlook.com 等）では利用不可
- トライアル終了後はワークスペースとアイテムは残るがアクセス不可になる
- URL: https://learn.microsoft.com/fabric/fundamentals/fabric-trial

---

## 0.6 データフロー全体図

### 0.6.1 エンドツーエンドのデータパイプライン

Fabric でのデータの流れを全体像として示す。

```
外部データソース                    Fabric 内部                           消費
─────────────                   ──────────                          ────
                                 ┌─────────────┐
Azure SQL DB  ──── ミラーリング ──→│             │
Cosmos DB     ──── ミラーリング ──→│  OneLake    │
Snowflake     ──── ミラーリング ──→│             │
                                 │  ┌────────┐ │
S3 / ADLS Gen2 ── ショートカット──→│  │ Bronze │ │
                                 │  └───┬────┘ │    ┌──────────────┐
CSV / API     ── Copy / Dataflow→│      │      │    │  Power BI    │
                                 │  ┌───▼────┐ │    │  (Direct     │
Event Hubs  ──── Eventstream ───→│  │ Silver │ │───→│   Lake /     │
IoT Hub     ──── Eventstream ───→│  └───┬────┘ │    │   Import)    │
                                 │      │      │    └──────────────┘
                                 │  ┌───▼────┐ │    ┌──────────────┐
                                 │  │  Gold  │ │───→│  Warehouse   │
                                 │  └────────┘ │    │  (T-SQL)     │
                                 │             │    └──────────────┘
                                 │  ┌────────┐ │    ┌──────────────┐
                                 │  │Eventhse│ │───→│  RTI         │
                                 │  │(KQL DB)│ │    │  Dashboard   │
                                 │  └────────┘ │    └──────────────┘
                                 └─────────────┘
```

### 0.6.2 メダリオンアーキテクチャ概要

Lakehouse でのデータ管理の標準パターンとしてメダリオンアーキテクチャ（Bronze / Silver / Gold）が推奨される。詳細は第2章で扱うが、概念を先に押さえておく。

| レイヤー | 目的 | データ特性 | 変換レベル |
|---------|------|-----------|-----------|
| Bronze（Raw） | 生データの着地 | ソースのまま。append-only | なし〜最小限（スキーマ付与程度） |
| Silver（Cleansed） | クレンジング・統合 | 型変換済み、重複排除済み、結合済み | 中（フィルタ、クレンジング、JOIN） |
| Gold（Curated） | ビジネスレベルの分析モデル | 集計済み、ディメンションモデル | 高（集計、非正規化、ビジネスロジック） |

Snowflake の dbt プロジェクトで `staging → intermediate → marts` のレイヤリングをしている場合、Bronze ≒ staging, Silver ≒ intermediate, Gold ≒ marts に概念的に対応する。

各レイヤーを別々の Lakehouse として分離するか、1つの Lakehouse 内でスキーマ（フォルダ）を分けるかは設計上の選択であり、試験でもシナリオ問題として問われる可能性がある。

```python
# メダリオンアーキテクチャのデータフロー例

# Bronze: 生データの取り込み（append-only）
df_raw = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("Files/raw/orders_20240115.csv")
)
df_raw.withColumn("_ingestion_ts", current_timestamp()) \
      .write.format("delta").mode("append").saveAsTable("bronze_orders")

# Silver: クレンジング・型変換
df_silver = (
    spark.read.format("delta").table("bronze_orders")
    .dropDuplicates(["order_id"])
    .filter(col("order_date").isNotNull())
    .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
    .withColumn("amount", col("amount").cast("decimal(18,2)"))
)
df_silver.write.format("delta").mode("overwrite").saveAsTable("silver_orders")

# Gold: ビジネスレベルの集計
df_gold = (
    spark.read.format("delta").table("silver_orders")
    .groupBy("region", "product_category", "order_date")
    .agg(
        count("order_id").alias("order_count"),
        sum("amount").alias("total_revenue"),
        avg("amount").alias("avg_order_value")
    )
)
df_gold.write.format("delta").mode("overwrite").saveAsTable("gold_daily_sales")
```

---

## 0.7 Fabric と Azure サービスの関係

試験では「Fabric の機能として正しいもの」「Azure 側で管理が必要なもの」の区別が問われることがある。

| 機能 | Fabric 内で完結？ | Azure 側が必要な場面 |
|------|-----------------|-------------------|
| データの格納（OneLake） | ○ | - |
| Spark ジョブの実行 | ○ | - |
| T-SQL クエリの実行 | ○ | - |
| KQL クエリの実行 | ○ | - |
| パイプラインの実行 | ○ | - |
| 容量の購入・スケーリング | △ | Azure ポータルで F SKU を作成 |
| ストリーミングソース | △ | Azure Event Hubs / IoT Hub が外部ソースとして必要 |
| Key Vault でのシークレット管理 | △ | Azure Key Vault を参照する場合 |
| オンプレミスへの接続 | △ | On-premises data gateway が必要 |
| Private Link / VNET | △ | Azure ネットワーク設定が必要 |
| Purview による高度なガバナンス | △ | Microsoft Purview との統合 |
| Git 連携 | △ | Azure DevOps または GitHub リポジトリ |

Snowflake + Azure の構成で言えば、Key Vault によるシークレット管理や VNET のネットワーク構成は引き続き Azure 側の作業だが、データの格納・変換・クエリはすべて Fabric 内で完結する。

---

## 0.8 試験範囲マッピング表（第0章関連）

| 試験スキル項目 | 本章の該当セクション |
|--------------|------------------|
| 適切なデータストアを選択する | 0.4.2 |
| データ変換用のツールを選択する | 0.4.2 |
| データフロー Gen 2、パイプライン、ノートブックを選択する | 0.4.2 |

---

## 0.9 確認問題

以下は試験で出題され得る形式の問題。

### Q1. データストアの選択

あなたの組織では、IoT デバイスから毎秒数千件のセンサーデータを受信し、直近24時間のデータに対してサブ秒のレイテンシでアドホッククエリを実行する必要がある。どの Fabric アイテムの組み合わせが最適か？

A) Lakehouse + Notebook (PySpark)
B) Warehouse + Pipeline (Copy アクティビティ)
C) Eventhouse (KQL DB) + Eventstream
D) Lakehouse + Dataflow Gen2

<details>
<summary>解答</summary>

**C) Eventhouse (KQL DB) + Eventstream**

理由:
- 毎秒数千件のストリーミングデータには Eventstream が適切
- サブ秒のレイテンシでのアドホッククエリには KQL DB（Eventhouse）が最適
- Lakehouse + PySpark はバッチ処理向き。Spark 構造化ストリーミングでもサブ秒は困難
- Warehouse はストリーミング取り込みに対応していない
</details>

### Q2. 変換ツールの選択

データアナリストが、複数の Excel ファイルから月次売上データを取り込み、簡単なフィルタリングと列の型変換を行った後、Lakehouse テーブルに保存したい。コーディング経験はない。最適なツールはどれか？

A) Notebook (PySpark)
B) Dataflow Gen2
C) Pipeline (Copy アクティビティ)
D) Warehouse (T-SQL ストアドプロシージャ)

<details>
<summary>解答</summary>

**B) Dataflow Gen2**

理由:
- コーディング不要で GUI 操作が可能（Power Query Online）
- Excel ファイルの読み込みに対応
- フィルタリングや型変換などの基本的な変換が可能
- Lakehouse テーブルをデスティネーションに指定可能
- Copy アクティビティはコピーのみで変換ロジックを含められない
</details>

### Q3. OneLake のアクセス

Notebook から別のワークスペースにある Lakehouse の Delta テーブルを読み込みたい。正しいパスの形式はどれか？

A) `wasbs://<workspace>@onelake.blob.core.windows.net/<lakehouse>/Tables/<table>/`
B) `abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>/Tables/<table>/`
C) `https://onelake.dfs.fabric.microsoft.com/<workspace>/<lakehouse>/Tables/<table>/`
D) `dbfs:/<workspace>/<lakehouse>/Tables/<table>/`

<details>
<summary>解答</summary>

**B) `abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>/Tables/<table>/`**

理由:
- OneLake は ADLS Gen2 互換のため `abfss://` プロトコルを使用する
- ワークスペース名が `@onelake.dfs.fabric.microsoft.com` の前に来る
- `wasbs://` は Azure Blob Storage 用で OneLake では使用しない
- `dbfs:/` は Databricks 固有のプロトコル
</details>

### Q4. 容量とライセンス

F32 の容量に割り当てられたワークスペースで Power BI レポートを閲覧するために必要なライセンスはどれか？

A) Fabric (Free) ライセンスのみ
B) Power BI Pro または PPU ライセンス
C) ライセンスは不要
D) Power BI Premium Per User ライセンスのみ

<details>
<summary>解答</summary>

**B) Power BI Pro または PPU ライセンス**

理由:
- F64 未満の SKU（F32）では、Power BI コンテンツの閲覧に Pro / PPU / 個人トライアルライセンスが必要
- F64 以上であれば Viewer ロールのユーザーは Free ライセンスで Power BI コンテンツを閲覧可能
- F32 で Lakehouse や Notebook 等の非 Power BI アイテムを操作するだけなら Free ライセンスで可能
</details>

---

> **次章**: 第1章 OneLake とショートカット（OneLake のセキュリティモデル、ショートカットの種類と管理、キャッシュ設定）
