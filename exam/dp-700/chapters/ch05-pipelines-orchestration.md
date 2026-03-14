# 第5章 パイプラインとオーケストレーション

> 📘 対応 MS Learn モジュール: Orchestrate processes and data movement with Microsoft Fabric
> 試験タグ: `[実装管理]` `[取込変換]` `[監視最適化]`

---

## 5.1 パイプラインの設計

パイプラインは Fabric Data Factory のオーケストレーションエンジンであり、Azure Data Factory（ADF）と互換性の高い設計。アクティビティを DAG（有向非巡回グラフ）として配置し、データの移動・変換・制御フローを一元管理する。

**制約**: 1パイプラインあたり最大 **120 アクティビティ**。

### 5.1.1 主要アクティビティ

#### データ移動アクティビティ

| アクティビティ | 用途 |
|-------------|------|
| **Copy** | ソース → シンクのデータコピー。ファイル形式変換やマッピングも可能 |
| **Copy Job** | 大規模データ移動に最適化された新しいコピーアイテム（パイプラインから呼び出し） |

**Copy アクティビティの構成要素**:

```
Copy アクティビティ:
  ├── Source（ソース）
  │     ├── データストアの種類（SQL, Blob, REST API 等）
  │     ├── テーブル / クエリ / ファイルパス
  │     └── フィルタ条件（WHERE 句、ファイルワイルドカード等）
  ├── Sink（シンク / デスティネーション）
  │     ├── データストアの種類（Lakehouse, Warehouse, Blob 等）
  │     ├── テーブル / ファイルパス
  │     └── 書き込み動作（Insert / Upsert / ファイル形式）
  ├── Mapping（マッピング）
  │     └── ソース列 → シンク列の対応（自動 or 手動）
  └── Settings
        ├── 並列度（DIU: Data Integration Unit）
        ├── ステージング設定
        └── フォールトトレランス
```

#### データ変換アクティビティ

| アクティビティ | 用途 |
|-------------|------|
| **Notebook** | PySpark ノートブックの実行。パラメータ渡し可能 |
| **Dataflow** | Dataflow Gen2 の実行 |
| **Stored Procedure** | Warehouse / Azure SQL の SP 実行 |
| **KQL** | KQL スクリプトの実行 |
| **Script** | SQL スクリプトの実行 |
| **Function** | Fabric Functions の呼び出し |

#### 制御フローアクティビティ

| アクティビティ | 用途 | 試験重要度 |
|-------------|------|-----------|
| **ForEach** | 配列の要素ごとにループ実行 | **高** |
| **If Condition** | 条件分岐（True / False パス） | **高** |
| **Switch** | 多分岐（値によるケース分岐） | 中 |
| **Until** | 条件を満たすまでループ | 中 |
| **Wait** | 指定秒数だけ一時停止 | 低 |
| **Lookup** | データストアからデータを取得（結果を後続で使用） | **高** |
| **Set Variable** | 変数に値を設定 | 高 |
| **Append Variable** | 配列変数に値を追加 | 中 |
| **Invoke Pipeline** | 別パイプラインを呼び出し | **高** |
| **Fail** | パイプラインを意図的にエラー終了 | 中 |
| **Web** | HTTP リクエストの送信 | 中 |
| **Get Metadata** | データストアのメタデータ取得（ファイル一覧等） | 中 |

### 5.1.2 依存関係の設定

アクティビティ間は以下の4つの条件で接続できる。

| 条件 | 意味 | 記号（UI） |
|------|------|----------|
| **On Success** | 前のアクティビティが成功したら実行 | 緑の矢印 |
| **On Failure** | 前のアクティビティが失敗したら実行 | 赤の矢印 |
| **On Completion** | 成功・失敗に関わらず実行 | 青の矢印 |
| **On Skip** | 前のアクティビティがスキップされたら実行 | 灰色の矢印 |

```
典型的なエラーハンドリングパターン:

  Copy Activity
    ├── [On Success] → Notebook Activity → ...
    └── [On Failure] → Web Activity (エラー通知メール送信)
                         └── [On Completion] → Fail Activity (パイプライン強制失敗)
```

---

## 5.2 パラメータと動的式 `[実装管理]`

### 5.2.1 パイプラインパラメータ

パラメータは外部からパイプラインに値を渡す仕組み。実行中は変更不可（定数）。

```
定義:
  パイプライン → Properties → Parameters タブ → + New
    Name: load_date
    Type: String
    Default: 2024-01-01

参照:
  @pipeline().parameters.load_date
```

| パラメータ型 | 用途 |
|------------|------|
| String | テーブル名、ファイルパス、日付文字列等 |
| Int | カウンター、バッチサイズ等 |
| Float | 閾値等 |
| Bool | フラグ等 |
| Array | ForEach で使用するリスト |
| Object | 構造化データ |

### 5.2.2 パイプライン変数

変数は実行中に値を変更可能。Set Variable / Append Variable で操作。

```
定義:
  パイプライン → Properties → Variables タブ → + New
    Name: processed_count
    Type: Int
    Default: 0
```

**パラメータ vs 変数**:

| | パラメータ | 変数 |
|---|----------|------|
| 外部から渡す | ○ | × |
| 実行中に変更 | × | ○ |
| ForEach 内で変更 | × | ○（Append Variable のみ） |

### 5.2.3 システム変数

```
@pipeline().RunId          → パイプライン実行の一意ID
@pipeline().TriggerTime    → トリガー時刻（UTC, ISO 8601）
@pipeline().TriggerId      → トリガーID
@pipeline().TriggerType    → トリガー種別（Manual, Schedule 等）
@pipeline().DataFactory    → Data Factory 名
@pipeline().GroupId        → 実行グループID
```

### 5.2.4 式言語

ADF 互換の式言語を使用。`@` で始まる式はランタイムで評価される。

```
// 文字列連結
@concat('raw/', formatDateTime(pipeline().TriggerTime, 'yyyy/MM/dd'), '/sales.csv')
→ "raw/2024/01/15/sales.csv"

// 文字列補間
@{pipeline().parameters.schema_name}.@{pipeline().parameters.table_name}
→ "bronze.raw_orders"

// 日付フォーマット
@formatDateTime(pipeline().TriggerTime, 'yyyy-MM-dd')
→ "2024-01-15"

// 日付計算（前日）
@formatDateTime(adddays(pipeline().TriggerTime, -1), 'yyyy-MM-dd')
→ "2024-01-14"

// 条件式
@if(equals(pipeline().parameters.is_full_load, 'true'), 'overwrite', 'append')

// アクティビティ出力の参照
@activity('LookupMaxDate').output.firstRow.max_date

// ForEach 内での現在アイテム
@item()

// 配列要素
@pipeline().parameters.table_list[0]

// JSON パス
@activity('CopyData').output.rowsCopied
@activity('RunNotebook').output.result.exitValue
```

**頻出パターン集**:

```
// 動的ファイルパス
@concat('Files/raw/', pipeline().parameters.source_name, '/',
        formatDateTime(pipeline().TriggerTime, 'yyyy/MM/dd'), '/')

// Notebook にパラメータを渡す
Base parameters:
  load_date = @formatDateTime(pipeline().TriggerTime, 'yyyy-MM-dd')
  source_table = @pipeline().parameters.source_table

// Lookup 結果でループ
ForEach Items: @activity('GetTableList').output.value

// 条件分岐
If Condition Expression: @greater(activity('CopyData').output.rowsCopied, 0)
```

**試験ポイント**: `@` の使い方と式内での文字列リテラルの扱い。`@@` はリテラルの `@` を表す（エスケープ）。

---

## 5.3 スケジュールとイベントベーストリガー `[実装管理]`

### 5.3.1 スケジュールトリガー

```
パイプライン → Schedule タブ
  ├── 頻度: Once / Hourly / Daily / Weekly / Monthly
  ├── 開始日時 / 終了日時
  ├── タイムゾーン
  └── 最大 48 回/日
```

複数のスケジュールを1つのパイプラインに割り当て可能（2025年以降の新機能）。

### 5.3.2 パイプラインからのチェーン実行

- **Invoke Pipeline** アクティビティで子パイプラインを呼び出し
- 同期実行（`waitOnCompletion=true`）/ 非同期実行（`waitOnCompletion=false`）
- パイプラインパラメータを子パイプラインに渡せる

### 5.3.3 実行の競合制御

同一パイプラインの複数インスタンスが同時に実行されるケースの制御:
- デフォルトでは同時実行が許可される
- 増分ロードパイプラインでは同時実行を避ける設計（前の実行完了を待つ）が必要
- Until + Web + Get Metadata の組み合わせで自前のロック機構を構築するパターンもある

---

## 5.4 オーケストレーションパターン `[実装管理]`

### 5.4.1 パターン1: シンプルな直列実行

```
Copy (raw → Bronze) → Notebook (Bronze → Silver) → Notebook (Silver → Gold)
```

最もシンプル。少数テーブルの ETL に適する。

### 5.4.2 パターン2: Lookup + ForEach（メタデータ駆動）

最も実践的で試験でも頻出。設定テーブルからロード対象のリストを取得し、ループ処理する。

```
Lookup (設定テーブルからテーブル一覧を取得)
  └── ForEach (テーブルごとにループ)
        └── [内部]
              ├── Copy (ソース → Bronze Lakehouse)
              └── Notebook (変換処理。テーブル名をパラメータで渡す)
```

```sql
-- Warehouse 上の設定テーブル
CREATE TABLE config.pipeline_config (
    source_schema    NVARCHAR(100),
    source_table     NVARCHAR(100),
    target_schema    NVARCHAR(100),
    target_table     NVARCHAR(100),
    load_type        NVARCHAR(20),    -- 'full' or 'incremental'
    watermark_column NVARCHAR(100),
    is_active        BIT
);

INSERT INTO config.pipeline_config VALUES
('dbo', 'customers', 'bronze', 'raw_customers', 'full', NULL, 1),
('dbo', 'orders', 'bronze', 'raw_orders', 'incremental', 'modified_date', 1),
('dbo', 'products', 'bronze', 'raw_products', 'full', NULL, 1);
```

```
Lookup Activity:
  Query: SELECT * FROM config.pipeline_config WHERE is_active = 1

ForEach Activity:
  Items: @activity('Lookup').output.value
  Sequential: false (並列実行)
  Batch count: 5

  [内部] Notebook Activity:
    Parameters:
      source_table = @concat(item().source_schema, '.', item().source_table)
      target_table = @concat(item().target_schema, '.', item().target_table)
      load_type = @item().load_type
      watermark_column = @item().watermark_column
```

### 5.4.3 パターン3: マスターパイプライン → 子パイプライン

大規模プロジェクトでの責務分離。

```
Master Pipeline:
  ├── Invoke Pipeline: pl_ingest_bronze (パラメータ: load_date)
  │     └── [On Success]
  ├── Invoke Pipeline: pl_transform_silver (パラメータ: load_date)
  │     └── [On Success]
  ├── Invoke Pipeline: pl_build_gold (パラメータ: load_date)
  │     └── [On Success]
  └── Invoke Pipeline: pl_refresh_semantic_model
```

メリット:
- 各子パイプラインを独立してテスト・デプロイ可能
- Git 管理で変更差分が明確
- 異なるチームが並行開発可能

### 5.4.4 パターン4: 条件分岐（全量 / 増分の切り替え）

```
If Condition:
  Expression: @equals(pipeline().parameters.load_type, 'full')
  True:  → Copy Activity (全量コピー、overwrite)
  False: → Copy Activity (増分コピー、ウォーターマーク使用)
           → Stored Procedure (ウォーターマーク更新)
```

### 5.4.5 Dataflow Gen2 / Pipeline / Notebook の使い分け

| 判断基準 | 推奨ツール |
|---------|-----------|
| GUI で変換を定義したい（非技術者含む） | Dataflow Gen2 |
| 大量データの Spark 変換 | Notebook |
| データのコピー（A→B 移動）のみ | Pipeline Copy アクティビティ |
| 複数ツールの逐次実行 | Pipeline（オーケストレーション） |
| 複雑な依存関係・Python ベースのワークフロー | Apache Airflow Job |

---

## 5.5 Apache Airflow Job（Data Workflow） `[実装管理]`

### 5.5.1 概要

Apache Airflow Job（旧 Data Workflow）は Fabric 内で Apache Airflow のマネージド環境を提供するアイテム。Python で DAG を定義し、複雑な依存関係やスケジューリングを管理する。

**Pipeline との使い分け**:

| 観点 | Pipeline | Apache Airflow Job |
|------|---------|-------------------|
| 定義方法 | GUI（キャンバス） | **Python コード（DAG）** |
| 対象ユーザー | ローコード開発者 | Python 開発者 |
| 柔軟性 | 中 | **高**（任意の Python ロジック） |
| Fabric 統合 | **ネイティブ** | FabricRunItemOperator で連携 |
| スケジュール | cron 式 + GUI | cron 式（Airflow ネイティブ） |
| 外部システム連携 | Web アクティビティ | **豊富な Operator/Hook**（REST, SSH, dbt 等） |
| 監視 | Monitoring Hub | Airflow Web UI + Monitoring Hub |
| CI/CD | Git + デプロイパイプライン | **Git + DAG ファイル管理** |

### 5.5.2 ワークスペース設定 `[実装管理]`

```
ワークスペース設定 → Data Factory → Data Workflow Settings
  ├── Default Data Workflow Setting: Starter Pool / Custom Pool
  ├── Custom Pool:
  │     ├── Name: airflow-prod-pool
  │     ├── Compute node size: Large / Small
  │     ├── Enable auto-pause: true / false
  │     └── Auto-pause delay: 分数
  └── Triggerers: 有効化（deferrable operator 用）
```

### 5.5.3 DAG の作成例

```python
# Fabric アイテム（Notebook / Pipeline）を Airflow から実行する DAG
from airflow import DAG
from datetime import datetime
from apache_airflow_microsoft_fabric_plugin.operators.fabric import FabricRunItemOperator

with DAG(
    dag_id="medallion_pipeline",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    ingest_bronze = FabricRunItemOperator(
        task_id="ingest_bronze",
        fabric_conn_id="fabric_conn",
        workspace_id="<workspace_id>",
        item_id="<notebook_bronze_id>",
        job_type="RunNotebook",
        wait_for_termination=True,
        deferrable=True,          # リソースを解放して非同期待機
    )

    transform_silver = FabricRunItemOperator(
        task_id="transform_silver",
        fabric_conn_id="fabric_conn",
        workspace_id="<workspace_id>",
        item_id="<notebook_silver_id>",
        job_type="RunNotebook",
        wait_for_termination=True,
        deferrable=True,
    )

    build_gold = FabricRunItemOperator(
        task_id="build_gold",
        fabric_conn_id="fabric_conn",
        workspace_id="<workspace_id>",
        item_id="<notebook_gold_id>",
        job_type="RunNotebook",
        wait_for_termination=True,
        deferrable=True,
    )

    # 依存関係の定義
    ingest_bronze >> transform_silver >> build_gold
```

**試験ポイント**: `FabricRunItemOperator` の `job_type` は大文字小文字を区別する（`"RunNotebook"`, `"Pipeline"`, `"sparkjob"`）。

---

## 5.6 パイプラインの最適化 `[監視最適化]`

### 5.6.1 Copy アクティビティの最適化

| 設定 | 効果 |
|------|------|
| **DIU（Data Integration Unit）** | 並列度とスループットの調整。デフォルト Auto。大量データでは手動で増加 |
| **並列コピー** | Source / Sink の並列度。パーティション対応ソースで効果大 |
| **ステージング** | Blob ストレージ経由のステージングで異種フォーマット間コピーを高速化 |
| **フォールトトレランス** | エラー行のスキップ、互換性のない行の処理 |

### 5.6.2 ForEach の最適化

```
ForEach:
  Sequential: false        ← 並列実行（デフォルト）
  Batch count: 10          ← 同時実行数（デフォルトは Auto。最大 50）
```

- 並列実行（`isSequential=false`）がデフォルト。順序が重要でなければそのまま使う
- `Batch count` で同時実行数を制限。CU の消費を制御

### 5.6.3 不要なアクティビティの削減

- Lookup + ForEach で1行ずつ処理する代わりに、Notebook で一括処理できないか検討
- 中間の Set Variable を減らし、式で直接計算

### 5.6.4 実行時間の分析

Monitoring Hub → パイプライン実行 → 各アクティビティの所要時間を確認。ボトルネックとなるアクティビティを特定してチューニング。

---

## 5.7 パイプラインエラーの特定と解決 `[監視最適化]`

### 5.7.1 一般的なエラーパターン

| エラー | 原因 | 対処 |
|-------|------|------|
| 認証エラー | 接続の資格情報が期限切れ / 不正 | 接続設定を更新。サービスプリンシパルの証明書有効期限を確認 |
| スキーマ不一致 | ソースとシンクの列数/型が一致しない | マッピングを再設定。列名を確認 |
| タイムアウト | アクティビティの実行がタイムアウト超過 | タイムアウト値を増加。処理対象データを分割 |
| スロットリング | ソース / シンクの API レート制限 | DIU / 並列度を下げる。Wait アクティビティを挿入 |
| Notebook 失敗 | ノートブック内の Spark エラー | パイプライン実行詳細 → Notebook の出力/ログを確認 |
| パイプラインサイクル | アクティビティ間の循環依存 | 依存関係を見直して DAG（非巡回）にする |
| 120 アクティビティ超過 | パイプラインの制限超過 | 子パイプラインに分割 |
| ForEach 内エラー | ループの一部アイテムで失敗 | エラーハンドリング追加。失敗アイテムのスキップまたはリトライ |

### 5.7.2 リトライポリシー

各アクティビティに設定可能。

```
General タブ:
  Retry: 3              ← 最大リトライ回数（0〜10）
  Retry interval: 30    ← リトライ間隔（秒）
```

### 5.7.3 トラブルシューティング手順

```
1. Monitoring Hub → パイプライン実行を選択
2. 実行ステータスを確認（Succeeded / Failed / Cancelled / InProgress）
3. 失敗したアクティビティをクリック → Error タブ
   ├── errorCode: エラーコード
   ├── failureType: UserError / SystemError
   └── message: エラーメッセージ
4. Copy アクティビティの場合:
   ├── Output → rowsCopied, rowsRead, throughput を確認
   └── Details → ソース/シンクの詳細エラー
5. Notebook アクティビティの場合:
   └── Output → result.exitValue, runPageUrl（Spark UI へのリンク）
6. 再実行:
   ├── 失敗したアクティビティから再実行（Rerun from failed）
   └── パイプライン全体の再実行
```

**試験ポイント**: 「失敗したアクティビティから再実行」機能の存在と、`@activity().output` でアクティビティの出力を後続で参照できること。

---

## 5.8 確認問題

### Q1. 式言語

パイプラインのトリガー時刻の前日の日付を `YYYY-MM-DD` 形式で取得する式として正しいものはどれか？

A) `@formatDateTime(subtractFromTime(pipeline().TriggerTime, 1, 'Day'), 'yyyy-MM-dd')`
B) `@formatDateTime(adddays(pipeline().TriggerTime, -1), 'yyyy-MM-dd')`
C) `@adddays(formatDateTime(pipeline().TriggerTime, 'yyyy-MM-dd'), -1)`
D) `@pipeline().TriggerTime - 1`

<details>
<summary>解答</summary>

**B) `@formatDateTime(adddays(pipeline().TriggerTime, -1), 'yyyy-MM-dd')`**

`adddays` で日数を加減算し、`formatDateTime` で書式を指定する。A の `subtractFromTime` も正しい構文だが、`adddays` が一般的。C は `formatDateTime` の結果（文字列）に `adddays` を適用しており型エラー。D は式言語として無効。
</details>

### Q2. メタデータ駆動パターン

設定テーブルからロード対象のテーブルリストを取得し、各テーブルに対して Copy アクティビティを実行するパターンで使用するアクティビティの組み合わせはどれか？

A) Get Metadata + Switch
B) Lookup + ForEach
C) Web + Until
D) Set Variable + If Condition

<details>
<summary>解答</summary>

**B) Lookup + ForEach**

Lookup で設定テーブルからデータを取得し、ForEach でその結果をループ処理する。メタデータ駆動パイプラインの最も一般的なパターン。
</details>

### Q3. パラメータ vs 変数

ForEach ループ内で処理済みのファイル名を蓄積し、ループ完了後にまとめて通知したい。適切な方法はどれか？

A) パイプラインパラメータに追加する
B) Array 型の変数を定義し、ForEach 内で Append Variable を使用する
C) ForEach 内で Set Variable を使用する
D) システム変数 `@pipeline().RunId` に追加する

<details>
<summary>解答</summary>

**B) Array 型の変数を定義し、ForEach 内で Append Variable を使用する**

- A は不正解: パラメータは実行中に変更不可
- B は正解: Array 変数 + Append Variable で蓄積可能
- C は不正解: ForEach 内の Set Variable は並列実行時に競合する（最後の値で上書き）。Append Variable が正解
- D は不正解: RunId はシステム変数で変更不可
</details>

### Q4. Pipeline vs Apache Airflow Job

以下のうち Apache Airflow Job が Pipeline よりも適しているシナリオはどれか？

A) GUI で簡単なデータコピーパイプラインを構築したい
B) Python で定義した複雑な依存関係と条件分岐を持つワークフローを実行したい
C) Dataflow Gen2 と Notebook を順次実行するシンプルなオーケストレーションを構築したい
D) Copy アクティビティで大量データを移動したい

<details>
<summary>解答</summary>

**B) Python で定義した複雑な依存関係と条件分岐を持つワークフローを実行したい**

Apache Airflow Job は Python で DAG を定義でき、任意の Python ロジック・豊富な Operator（dbt, REST, SSH 等）を使える。Pipeline は GUI ベースでシンプルなオーケストレーション向き。A, C, D は Pipeline の方が適している。
</details>

### Q5. エラーハンドリング

パイプラインで Copy アクティビティが失敗した場合にメール通知を送信し、その後パイプラインをエラー終了させたい。正しい構成はどれか？

A) Copy → [On Failure] → Web Activity (メール送信) → [On Success] → Fail Activity
B) Copy → [On Success] → Web Activity (メール送信) → Fail Activity
C) Copy → [On Completion] → If Condition (status check) → Fail Activity
D) Copy → [On Failure] → Fail Activity → [On Completion] → Web Activity (メール送信)

<details>
<summary>解答</summary>

**A) Copy → [On Failure] → Web Activity (メール送信) → [On Success] → Fail Activity**

Copy が失敗した場合に On Failure パスで Web Activity（メール送信）を実行し、その後 Fail Activity でパイプラインをエラー終了させる。D は Fail Activity の後に処理を続けられない（Fail で即座に終了）。
</details>

---

> **次章**: 第6章 Data Warehouse（T-SQL によるデータ変換、MERGE、ミラーリング、Warehouse のセキュリティと最適化）
