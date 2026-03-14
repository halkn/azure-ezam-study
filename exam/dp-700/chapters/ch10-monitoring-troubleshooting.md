# 第10章 監視とトラブルシューティング

> 📘 対応 MS Learn モジュール: Monitor activities in Microsoft Fabric
> 試験タグ: `[監視最適化]`

---

## 10.1 Monitoring Hub

### 10.1.1 概要

Monitoring Hub は Fabric のすべてのアクティビティ（パイプライン実行、ノートブック、Dataflow 更新、Spark ジョブ等）を一元的に監視するダッシュボード。

```
アクセス: Fabric ポータル左ナビ → Monitor
```

### 10.1.2 画面構成と機能

| 要素 | 説明 |
|------|------|
| **テーブルビュー** | 直近30日分のアクティビティを最大100件表示（開始時刻降順） |
| **フィルター** | アイテム種別、ステータス、時間範囲、サブミッター |
| **詳細ペイン** | アクティビティを選択すると右側にステータス、開始時刻、所要時間、エラー詳細を表示 |
| **アクション** | アイテム種別に応じてキャンセル、再実行、ログ参照等が可能 |

### 10.1.3 表示されるアイテムと実行タイプ

| アイテム | 表示される実行 |
|---------|-------------|
| Pipeline | パイプライン実行（手動 / スケジュール / イベントトリガー） |
| Notebook | インタラクティブ実行 / パイプラインからの実行 |
| Spark Job Definition | Spark ジョブの実行 |
| Dataflow Gen2 | 更新実行（オンデマンド / スケジュール） |
| Semantic Model | 更新実行 |
| Copy Job | コピージョブの実行 |
| Eventstream | ストリーミング処理の状態 |
| Lakehouse | テーブルメンテナンスジョブ |
| KQL Queryset | クエリ実行 |

**注意**: Dataflow Gen1 は Monitoring Hub に表示**されない**。

---

## 10.2 データ取り込みの監視

### 10.2.1 パイプライン実行の監視

```
Monitoring Hub → Pipeline 実行を選択
  ├── ステータス: Succeeded / Failed / Cancelled / InProgress
  ├── 開始時刻 / 終了時刻 / 所要時間
  ├── アクティビティごとの詳細:
  │     ├── Copy Activity → rowsCopied, rowsRead, throughput, duration
  │     ├── Notebook Activity → exitValue, runPageUrl（Spark UI）
  │     ├── Dataflow Activity → 処理行数, 更新ステータス
  │     └── Stored Procedure → 影響行数, 実行時間
  └── エラー情報:
        ├── errorCode
        ├── failureType (UserError / SystemError)
        └── message（詳細エラーメッセージ）
```

**パイプラインの再実行オプション**:
- **全体の再実行**: パイプライン全体を最初から実行
- **失敗アクティビティからの再実行（Rerun from failed）**: 失敗したアクティビティとその下流のみ再実行（成功済みアクティビティはスキップ）

### 10.2.2 Eventstream の監視

```
Monitoring Hub → Eventstream を選択
  ├── 入力レート（events/sec）
  ├── 出力レート（events/sec）
  ├── レイテンシ
  ├── バックログ（未処理イベント数）
  └── エラー（シリアライゼーション失敗、デスティネーション書き込みエラー等）

Eventstream エディター → デスティネーションノードを選択 → Data preview
  └── リアルタイムのデータプレビュー（データが流れているかの確認）
```

### 10.2.3 Copy Job / Dataflow Gen2 の監視

```
Copy Job:
  Monitoring Hub → Copy Job 実行 → 詳細
    ├── ソースから読み取った行数
    ├── デスティネーションに書き込んだ行数
    ├── スキップされた行数（エラー行）
    └── スループット（MB/s）

Dataflow Gen2:
  ワークスペース → Dataflow → 「...」→ Refresh history
    ├── 各更新のステータス
    ├── 所要時間
    ├── 処理行数
    └── エラーメッセージ（失敗時）
```

---

## 10.3 データ変換の監視

### 10.3.1 Notebook / Spark ジョブの監視

```
Monitoring Hub → Notebook 実行を選択
  ├── ステータス / 所要時間
  ├── Spark Application URL → Spark UI
  └── ログ出力

Spark UI（詳細分析）:
  ├── Jobs タブ: ジョブ一覧、成功/失敗、所要時間
  ├── Stages タブ: 各ステージのタスク数、シャッフル量、データスキュー
  ├── Storage タブ: キャッシュされた RDD/DataFrame
  ├── SQL/DataFrame タブ: 実行プラン（Physical Plan）
  ├── Executors タブ: エグゼキューターのメモリ/CPU 使用状況
  └── Environment タブ: Spark プロパティ、Runtime バージョン
```

**Spark UI のボトルネック特定**:

| 症状 | 確認箇所 | 対処 |
|------|---------|------|
| 特定ステージが極端に遅い | Stages → タスクの所要時間分布 | データスキュー。salting, repartition |
| Shuffle Read/Write が巨大 | Stages → Shuffle Read/Write | パーティションキー見直し、broadcast join |
| GC 時間が長い | Executors → GC Time | メモリ不足。ノードサイズ拡大、キャッシュ削減 |
| タスク数が非常に多い | Jobs → Total Tasks | coalesce でパーティション削減 |
| 失敗タスクがある | Stages → Failed Tasks → エラー詳細 | OOM, シリアライゼーションエラー |

### 10.3.2 Dataflow Gen2 変換の監視

```
Dataflow Gen2 → Refresh history → 特定の更新を展開
  ├── クエリごとの処理時間
  ├── ステージング（有効/無効）
  ├── デスティネーションへの書き込み時間
  └── エラー詳細（変換エラー、型変換エラー等）
```

### 10.3.3 Warehouse クエリの監視

```sql
-- 実行中のクエリ
SELECT
    request_id,
    session_id,
    status,
    submit_time,
    start_time,
    total_elapsed_time,
    command
FROM sys.dm_exec_requests
WHERE status = 'running'
ORDER BY total_elapsed_time DESC;

-- クエリ履歴（直近のクエリ）
SELECT
    request_id,
    status,
    submit_time,
    start_time,
    end_time,
    total_elapsed_time,
    row_count,
    command
FROM sys.dm_exec_requests_history
ORDER BY submit_time DESC;

-- セッション情報
SELECT
    session_id,
    login_name,
    login_time,
    status
FROM sys.dm_exec_sessions
WHERE is_user_process = 1;
```

---

## 10.4 セマンティックモデルの更新の監視

### 10.4.1 監視方法

| 方法 | 確認内容 |
|------|---------|
| **Monitoring Hub** | セマンティックモデルの更新ステータス、所要時間 |
| **Refresh history** | ワークスペース → セマンティックモデル → Refresh ドロップダウン → History |
| **パイプライン出力** | Semantic Model Refresh アクティビティの output（テーブルごとの更新ステータス） |
| **Semantic Link（ノートブック）** | `sempy.fabric` で更新履歴をプログラム的に取得・可視化 |

### 10.4.2 パイプラインからのセマンティックモデル更新

```
Pipeline:
  ├── Notebook Activity (Gold テーブル更新)
  │     └── [On Success]
  ├── Semantic Model Refresh Activity
  │     ├── 対象モデルとテーブルを選択
  │     ├── 全テーブル更新 or 選択テーブルのみ
  │     └── 増分更新（パーティション指定）
  │     └── [On Success]
  ├── Teams Activity (更新成功通知)
  │     └── [On Failure]
  └── Email Activity (更新失敗通知)
```

### 10.4.3 更新失敗時の典型的な原因

| 原因 | 対処 |
|------|------|
| ソースデータへの接続失敗 | 接続文字列・認証情報の確認 |
| タイムアウト | 更新対象データの削減。増分更新の導入 |
| メモリ不足 | モデルの最適化（不要列の削除、カーディナリティ削減） |
| 並行更新の競合 | 同時に同じモデルを更新しないようスケジュール調整 |
| Direct Lake フレーミング失敗 | Delta テーブルの VACUUM がフレーミング中のバージョンを削除した可能性。保持期間を延長 |

---

## 10.5 アラートの構成

### 10.5.1 Fabric のアラート機能

| アラート種類 | 設定場所 | トリガー |
|------------|---------|--------|
| **Data Activator** | Real-Time Hub / Eventhouse / Real-Time Dashboard | データ条件（閾値超過、パターン検出） |
| **Fabric イベントアラート** | Real-Time Hub → Fabric events | ワークスペースアイテムのイベント（ジョブ失敗、更新完了等） |
| **パイプライン内通知** | Pipeline → Email / Teams アクティビティ | パイプラインの成功/失敗 |

### 10.5.2 Data Activator によるアラート

```
構成手順:
1. Real-Time Hub → データソース（Eventstream / Fabric events）を選択
2. 「Set alert」をクリック
3. 条件を定義:
   ├── 測定値: Temperature, ErrorCount, RowCount 等
   ├── 条件: > / < / == / 変化率
   └── 期間: 直近5分間の平均、累積等
4. アクションを定義:
   ├── メール通知
   ├── Teams メッセージ（チャット / チャネル）
   ├── Pipeline の実行
   ├── Notebook の実行
   ├── Spark Job Definition の実行
   ├── Dataflow の実行（プレビュー）
   ├── Function の実行
   └── Power Automate ワークフロー（カスタムアクション）
5. パラメータの受け渡し（Pipeline, Notebook 等にパラメータを渡せる）
```

### 10.5.3 Fabric イベントによるワークスペース監視

```
例: パイプライン失敗時にアラートを送信

Real-Time Hub → Fabric events → Workspace item events
  → イベント種類: Microsoft.Fabric.ItemJobFailed
  → フィルタ: itemType == "Pipeline"
  → アクション: Teams メッセージを送信
```

利用可能な Fabric イベント:
- `ItemJobSucceeded` / `ItemJobFailed`: ジョブの成功/失敗
- `ItemCreated` / `ItemUpdated` / `ItemDeleted`: アイテムの作成/更新/削除
- `OneLakeFileCreated` / `OneLakeFileDeleted`: OneLake ファイルの作成/削除

---

## 10.6 エラー特定の横断チェックリスト

各アイテムのエラー特定は個別の章で詳述した。ここでは試験対策用の横断リファレンスを提供する。

### 10.6.1 アイテム別エラー診断早見表

| アイテム | 主な確認先 | 診断コマンド/ツール | よくある原因 | 詳細参照 |
|---------|-----------|-----------------|------------|---------|
| **Pipeline** | Monitoring Hub → 実行詳細 → Error タブ | `@activity().output.errors` | 認証、タイムアウト、スキーマ不一致 | 5.7 |
| **Dataflow Gen2** | Refresh history | - | 接続、型変換、NULL、ゲートウェイ | 4.6 |
| **Notebook** | Spark UI → Stages → Failed Tasks | `spark.ui` / ドライバーログ | OOM、スキーマ不一致、ライブラリ依存 | 3.7 |
| **Eventhouse** | KQL DB 診断クエリ | `.show ingestion failures` | マッピング、フォーマット、クォータ | 7.9 |
| **Eventstream** | Monitoring Hub → Eventstream 詳細 | データプレビュー | ソース断、スキーマ変更、バックプレッシャー | 7.9 |
| **T-SQL (Warehouse)** | クエリ実行ログ / DMV | `sys.dm_exec_requests_history` | 未サポート構文、型不一致、NULL制約 | 6.7 |
| **Shortcut** | OneLake Explorer → Properties | - | 認証期限切れ、パス変更、ソース削除 | 1.6 |
| **Semantic Model** | Refresh history / パイプライン出力 | Semantic Link (sempy) | 接続、タイムアウト、メモリ、並行競合 | 10.4 |

### 10.6.2 エラーレスポンスパターンの読み方

```
Pipeline エラー出力の典型的な構造:

{
    "errorCode": "2011",
    "message": "The copy activity encountered an error...",
    "failureType": "UserError",        ← ユーザー側の問題
    "target": "CopyActivity_Orders",   ← エラーが発生したアクティビティ
    "details": [
        {
            "source": "AzureSqlDatabase",
            "message": "Login failed for user 'fabric_service'."
        }
    ]
}
```

| failureType | 意味 | 対応 |
|------------|------|------|
| **UserError** | ユーザーの設定/データの問題 | 接続設定、クエリ、データを確認 |
| **SystemError** | Fabric 側の問題 | リトライ。改善しなければサポートに連絡 |

---

## 10.7 クエリパフォーマンスの最適化 `[監視最適化]`

各エンジンの最適化は該当章で詳述した。ここでは横断的な最適化原則を整理する。

### 10.7.1 共通原則

| 原則 | Spark (PySpark) | T-SQL (Warehouse) | KQL (Eventhouse) |
|------|----------------|-------------------|-----------------|
| **フィルタ先行** | `.filter()` を早期に | WHERE 句を先に | `where` を最初のパイプに |
| **不要列の除去** | `.select()` で必要列のみ | SELECT で必要列のみ | `project` で必要列のみ |
| **適切な結合** | `broadcast()` で小テーブル | 統計情報の最新化 | `lookup` で小テーブル |
| **パーティション/インデックス** | パーティション設計、ZORDER | 統計情報、V-Order | 時間パーティション（自動） |
| **不要な計算の回避** | UDF → 組み込み関数 | カーソル → セット操作 | `contains` → `has` |
| **データスキュー対策** | salting, AQE | 統計情報の手動作成 | bin() の粒度調整 |

### 10.7.2 各エンジンの詳細参照

| エンジン | 最適化の詳細 |
|---------|------------|
| Spark | 第3章 3.6（パーティション、broadcast、キャッシュ、AQE、Spark UI） |
| T-SQL (Warehouse) | 第6章 6.5（統計情報、述語プッシュダウン、DMV、Direct Lake） |
| KQL (Eventhouse) | 第7章 7.10（時間フィルタ先行、has vs contains、materialize、join の右側サイズ） |
| Power Query M (Dataflow Gen2) | 第4章 4.2.3（クエリフォールディング、ステージングの有効/無効） |

---

## 10.8 確認問題

### Q1. Monitoring Hub

Monitoring Hub について正しいものはどれか？

A) Dataflow Gen1 の更新も表示される
B) 直近30日分のアクティビティを最大100件表示する
C) Monitoring Hub からパイプラインのスケジュールを変更できる
D) Monitoring Hub はテナント管理者のみがアクセスできる

<details>
<summary>解答</summary>

**B) 直近30日分のアクティビティを最大100件表示する**

- A は不正解: Dataflow Gen1 は Monitoring Hub に表示**されない**
- C は不正解: Monitoring Hub は監視用。スケジュール変更はパイプラインの設定から行う
- D は不正解: 適切な権限を持つユーザーはアクセス可能
</details>

### Q2. Spark UI

Spark ジョブの実行が遅い。Spark UI の Stages タブで特定のステージの一部タスクが極端に長い（他のタスクの10倍以上）ことが判明した。最も可能性の高い原因はどれか？

A) Spark Runtime のバージョンが古い
B) データスキュー（特定パーティションのデータ量偏り）
C) Spark プールのノードサイズが小さい
D) V-Order が有効になっている

<details>
<summary>解答</summary>

**B) データスキュー（特定パーティションのデータ量偏り）**

一部のタスクだけが極端に遅いのはデータスキューの典型的な症状。特定のパーティションキーの値にデータが偏っており、そのパーティションを処理するタスクがボトルネックになっている。対処法: AQE の Skew Join Optimization、salting（キーにランダム値を追加して分散）、repartition。
</details>

### Q3. セマンティックモデルの監視

パイプラインの最終ステップでセマンティックモデルを更新し、成功したら Teams 通知を送信、失敗したらメール通知を送信したい。正しい構成はどれか？

A) Semantic Model Refresh → [On Success] → Teams Activity / [On Failure] → Email Activity
B) Semantic Model Refresh → [On Completion] → If Condition (ステータス確認) → Teams or Email
C) Notebook Activity (セマンティックモデル更新のコードを記述) → Web Activity (通知)
D) Dataflow Gen2 でセマンティックモデルを更新し、Refresh history で手動確認

<details>
<summary>解答</summary>

**A) Semantic Model Refresh → [On Success] → Teams Activity / [On Failure] → Email Activity**

Semantic Model Refresh アクティビティの On Success / On Failure パスで分岐し、それぞれ Teams / Email アクティビティを実行するのが最もシンプル。B も動作するが不必要に複雑。C はセマンティックモデル更新に専用アクティビティがあるため非推奨。
</details>

### Q4. アラート

パイプラインのジョブが失敗した際にリアルタイムで Teams チャネルに通知したい。パイプライン内に通知アクティビティを追加する**以外**の方法として適切なものはどれか？

A) Monitoring Hub のスケジュール更新を設定する
B) Real-Time Hub で Fabric workspace item events（ItemJobFailed）にアラートを設定する
C) Power BI ダッシュボードにアラートを設定する
D) OneLake 診断ログを監視する Notebook をスケジュール実行する

<details>
<summary>解答</summary>

**B) Real-Time Hub で Fabric workspace item events（ItemJobFailed）にアラートを設定する**

Fabric イベント（workspace item events）を使うと、パイプライン定義を変更せずに外部からジョブ失敗を検知してアラートを送信できる。Data Activator がアクションを実行する。A は監視のみで通知機能なし。C は Power BI レポートのデータアラートで、パイプラインのジョブ失敗監視には不適。
</details>

### Q5. エラー診断

Eventhouse の KQL DB でデータ取り込み失敗が疑われる。取り込みエラーを診断するための最初のステップとして適切な KQL コマンドはどれか？

A) `Events | where Timestamp > ago(1h) | count`
B) `.show ingestion failures | where FailedOn > ago(24h)`
C) `.show database schema`
D) `Events | summarize count() by EventType`

<details>
<summary>解答</summary>

**B) `.show ingestion failures | where FailedOn > ago(24h)`**

`.show ingestion failures` は KQL DB の取り込み失敗をリストする管理コマンド。FailedOn、OperationId、Table、FailureKind、Details 等の情報が得られる。A と D はデータクエリであり取り込みエラーの診断には不適。C はスキーマ確認のみ。
</details>

---

> これで本編（第0章〜第10章）が完了。付録（PySpark / KQL / T-SQL / Power Query M のクイックリファレンス、試験ドメイン逆引き表）は別途作成。
