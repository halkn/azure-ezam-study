# DP-700 教科書：Microsoft Fabric を使用した Data Engineering ソリューションの実装

> 公式ラーニングパス DP-700T00 準拠 ｜ 試験スキル 2026年1月26日版対応

---

## 凡例

- `[実装管理]` = 分析ソリューションの実装と管理 (30–35%)
- `[取込変換]` = データの取り込みと変換 (30–35%)
- `[監視最適化]` = 分析ソリューションの監視と最適化 (30–35%)
- 📘 = 対応する MS Learn モジュール
- ⚠️ = 公式モジュールのカバーが薄く、ドキュメント補完が必要

---

## 第0章 Fabric アーキテクチャ概論

> 📘 Introduction to end-to-end analytics using Microsoft Fabric

### 0.1 Microsoft Fabric とは

- SaaS 統合分析基盤としての位置づけ
- Azure Synapse / ADF / Power BI との関係と統合の意味
- Fabric のライセンスモデル（容量ベース: F SKU, Trial）

### 0.2 OneLake

- ADLS Gen2 ベースの論理データレイク
- ワークスペースごとの自動プロビジョニング
- Delta Parquet を標準フォーマットとする設計思想
- Snowflake の外部ステージ / 内部ステージとの対比

### 0.3 ワークスペースの概念

- ワークスペース = 論理的なコンテナ（アイテムの集合）
- ドメインによるワークスペースのグルーピング
- 容量の割り当てと課金単位

### 0.4 Fabric の主要アイテムと選択基準

- Lakehouse：非構造化 + 構造化データ、Spark + SQL 分析エンドポイント
- Warehouse：フル T-SQL、リレーショナルモデリング向き
- Eventhouse / KQL DB：時系列・ストリーミングデータ
- Notebook：PySpark / SQL / R のインタラクティブ実行
- Pipeline：オーケストレーション（ADF 互換）
- Dataflow Gen2：Power Query ベースの GUI 変換
- Data Workflow：Apache Airflow マネージド版
- 選択フローチャート：データ特性（バッチ/ストリーム/構造化度）× ユースケースで判断

### 0.5 データフロー全体図

- 取り込み → 変換 → 格納 → 提供 のエンドツーエンドパイプライン
- メダリオンアーキテクチャ（Bronze/Silver/Gold）の全体像
- 各アイテムがどのレイヤーに位置するか

### 0.6 試験範囲マッピング表

- 試験スキル項目 → 教科書の章・節の逆引き一覧

---

## 第1章 OneLake とショートカット `[実装管理][取込変換][監視最適化]`

> 📘 該当する単独モジュールなし（各モジュールに分散）
> ⚠️ 公式ドキュメント補完必須

### 1.1 OneLake のアーキテクチャ

- 単一テナントに1つの OneLake
- 名前空間の階層構造（テナント → ワークスペース → アイテム → Tables/Files）
- Delta Parquet の自動管理
- ADLS Gen2 との互換性と API アクセス

### 1.2 ショートカットの種類と作成

- 内部ショートカット（同一テナント内の別 Lakehouse/Warehouse）
- 外部ショートカット（ADLS Gen2, S3, GCS, Dataverse）
- ショートカット作成手順と権限要件
- ショートカットによるゼロコピーデータ共有

### 1.3 OneLake セキュリティ `[実装管理]`

- フォルダレベルのアクセス制御（OneLake データアクセスロール）
- ファイルレベルの制御
- SQL 分析エンドポイントのセキュリティとの関係
- Snowflake の RBAC/DAC との概念比較

### 1.4 OneLake ワークスペース設定 `[実装管理]`

- OneLake ワークスペース設定の構成項目
- キャッシュの有効化/無効化
  - キャッシュ対象の条件（ファイルサイズ ≤ 1GB）
  - 保持ポリシー（1–28日、アクセスでリセット）
  - 有効にすべき場面 / 無効にすべき場面

### 1.5 ショートカットエラーの特定と解決 `[監視最適化]`

- 一般的なエラーパターン（認証失敗、パス不正、ソース側変更）
- 外部ソース側の接続断 / 権限変更時の挙動
- トラブルシューティング手順

---

## 第2章 Lakehouse と Delta Lake `[取込変換][監視最適化]`

> 📘 Get started with lakehouses in Microsoft Fabric
> 📘 Work with Delta Lake tables in Microsoft Fabric
> 📘 Organize a Fabric lakehouse using medallion architecture design

### 2.1 Lakehouse の作成と構造

- Lakehouse アイテムの作成
- Tables フォルダ（マネージド Delta テーブル）と Files フォルダ（非構造化）
- SQL 分析エンドポイント（自動生成、読み取り専用）
- デフォルトセマンティックモデル

### 2.2 Delta Lake 基礎

- Delta Lake とは（ACID トランザクション on Parquet）
- トランザクションログ（_delta_log）の仕組み
- Parquet との違い（スキーマ強制、タイムトラベル、MERGE サポート）
- Snowflake の Time Travel / CLONE との比較

### 2.3 Delta テーブル管理

#### 2.3.1 V-Order 最適化

- V-Order とは（読み取り最適化されたカラムソート）
- 有効化の方法とデフォルト動作

#### 2.3.2 OPTIMIZE と VACUUM

- OPTIMIZE：小さいファイルの圧縮（compaction）
- ZORDER BY：クエリパターンに合わせたデータ配置
- VACUUM：古いファイルの物理削除、保持期間の設定
- OPTIMIZE と VACUUM の実行タイミングの設計

#### 2.3.3 スキーマ進化

- mergeSchema オプション
- overwriteSchema オプション
- 列追加・型変更時の挙動

#### 2.3.4 タイムトラベル

- バージョン指定 / タイムスタンプ指定での読み取り
- RESTORE コマンド
- VACUUM との関係（VACUUM 後はタイムトラベル不可）

### 2.4 メダリオンアーキテクチャ設計

- Bronze レイヤー：生データの着地（append-only、スキーマ on read）
- Silver レイヤー：クレンジング・型変換・重複排除・結合
- Gold レイヤー：ビジネスレベルの集計・ディメンションモデル
- レイヤー間のデータ移動パターン（ノートブック / パイプライン / Dataflow Gen2）

### 2.5 ディメンションモデルの準備 `[取込変換]`

#### 2.5.1 スタースキーマ設計

- ファクトテーブルとディメンションテーブル
- サロゲートキーの生成（monotonically_increasing_id, ROW_NUMBER）

#### 2.5.2 SCD（Slowly Changing Dimensions）

- Type 0（固定）：変更しない属性
- Type 1（上書き）：最新値のみ保持
- Type 2（履歴保持）：有効期間カラム + サロゲートキー
- Type 3（前回値保持）：限定的な履歴を追加カラムで管理
- Type 4（履歴テーブル分離）
- MERGE 文を使った SCD Type 2 の実装パターン

#### 2.5.3 データの非正規化 `[取込変換]`

- 非正規化の目的と適用場面
- Spark DataFrame での join → select による非正規化実装

### 2.6 データの重複・欠落・遅延の処理 `[取込変換]`

- 重複排除：dropDuplicates, ROW_NUMBER + QUALIFY
- 欠落データ：fillna / COALESCE / デフォルト値戦略
- 到着遅延データ：ウォーターマーク、再処理パターン

### 2.7 Lakehouse テーブルの最適化 `[監視最適化]`

- ファイルサイズの最適化（OPTIMIZE のターゲットサイズ）
- パーティション戦略（日付パーティション、過剰パーティションの回避）
- V-Order の効果と適用基準
- テーブルメンテナンスのスケジューリング

---

## 第3章 Apache Spark とノートブック `[取込変換][監視最適化]`

> 📘 Use Apache Spark in Microsoft Fabric

### 3.1 Fabric における Spark の仕組み

#### 3.1.1 Spark プール

- Starter Pool（共有、即時起動）
- カスタム Spark プール（ノードサイズ・数の構成）
- 自動スケーリングの設定

#### 3.1.2 Spark ワークスペース設定 `[実装管理]`

- ワークスペースレベルの Spark 構成
- ライブラリ管理（環境アイテム: インライン / 要件ファイル / カスタム jar）
- Spark プロパティのオーバーライド
- Runtime バージョンの選択

#### 3.1.3 環境アイテム

- 環境の作成と Notebook へのアタッチ
- パブリックライブラリ / カスタムライブラリの管理
- Spark 構成のカスタマイズ

### 3.2 PySpark DataFrame 操作

#### 3.2.1 読み込みと書き込み

- spark.read.format("delta" / "parquet" / "csv" / "json")
- オプション指定（header, schema, inferSchema, multiLine）
- spark.write / spark.writeStream
- saveAsTable vs save（マネージド vs 外部テーブル）

#### 3.2.2 基本変換

- select, filter / where, withColumn, drop
- cast による型変換
- when / otherwise による条件分岐
- lit, col, expr の使い分け

#### 3.2.3 結合と集計

- join（inner, left, right, full, anti, semi）
- groupBy + agg（count, sum, avg, min, max, collect_list）
- pivot / unpivot
- Snowflake SQL との構文対応

#### 3.2.4 ウィンドウ関数

- Window.partitionBy().orderBy()
- row_number, rank, dense_rank, lag, lead
- 累積集計（rangeBetween, rowsBetween）

#### 3.2.5 UDF（ユーザー定義関数）

- Python UDF の定義と登録
- pandas UDF（Vectorized UDF）のパフォーマンス優位性
- UDF のデメリットと回避パターン

### 3.3 mssparkutils / notebookutils API

- ファイル操作（mssparkutils.fs: ls, cp, mv, rm, mkdirs）
- ノートブック連携（mssparkutils.notebook.run, runMultiple）
- 資格情報管理（mssparkutils.credentials: getToken, getSecret）
- 環境変数・パラメータの受け渡し

### 3.4 ノートブック間の連携

- %run マジックコマンド（同期実行、変数共有）
- mssparkutils.notebook.run（パラメータ付き呼び出し、戻り値取得）
- パイプラインからのノートブック呼び出し（→ 第5章で詳述）
- 参照ノートブック vs 実行ノートブック の使い分け

### 3.5 Spark 構造化ストリーミング `[取込変換]`

> ⚠️ 公式ドキュメント補完推奨

- readStream / writeStream の基本パターン
- Delta テーブルをストリーミングソースとして使用
  - append のみ対応、ignoreChanges / ignoreDeletes オプション
- checkpointLocation の設定と役割
- トリガーの種類（Trigger.processingTime, Trigger.once, Trigger.availableNow）
- Spark Job Definition でのバックグラウンド実行
- Eventstream との使い分け（→ 第7章で比較）

### 3.6 Spark パフォーマンス最適化 `[監視最適化]`

- パーティション数の調整（repartition vs coalesce）
- ブロードキャスト結合（broadcast hint）
- キャッシュ戦略（cache / persist / unpersist）
- シャッフルの最小化（パーティションキーの設計）
- AQE（Adaptive Query Execution）の活用
- Spark UI の読み方（ジョブ / ステージ / タスク / シャッフル量）

### 3.7 ノートブックエラーの特定と解決 `[監視最適化]`

- 一般的なエラーパターン（OOM, シリアライゼーション失敗, スキーマ不一致）
- Spark UI / ドライバーログでの原因特定
- セル実行エラー vs Spark ジョブエラーの区別
- リトライ戦略と冪等性の確保

---

## 第4章 Dataflow Gen2 と Power Query M `[取込変換][監視最適化]`

> 📘 Ingest Data with Dataflows Gen2 in Microsoft Fabric

### 4.1 Dataflow Gen2 のアーキテクチャ

- Dataflow Gen1 との違い（デスティネーション指定、ステージング Lakehouse）
- マッシュアップエンジンの仕組み
- データソース接続（オンプレミスゲートウェイ、クラウドコネクタ）
- Dataflow vs Pipeline vs Notebook の選択基準 `[実装管理]`

### 4.2 Power Query M 言語の基本パターン

#### 4.2.1 クエリ構造

- let ... in 構文
- ステップの連鎖（前のステップ名を参照）
- 型の明示（type table, type text, type number）

#### 4.2.2 よく使う変換関数

- Table.SelectRows / Table.SelectColumns
- Table.AddColumn / Table.RenameColumns / Table.TransformColumns
- Table.Group / Table.Join / Table.NestedJoin
- Table.Pivot / Table.Unpivot
- Text, Number, Date, DateTime 系関数

#### 4.2.3 パラメータとフィルタの動的化

- パラメータの定義と参照
- 増分更新のフィルタ条件への適用

### 4.3 デスティネーション設定

- Lakehouse テーブルへの出力（Replace / Append）
- Warehouse テーブルへの出力
- スキーママッピングとデータ型変換
- ステージングの有無による挙動の違い

### 4.4 増分更新の構成

- 増分更新の条件（日付/日時列によるフィルタ）
- 更新範囲の設定（保持期間、更新期間）
- Dataflow Gen2 における増分更新の仕組みとフォールド

### 4.5 データフローエラーの特定と解決 `[監視最適化]`

- 更新失敗の原因（データソース接続, 型変換エラー, タイムアウト）
- 更新履歴の確認方法
- ステージング Lakehouse のデータ確認
- ゲートウェイ関連のトラブルシューティング

---

## 第5章 パイプラインとオーケストレーション `[実装管理][取込変換][監視最適化]`

> 📘 Orchestrate processes and data movement with Microsoft Fabric

### 5.1 パイプラインの設計

#### 5.1.1 主要アクティビティ

- Copy アクティビティ（ソース/シンク設定、マッピング）
- Dataflow アクティビティ
- Notebook アクティビティ
- Stored Procedure アクティビティ
- KQL アクティビティ

#### 5.1.2 制御フローアクティビティ

- ForEach（並列度の設定）
- If Condition / Switch
- Until / Wait
- Set Variable / Append Variable
- Lookup + ForEach パターン
- Web アクティビティ

#### 5.1.3 パイプラインの依存関係

- 成功 / 失敗 / 完了 / スキップ の依存条件
- 依存関係グラフの設計

### 5.2 パラメータと動的式 `[実装管理]`

- パイプラインパラメータの定義と型
- 式言語（@pipeline().parameters, @activity().output）
- システム変数（@pipeline().RunId, @pipeline().TriggerTime）
- 動的コンテンツとしてのファイルパス・テーブル名の生成
- ADF の式言語との差異

### 5.3 スケジュールとイベントベーストリガー `[実装管理]`

- スケジュールトリガーの構成（cron 式、タイムゾーン）
- イベントベーストリガー（対応イベントソース）
- トリガーの有効化/無効化と管理
- 実行の競合制御

### 5.4 オーケストレーションパターン `[実装管理]`

- パターン1：パイプライン → Copy → Notebook → Dataflow の直列実行
- パターン2：Lookup + ForEach によるメタデータ駆動パイプライン
- パターン3：マスターパイプライン → 子パイプライン呼び出し
- パターン4：ノートブックからのパイプライン呼び出し（REST API）
- Dataflow Gen2, パイプライン, ノートブックの使い分け判断基準

### 5.5 データワークフロー（Apache Airflow in Fabric） `[実装管理]`

> ⚠️ 公式ドキュメント補完推奨

- データワークフローの概要と位置づけ
- Airflow DAG の作成と Fabric アイテムとの連携
- データワークフロー ワークスペース設定の構成
- パイプラインとの使い分け

### 5.6 パイプラインの最適化 `[監視最適化]`

- Copy アクティビティの並列度とDIU設定
- ForEach の並列実行数チューニング
- パイプライン実行の監視と所要時間分析
- 不要なアクティビティの削減

### 5.7 パイプラインエラーの特定と解決 `[監視最適化]`

- 実行ステータスの確認（Monitoring Hub、パイプライン実行ビュー）
- アクティビティごとのエラー出力（errorCode, failureType, message）
- Copy アクティビティの典型的エラー（認証, スキーマ不一致, スロットリング）
- リトライポリシーの設定

---

## 第6章 Data Warehouse `[取込変換][監視最適化]`

> 📘 Get started with data warehouses in Microsoft Fabric
> 📘 Load data into a Microsoft Fabric data warehouse
> 📘 Query a data warehouse in Microsoft Fabric
> 📘 Monitor a Microsoft Fabric data warehouse
> 📘 Secure a Microsoft Fabric data warehouse

### 6.1 Fabric Warehouse の特性

- フル T-SQL サポート（DDL, DML, ストアドプロシージャ、関数）
- Lakehouse SQL 分析エンドポイントとの違い（読み書き vs 読み取り専用）
- 分散処理エンジンの仕組み
- Snowflake Warehouse との概念比較

### 6.2 データロード `[取込変換]`

#### 6.2.1 COPY INTO

- 構文とオプション（ファイル形式, 圧縮, エラー処理）
- OneLake / ADLS Gen2 / S3 からのロード
- Snowflake の COPY INTO との構文差異

#### 6.2.2 パイプラインからのロード

- Copy アクティビティによる Warehouse への書き込み
- Dataflow Gen2 からの Warehouse デスティネーション

#### 6.2.3 増分ロード（MERGE）

- MERGE 文の構文（MATCHED / NOT MATCHED / NOT MATCHED BY SOURCE）
- SCD Type 1/2 の MERGE による実装
- ウォーターマークパターンによる増分検出
- CDC（Change Data Capture）の活用

### 6.3 T-SQL によるデータ変換 `[取込変換]`

#### 6.3.1 CTE とウィンドウ関数

- WITH 句による CTE の連鎖
- ROW_NUMBER, RANK, DENSE_RANK, NTILE
- LAG, LEAD, FIRST_VALUE, LAST_VALUE
- 累積合計・移動平均

#### 6.3.2 ストアドプロシージャと関数

- CREATE PROCEDURE の構文
- パラメータ付きプロシージャ
- テーブル値関数
- パイプラインからの Stored Procedure 呼び出し

#### 6.3.3 データのグループ化と集計

- GROUP BY / HAVING
- GROUPING SETS / ROLLUP / CUBE
- STRING_AGG による集約
- PIVOT / UNPIVOT

### 6.4 ミラーリング `[取込変換]`

> ⚠️ 公式ドキュメント補完推奨

- ミラーリングの概要（ソースDBの変更を OneLake に自動複製）
- 対応ソース（Azure SQL DB, Azure Cosmos DB, Snowflake, etc.）
- ミラーリングの構成手順
- ミラーリングと増分ロードとの使い分け
- OneLake からの継続的インテグレーション `[取込変換]`

### 6.5 Warehouse の最適化 `[監視最適化]`

- 統計情報の手動作成（頻繁にクエリされる列）
- 述語プッシュダウンの活用
- 結果セットサイズの最小化
- Direct Lake モード（Power BI との連携）
- DMV による実行プラン分析

### 6.6 Warehouse のセキュリティ `[実装管理]`

- 行レベルセキュリティ（RLS）の実装（CREATE SECURITY POLICY）
- 列レベルセキュリティ（CLS）の実装（GRANT SELECT ON columns）
- オブジェクトレベルセキュリティ（DENY / GRANT）
- 動的データマスク（DDM）の実装（ALTER TABLE ... ADD MASKED）
- SQL ポリシーと OneLake セキュリティの関係

### 6.7 T-SQL エラーの特定と解決 `[監視最適化]`

- 一般的な T-SQL エラー（型不一致, NULL 処理, ロック競合）
- Fabric Warehouse 固有の制約によるエラー（未サポート構文）
- クエリ実行ログの確認方法
- Snowflake SQL との構文差異に起因するエラー

---

## 第7章 Real-Time Intelligence `[取込変換][監視最適化]`

> 📘 Get started with Real-Time Intelligence in Microsoft Fabric
> 📘 Use real-time eventstreams in Microsoft Fabric
> 📘 Work with real-time data in a Microsoft Fabric eventhouse
> 📘 Create Real-Time Dashboards with Microsoft Fabric

### 7.1 RTI アーキテクチャ

- Real-Time Intelligence の全体像
- Eventhouse / KQL DB / Eventstream / Real-Time Dashboard の関係
- RTI と他の Fabric アイテム（Lakehouse, Warehouse）との連携
- Snowflake Streams/Tasks / Kafka との概念比較

### 7.2 Eventstream の設計 `[取込変換]`

#### 7.2.1 ソースの構成

- Azure Event Hubs
- Azure IoT Hub
- カスタムアプリ / REST API
- サンプルデータ

#### 7.2.2 変換ノード

- フィルター
- 集計（タンブリング/ホッピング/セッション ウィンドウ）
- グループ化
- フィールドの管理（追加/削除/変更）

#### 7.2.3 デスティネーションの構成

- Eventhouse（KQL DB テーブル）
- Lakehouse（Delta テーブル）
- Derived Stream（別の Eventstream への出力）
- カスタムエンドポイント
- Activator

### 7.3 KQL 基礎 `[取込変換]`

#### 7.3.1 基本構文

- パイプ（|）モデル：テーブル | オペレータ | オペレータ ...
- project（列選択）、project-away、project-rename
- where（フィルタ）、has / contains / startswith / matches regex
- extend（列追加・計算列）
- sort by / order by、top、take / limit

#### 7.3.2 集計

- summarize + 集計関数（count, sum, avg, min, max, dcount, percentile）
- summarize by（グループ化）
- bin() による時間バケット
- make-series（時系列データの生成）

#### 7.3.3 結合

- join（inner, leftouter, rightouter, fullouter, anti, semi）
- lookup（ディメンションテーブル参照の軽量版）
- union（複数テーブルの結合）

#### 7.3.4 render（可視化）

- render timechart / barchart / piechart / scatterchart

### 7.4 KQL 関数 `[取込変換]`

#### 7.4.1 文字列関数

- strlen, substring, toupper, tolower, trim
- strcat, replace_string, extract (正規表現)
- split, parse, parse_json

#### 7.4.2 数値関数

- round, floor, ceiling, abs, log, pow
- isnan, isinf

#### 7.4.3 日時・タイムスパン関数

- now(), ago(), datetime_diff, datetime_add
- format_datetime, startofday / startofweek / startofmonth
- todatetime, totimespan
- bin() による日時の丸め

#### 7.4.4 動的型（dynamic）

- JSON の操作：parse_json, tostring, toint, tobool
- 配列操作：array_length, mv-expand, mv-apply
- bag 操作：bag_keys, bag_merge

### 7.5 KQL ウィンドウ関数と時系列 `[取込変換]`

- row_number, rank, dense_rank（serialize が必要）
- prev / next（行間参照）
- make-series + series_fit_line / series_outliers
- sliding_window_counts

### 7.6 ストリーミングエンジンの選択 `[取込変換]`

- Eventstream vs Spark 構造化ストリーミング の比較
  - レイテンシ要件
  - 変換の複雑さ
  - デスティネーションの種類
  - コーディング vs ノーコード
- 判断フローチャート

### 7.7 ストレージとショートカットの選択 `[取込変換]`

- ネイティブストレージ（KQL DB テーブル）
- ミラー化ストレージ（OneLake 連携）
- RTI のショートカット
  - 高速ショートカット（インデックス化、クエリ高速）
  - 非高速ショートカット（ゼロコピー参照、クエリ低速）
- 選択基準（クエリ頻度、データ鮮度、コスト）

### 7.8 リアルタイムダッシュボードとアラート `[監視最適化]`

- Real-Time Dashboard の作成
- KQL クエリによるタイル構成
- パラメータ付きダッシュボード
- Activator によるアラート設定（閾値、条件、アクション）

### 7.9 Eventstream / Eventhouse エラーの特定と解決 `[監視最適化]`

- Eventstream の一般的エラー（ソース接続断、スキーマ変更、バックプレッシャー）
- Eventhouse の取り込みエラー（マッピング不一致、フォーマットエラー）
- KQL DB の診断クエリ（.show ingestion failures）
- ログの確認手順

### 7.10 Eventstream / Eventhouse の最適化 `[監視最適化]`

- バッチング設定のチューニング（バッチサイズ、バッチ間隔）
- スループットの最適化
- 取り込みマッピングの最適化
- データ保持ポリシーの設定
- KQL クエリの最適化（materialize, has > contains, 時間フィルタ先行）

---

## 第8章 セキュリティとガバナンス `[実装管理]`

> 📘 Secure data access in Microsoft Fabric

### 8.1 ワークスペースロール

- Admin / Member / Contributor / Viewer の権限マトリクス
- ロールの割り当て（ユーザー, グループ, サービスプリンシパル）
- 最小権限の原則に基づく設計

### 8.2 アイテムレベルのアクセス制御

- 個別アイテムへの共有と権限付与
- ReadAll / Build / Write / Reshare の権限種別
- 共有リンクの種類とスコープ

### 8.3 行・列・オブジェクト・フォルダー/ファイルレベルのアクセス制御

- Warehouse: RLS, CLS, DDM, オブジェクトレベル（→ 第6章6.6参照）
- Lakehouse: SQL 分析エンドポイントの SQL ポリシー
- Lakehouse: OneLake フォルダ/ファイルレベル（→ 第1章1.3参照）
- Eventhouse: 行レベルセキュリティ（restrict_access）
- 各アイテムのセキュリティ粒度の比較表

### 8.4 秘密度ラベルの適用

- Microsoft Purview Information Protection との統合
- ラベルの種類と適用方法
- ラベルの自動適用と手動適用
- ダウンストリームへのラベル継承

### 8.5 項目の承認

- Promoted（プロモート）：ワークスペースメンバーが設定可能
- Certified（認定）：管理者が指定した認定者のみ
- 承認ステータスの表示と検索への影響

### 8.6 ワークスペースログの実装と活用 `[実装管理][監視最適化]`

- ワークスペースログの有効化
- ログの出力先と形式
- 監査イベントの種類
- ログを使ったセキュリティ監視

---

## 第9章 ライフサイクル管理（CI/CD） `[実装管理]`

> 📘 Implement continuous integration and continuous delivery (CI/CD) in Microsoft Fabric
> 📘 Administer a Microsoft Fabric environment

### 9.1 バージョンコントロールの構成

#### 9.1.1 Git 連携

- Azure DevOps リポジトリとの接続
- GitHub リポジトリとの接続
- ブランチ戦略（ワークスペースごとにブランチを対応付け）
- コミット / 同期 / 競合解決

#### 9.1.2 Git 連携の制約

- サポートされるアイテムの種類
- サポートされない項目と回避策
- Git 連携時の注意点（機密情報, バイナリファイル）

### 9.2 デプロイパイプラインの作成と構成

- デプロイパイプラインの概念（Development → Test → Production）
- ステージの構成とワークスペースの割り当て
- デプロイルールの設定（データソース / パラメータの環境別切り替え）
- 手動デプロイ / API によるデプロイ
- Git 連携との併用パターン

### 9.3 データベースプロジェクト

- データベースプロジェクトの概要
- ソースコントロールとの統合
- スキーマ比較とデプロイ

### 9.4 ワークスペース設定の構成 `[実装管理]`

#### 9.4.1 Spark ワークスペース設定

- デフォルトプール、ランタイムバージョン、ライブラリ

#### 9.4.2 ドメインワークスペース設定

- ドメインの作成と割り当て
- ドメインレベルのポリシー

#### 9.4.3 OneLake ワークスペース設定

- → 第1章 1.4 への参照

#### 9.4.4 データワークフロー ワークスペース設定

- Apache Airflow の構成オプション
- → 第5章 5.5 への参照

### 9.5 管理者タスク

- 容量の割り当てと管理
- テナント設定との関係
- 使用状況メトリクスの確認

---

## 第10章 監視とトラブルシューティング `[監視最適化]`

> 📘 Monitor activities in Microsoft Fabric

### 10.1 Monitoring Hub

- Monitoring Hub の概要と画面構成
- フィルタリング（アイテム種別, ステータス, 時間範囲）
- 実行の詳細確認（所要時間, ステータス, エラーメッセージ）

### 10.2 データ取り込みの監視

- パイプライン実行の監視（Copy アクティビティ, ノートブック, Dataflow）
- Eventstream の監視（入力レート, 出力レート, 遅延）
- 取り込み失敗アラートの設定

### 10.3 データ変換の監視

- ノートブック / Spark ジョブの監視（Spark UI, Monitoring Hub）
- Dataflow Gen2 の更新履歴
- Warehouse のクエリ実行履歴（DMV）

### 10.4 セマンティックモデルの更新の監視

- セマンティックモデルの更新ステータス
- 更新失敗時の通知と原因特定

### 10.5 アラートの構成

- Fabric 内蔵のアラート機能
- Data Activator によるリアルタイムアラート
- メール / Teams 通知の設定

### 10.6 エラー特定の横断チェックリスト

| アイテム | 主な確認先 | よくある原因 |
|---------|-----------|-------------|
| パイプライン | Monitoring Hub → 実行詳細 | 認証, タイムアウト, ソース変更 |
| Dataflow Gen2 | 更新履歴 | 接続, 型変換, ゲートウェイ |
| ノートブック | Spark UI, セルエラー出力 | OOM, スキーマ不一致, ライブラリ依存 |
| Eventhouse | .show ingestion failures | マッピング, フォーマット, スキーマ |
| Eventstream | Monitoring Hub → Eventstream 詳細 | ソース断, バックプレッシャー |
| T-SQL | クエリ実行ログ, DMV | 未サポート構文, 型不一致 |
| ショートカット | OneLake エクスプローラー | 認証, パス変更, ソース削除 |

### 10.7 クエリパフォーマンスの最適化 `[監視最適化]`

- Spark クエリ：→ 第3章 3.6 への参照
- T-SQL クエリ：→ 第6章 6.5 への参照
- KQL クエリ：→ 第7章 7.10 への参照
- 横断的な最適化原則（フィルタ先行、不要列の除去、適切なデータ型）

---

## 付録 A：PySpark クイックリファレンス

### A.1 DataFrame 操作チートシート

- 読み込み / 書き込み / スキーマ定義
- 変換（select, filter, join, groupBy, window）
- Delta 固有操作（MERGE, OPTIMIZE, VACUUM）

### A.2 mssparkutils チートシート

- fs 操作, notebook 連携, credentials

---

## 付録 B：KQL クイックリファレンス

### B.1 演算子チートシート

- テーブル演算子（where, project, extend, summarize, join, union, render）
- スカラー関数（文字列, 数値, 日時, 動的型）

### B.2 KQL と SQL の対応表

- SELECT → project, WHERE → where, GROUP BY → summarize by, JOIN → join

---

## 付録 C：T-SQL クイックリファレンス（Fabric Warehouse）

### C.1 DDL / DML チートシート

- CREATE TABLE, COPY INTO, MERGE, UPDATE, DELETE

### C.2 Fabric Warehouse 固有の制約

- 未サポート構文の一覧
- Snowflake SQL との主要な構文差異

---

## 付録 D：Power Query M クイックリファレンス

### D.1 よく使う関数チートシート

- Table 系 / Text 系 / Number 系 / Date 系 / List 系

---

## 付録 E：試験ドメイン → 章の逆引き表

| 試験スキル項目 | 章 |
|--------------|---|
| Spark ワークスペース設定を構成する | 3.1.2, 9.4.1 |
| ドメイン ワークスペース設定を構成する | 9.4.2 |
| OneLake ワークスペース設定を構成する | 1.4, 9.4.3 |
| データ ワークフロー ワークスペース設定を構成する | 5.5, 9.4.4 |
| バージョン コントロールを構成する | 9.1 |
| データベース プロジェクトを実装する | 9.3 |
| デプロイ パイプラインの作成と構成 | 9.2 |
| ワークスペースレベルのアクセス制御を実装する | 8.1 |
| 項目レベルのアクセス制御を実装する | 8.2 |
| 行/列/オブジェクト/フォルダー/ファイル レベルのアクセス制御を実装する | 8.3, 6.6, 1.3 |
| 動的データ マスクを実装する | 6.6 |
| 項目に秘密度ラベルを適用する | 8.4 |
| 項目を承認する | 8.5 |
| ワークスペース ログを実装して使用する | 8.6 |
| OneLake セキュリティの構成と実装 | 1.3 |
| データフロー Gen 2、パイプライン、ノートブックを選択する | 5.4, 4.1 |
| スケジュールとイベントベースのトリガーを設計および実装する | 5.3 |
| オーケストレーション パターンを実装する | 5.4 |
| 完全および増分データ読み込みの設計と実装 | 2.5, 4.4, 6.2.3 |
| ディメンション モデルに読み込むデータを準備する | 2.5 |
| ストリーミング データの読み込みパターンの設計と実装 | 7.2, 3.5 |
| 適切なデータ ストアを選択する | 0.4 |
| データ変換用ツールを選択する | 4.1, 0.4 |
| データへのショートカットの作成と管理 | 1.2 |
| ミラーリングを実装する | 6.4 |
| パイプラインを使用してデータを取り込む | 5.1 |
| OneLake からの継続的インテグレーション | 6.4 |
| Power Query (M) を使用してデータを変換する | 4.2 |
| PySpark を使用してデータを変換する | 3.2 |
| SQL を使用してデータを変換する | 6.3 |
| KQL を使用してデータを変換する | 7.3, 7.4 |
| データを非正規化する | 2.5.3 |
| データをグループ化して集計する | 3.2.3, 6.3.3, 7.3.2 |
| データの重複、欠落、到着の遅延を処理する | 2.6 |
| 適切なストリーミング エンジンを選択する | 7.6 |
| ストレージとショートカットを選択する | 7.7 |
| 高速/非高速ショートカットを選択する | 7.7 |
| イベントストリームを使用してデータを処理する | 7.2 |
| Spark 構造化ストリーミングを使用してデータを処理する | 3.5 |
| KQL を使用してデータを処理する | 7.3, 7.4 |
| ウィンドウ関数を作成する | 3.2.4, 6.3.1, 7.5, 7.2.2 |
| データ インジェストを監視する | 10.2 |
| データ変換を監視する | 10.3 |
| セマンティック モデルの更新を監視する | 10.4 |
| アラートを構成する | 10.5 |
| パイプライン エラーの特定と解決 | 5.7 |
| データフロー エラーの特定と解決 | 4.5 |
| ノートブック エラーの特定と解決 | 3.7 |
| イベント ハウス エラーの特定と解決 | 7.9 |
| イベントストリーム エラーの特定と解決 | 7.9 |
| T-SQL エラーの特定と解決 | 6.7 |
| ショートカット エラーを特定して解決する | 1.5 |
| レイクハウス テーブルを最適化する | 2.7 |
| パイプラインを最適化する | 5.6 |
| データ ウェアハウスを最適化する | 6.5 |
| イベントストリームとイベントハウスを最適化する | 7.10 |
| Spark のパフォーマンスを最適化する | 3.6 |
| クエリ パフォーマンスを最適化する | 10.7 |
