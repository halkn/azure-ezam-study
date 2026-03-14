# DP-700 学習計画：公式ラーニングパスと試験範囲マッピング

## 試験概要

| 項目 | 内容 |
|------|------|
| 認定名 | Microsoft Certified: Fabric Data Engineer Associate |
| 試験時間 | 100分 |
| 合格点 | 700/1000 |
| 言語コード | SQL, PySpark, KQL |
| 特記事項 | **オープンブック**（試験中に Microsoft Learn を参照可能） |

---

## 公式コース構成（DP-700T00）

公式コースは5つのラーニングパスで構成される。以下に試験スキル領域との対応を示す。

---

### Part 1: Ingest data with Microsoft Fabric

**対応する試験範囲**: データの取り込みと変換 (30–35%)

| モジュール | 主要トピック |
|-----------|-------------|
| Ingest Data with Dataflows Gen2 in Microsoft Fabric | Power Query (M言語), データソース接続, Dataflow Gen2 のデスティネーション設定 |
| Orchestrate processes and data movement with Microsoft Fabric | パイプライン設計, Copy アクティビティ, スケジュール/イベントトリガー, パラメータと動的式 |
| Use Apache Spark in Microsoft Fabric | ノートブック, PySpark DataFrame操作, Spark ジョブ定義 |
| Get started with Real-Time Intelligence in Microsoft Fabric | Eventhouse/KQL DB, Eventstream の概念, RTI 全体像 |
| Use real-time eventstreams in Microsoft Fabric | Eventstream ソース/デスティネーション設定, ストリーミング取り込みパターン |
| Work with real-time data in a Microsoft Fabric eventhouse | KQL によるクエリ, ストリーミングデータの処理 |

**試験スキルとの具体的マッピング**:

- 完全/増分データ読み込みの設計と実装 → Dataflows Gen2, パイプライン Copy
- ストリーミングデータの読み込みパターン → Eventstream, Spark 構造化ストリーミング
- Power Query (M), PySpark, SQL, KQL を使用したデータ変換
- パイプラインを使用したデータ取り込み
- データの重複・欠落・到着の遅延を処理

---

### Part 2: Implement a Lakehouse with Microsoft Fabric

**対応する試験範囲**: データの取り込みと変換 (30–35%) + 分析ソリューションの実装と管理の一部

| モジュール | 主要トピック |
|-----------|-------------|
| Introduction to end-to-end analytics using Microsoft Fabric | Fabric アーキテクチャ, OneLake, ワークスペースの概念 |
| Get started with lakehouses in Microsoft Fabric | Lakehouse 作成, SQL分析エンドポイント, Delta テーブル |
| Use Apache Spark in Microsoft Fabric | DataFrame 操作（filter, join, groupBy, window）, PySpark 変換 |
| Work with Delta Lake tables in Microsoft Fabric | Delta Lake形式, V-Order, VACUUM, スキーマ進化, タイムトラベル |
| Ingest Data with Dataflows Gen2 in Microsoft Fabric | ※Part 1 と重複 |
| Orchestrate processes and data movement with Microsoft Fabric | ※Part 1 と重複 |
| Organize a Fabric lakehouse using medallion architecture design | メダリオンアーキテクチャ (Bronze/Silver/Gold), データ非正規化, ショートカット |

**試験スキルとの具体的マッピング**:

- 適切なデータストアの選択（Lakehouse vs Warehouse vs Eventhouse）
- データへのショートカットの作成と管理
- ディメンションモデルに読み込むデータの準備（SCD, サロゲートキー）
- データのグループ化と集計
- OneLake からの継続的インテグレーション

---

### Part 3: Implement Real-Time Intelligence with Microsoft Fabric

**対応する試験範囲**: データの取り込みと変換 > ストリーミングデータの取り込みと変換

| モジュール | 主要トピック |
|-----------|-------------|
| Get started with Real-Time Intelligence in Microsoft Fabric | RTI のアーキテクチャ, Eventhouse, KQL DB |
| Use real-time eventstreams in Microsoft Fabric | Eventstream の設計, ソース/デスティネーション構成 |
| Work with real-time data in a Microsoft Fabric eventhouse | KQL クエリ（summarize, extend, project）, ウィンドウ関数 |
| Create Real-Time Dashboards with Microsoft Fabric | リアルタイムダッシュボード, アラート設定 |

**試験スキルとの具体的マッピング**:

- 適切なストリーミングエンジンの選択
- ネイティブ/ミラー化/RTI ショートカットの選択
- 高速/非高速ショートカットの選択
- KQL を使用したデータ処理（string, number, datetime, timespan関数）
- ウィンドウ関数（ROW_NUMBER, RANK, LAG/LEAD 等）
- イベントストリームを使用したデータ処理

---

### Part 4: Implement a data warehouse with Microsoft Fabric

**対応する試験範囲**: データの取り込みと変換 > バッチデータ + 監視と最適化の一部

| モジュール | 主要トピック |
|-----------|-------------|
| Introduction to end-to-end analytics using Microsoft Fabric | ※Part 2 と重複 |
| Get started with data warehouses in Microsoft Fabric | Warehouse 作成, T-SQL, テーブル設計 |
| Load data into a Microsoft Fabric data warehouse | COPY INTO, パイプラインからの読み込み, 増分ロード（MERGE） |
| Query a data warehouse in Microsoft Fabric | T-SQL クエリ, CTE, ウィンドウ関数, ストアドプロシージャ |
| Monitor a Microsoft Fabric data warehouse | DMV, クエリ実行プラン, 統計情報 |
| Secure a Microsoft Fabric data warehouse | 行レベルセキュリティ (RLS), 列レベルセキュリティ (CLS), 動的データマスク |

**試験スキルとの具体的マッピング**:

- ミラーリングの実装
- T-SQL によるデータ変換
- データウェアハウスの最適化（統計情報, クエリパフォーマンス）
- 行レベル/列レベル/オブジェクトレベルのアクセス制御
- 動的データマスクの実装

---

### Part 5: Manage a Microsoft Fabric environment

**対応する試験範囲**: 分析ソリューションの実装と管理 (30–35%) + 監視と最適化 (30–35%)

| モジュール | 主要トピック |
|-----------|-------------|
| Implement CI/CD in Microsoft Fabric | Git 連携（Azure DevOps/GitHub）, デプロイパイプライン, データベースプロジェクト |
| Monitor activities in Microsoft Fabric | Monitoring Hub, ワークスペースログ, アラート構成 |
| Secure data access in Microsoft Fabric | ワークスペースロール, アイテムレベル権限, 秘密度ラベル, 承認 |
| Administer a Microsoft Fabric environment | Spark/ドメイン/OneLake ワークスペース設定, 容量管理 |

**試験スキルとの具体的マッピング**:

- Spark ワークスペース設定の構成
- ドメイン/OneLake/データワークフロー ワークスペース設定の構成
- バージョンコントロールの構成
- デプロイパイプラインの作成と構成
- ワークスペースレベル/項目レベルのアクセス制御
- OneLake セキュリティの構成と実装
- 項目に秘密度ラベルを適用 / 項目を承認

---

## 試験範囲のうち公式モジュールでカバーが薄い領域

以下は公式ラーニングパスだけでは不十分で、**公式ドキュメントの補完が必要**な領域。

| トピック | 補足先 |
|---------|-------|
| データワークフロー (Apache Airflow in Fabric) | [Fabric Data Workflows ドキュメント](https://learn.microsoft.com/fabric/data-factory/data-workflows-overview) |
| Spark 構造化ストリーミング（Lakehouse への書き込み） | [Structured Streaming in Fabric](https://learn.microsoft.com/fabric/data-engineering/lakehouse-streaming-data) |
| OneLake セキュリティ（フォルダ/ファイルレベル） | [OneLake security](https://learn.microsoft.com/fabric/onelake/onelake-security) |
| ミラーリング（Azure SQL DB, Cosmos DB 等） | [Fabric Mirroring ドキュメント](https://learn.microsoft.com/fabric/database/mirrored-database/overview) |
| Spark パフォーマンス最適化（V-Order, パーティション, キャッシュ） | [Spark optimization](https://learn.microsoft.com/fabric/data-engineering/spark-optimization) |
| エラー特定と解決（パイプライン/ノートブック/Eventhouse 等） | 各項目の Troubleshooting ドキュメント |
| T-SQL エラーの特定と解決 | [Warehouse troubleshooting](https://learn.microsoft.com/fabric/data-warehouse/troubleshoot-synapse-data-warehouse) |
| ショートカットエラーの特定と解決 | [OneLake shortcut ドキュメント](https://learn.microsoft.com/fabric/onelake/onelake-shortcuts) |

---

## あなたの既存スキルからの差分（推定）

Python・Snowflake・Azure を軸にした DE 経験を前提にすると：

| 領域 | 推定ギャップ | 優先度 |
|------|------------|--------|
| **KQL (Kusto Query Language)** | Snowflake SQL とは別体系。summarize, extend, project, make-series 等の構文習得が必要 | **高** |
| **Real-Time Intelligence** | Eventstream, Eventhouse, KQL DB は Fabric 固有。Snowflake の Streams/Tasks とは設計思想が異なる | **高** |
| **Dataflow Gen2 / Power Query M言語** | ADF Data Flow とは別物。GUIベースだが M言語の読解力が問われる | **中** |
| **Fabric Lakehouse / Delta Lake** | Delta Lake 固有機能（V-Order, VACUUM, タイムトラベル）。Snowflake の CLONE/TIME_TRAVEL と対比して理解 | **中** |
| **Fabric ワークスペース管理** | Spark 設定, ドメイン, OneLake セキュリティは Fabric 固有 | **中** |
| **デプロイパイプライン / Git 連携** | ADF の CI/CD とは手順が異なる。Fabric 固有の Git 統合 | **中** |
| **PySpark (Spark DataFrame)** | 既存 Python スキルで対応可能だが、Fabric ノートブック固有の API（mssparkutils 等）を確認 | **低** |
| **T-SQL (Warehouse)** | Snowflake SQL との構文差異（MERGE, COPY INTO 等）を確認すれば対応可能 | **低** |
| **パイプライン設計** | ADF 経験があれば概念は同じ。Fabric パイプライン固有の機能（式言語の差異等）を確認 | **低** |

---

## 推奨学習順序

```
1. Part 5（管理）  ← 全体の土台。ワークスペース・セキュリティの概念を先に押さえる
2. Part 2（Lakehouse）← Fabric の中核。Delta Lake + Spark
3. Part 1（取り込み）← Dataflow Gen2 + パイプライン + Spark
4. Part 4（Warehouse）← T-SQL, Snowflake 経験が活きる。差分だけ集中
5. Part 3（RTI）     ← KQL は新規学習。最も時間をかけるべき
```

---

## 追加リソース

- **Practice Assessment**: [公式模擬試験（無料）](https://learn.microsoft.com/credentials/certifications/azure-administrator/practice/assessment?assessment-type=practice&assessmentId=1704375541&practice-assessment-type=certification)
- **Exam Sandbox**: [試験環境デモ](https://go.microsoft.com/fwlink/?linkid=2226877)
- **Fabric 無料トライアル**: [60日間無料容量](https://learn.microsoft.com/fabric/fundamentals/fabric-trial)
- **試験中の MS Learn ナビゲーション**: PySpark / KQL / T-SQL の構文リファレンスページをブックマークしておく
