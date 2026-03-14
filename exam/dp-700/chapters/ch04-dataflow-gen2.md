# 第4章 Dataflow Gen2 と Power Query M

> 📘 対応 MS Learn モジュール: Ingest Data with Dataflows Gen2 in Microsoft Fabric
> 試験タグ: `[取込変換]` `[監視最適化]`

---

## 4.1 Dataflow Gen2 のアーキテクチャ

### 4.1.1 Dataflow Gen2 とは

Dataflow Gen2 は Power Query Online をベースとしたノーコード/ローコードのデータ変換ツールで、300以上のデータソースへの接続・変換・デスティネーションへの出力を GUI で実行できる。

### 4.1.2 Gen1 との違い

| 観点 | Dataflow Gen1 (Power BI) | Dataflow Gen2 (Fabric) |
|------|--------------------------|----------------------|
| データ格納先 | 内部ストレージのみ | **任意のデスティネーション**（Lakehouse, Warehouse, Azure SQL 等） |
| 実行エンジン | マッシュアップエンジンのみ | マッシュアップ + **SQL Warehouse コンピュート**（ステージング経由） |
| 監視 | 限定的 | Monitoring Hub 統合 |
| パイプライン連携 | 制限あり | Dataflow アクティビティとして直接呼び出し可 |
| 増分更新 | ○ | ○（改善版） |
| CI/CD | × | ○（Git / デプロイパイプライン対応） |

### 4.1.3 内部アーキテクチャ

Dataflow Gen2 は公開（Publish）すると、内部的に以下のコンポーネントが使われる。

```
データソース
  │
  ▼
┌────────────────────────────┐
│ マッシュアップエンジン        │ ← Power Query M を解釈・実行
│ (Mashup Engine)            │
└────────────┬───────────────┘
             │
     ┌───────▼───────┐
     │ ステージング     │ ← StagingLakehouse + DataflowsStagingWarehouse
     │ (Lakehouse +   │    （ワークスペースに自動生成）
     │  Warehouse)    │
     └───────┬───────┘
             │  SQL コンピュートで変換を高速化
             ▼
     デスティネーション
     (Lakehouse Table / Warehouse Table / Azure SQL 等)
```

**ステージング**:
- クエリの「Enable staging」プロパティで制御（デフォルト ON）
- 有効にすると、ソースから取得したデータを一旦ステージング Lakehouse に書き込み、そこから SQL コンピュートで変換してデスティネーションに出力
- 大量データの JOIN・集計で高速化が期待できる
- 小量データやシンプルな取り込みではステージングを**無効**にした方が速い場合がある

### 4.1.4 Dataflow Gen2 vs Pipeline vs Notebook の選択

| 判断基準 | Dataflow Gen2 | Pipeline | Notebook |
|---------|--------------|---------|---------|
| コーディング要否 | ノーコード | ローコード（式言語） | コード必須 |
| データ変換の複雑さ | 低〜中 | 低（コピー中心） | 高 |
| データ量 | 小〜中規模 | 大規模（Copy アクティビティ） | 大規模（Spark） |
| リアルタイム処理 | × | × | ○（構造化ストリーミング） |
| オーケストレーション | × | **○（本業）** | △（notebookutils で可能） |
| Excel / 非技術者が使う | **○** | △ | × |
| オンプレミスソース | ○（ゲートウェイ経由） | ○（ゲートウェイ経由） | △ |
| 増分更新 | ○（組み込み） | △（自分でロジック構築） | △ |

**試験ポイント**: 「コーディング不要」「GUI で操作」「300以上のコネクタ」「非技術者向け」→ Dataflow Gen2。「大規模・複雑な変換」→ Notebook。「オーケストレーション」→ Pipeline。

---

## 4.2 Power Query M 言語の基本パターン

### 4.2.1 クエリ構造

M 言語は `let ... in` 構文で各ステップを定義し、前のステップを参照する関数型言語。

```m
let
    // Step 1: ソースへの接続
    Source = Csv.Document(
        Web.Contents("https://example.com/data/sales.csv"),
        [Delimiter = ",", Encoding = 65001, QuoteStyle = QuoteStyle.Csv]
    ),

    // Step 2: ヘッダー昇格
    PromotedHeaders = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),

    // Step 3: 型変換
    ChangedTypes = Table.TransformColumnTypes(PromotedHeaders, {
        {"OrderDate", type date},
        {"Amount", type number},
        {"Quantity", Int64.Type},
        {"CustomerName", type text}
    }),

    // Step 4: フィルタリング
    FilteredRows = Table.SelectRows(ChangedTypes, each [Amount] > 0),

    // Step 5: 列追加
    AddedRevenue = Table.AddColumn(FilteredRows, "Revenue", each [Amount] * [Quantity], type number),

    // Step 6: 不要列の削除
    RemovedColumns = Table.RemoveColumns(AddedRevenue, {"TempColumn", "DebugFlag"})
in
    RemovedColumns
```

**構造のポイント**:
- 各ステップは前のステップ名を参照（`FilteredRows` → `AddedRevenue`）
- `in` の後に最終結果を返す
- ステップの順序変更・挿入・削除は GUI で操作可能
- PySpark の `df = df.filter(...).withColumn(...)` のチェーンに対応する概念

### 4.2.2 よく使う変換関数

#### テーブル操作

```m
// 行のフィルタリング
Table.SelectRows(Source, each [Status] <> "Cancelled" and [Amount] > 100)

// 列の選択
Table.SelectColumns(Source, {"OrderId", "CustomerName", "Amount"})

// 列の削除
Table.RemoveColumns(Source, {"TempCol1", "TempCol2"})

// 列名の変更
Table.RenameColumns(Source, {{"OldName", "NewName"}, {"OldName2", "NewName2"}})

// 列の型変換
Table.TransformColumnTypes(Source, {
    {"OrderDate", type date},
    {"Amount", Currency.Type}
})

// 列の追加（計算列）
Table.AddColumn(Source, "TotalPrice", each [Quantity] * [UnitPrice], type number)

// 条件列（if-then-else）
Table.AddColumn(Source, "OrderSize",
    each if [Amount] >= 10000 then "Large"
         else if [Amount] >= 1000 then "Medium"
         else "Small",
    type text
)

// 列の値変換
Table.TransformColumns(Source, {
    {"CustomerName", Text.Upper, type text},
    {"Email", Text.Trim, type text}
})
```

#### 結合と集計

```m
// テーブル結合（INNER JOIN 相当）
Table.NestedJoin(Orders, {"CustomerId"}, Customers, {"CustomerId"}, "CustomerData", JoinKind.Inner)

// 結合後の展開（JOIN 結果のネストされたテーブルをフラット化）
Table.ExpandTableColumn(JoinedTable, "CustomerData", {"CustomerName", "Region"})

// LEFT JOIN
Table.NestedJoin(Orders, {"CustomerId"}, Customers, {"CustomerId"}, "CustomerData", JoinKind.LeftOuter)

// グループ化と集計
Table.Group(Source, {"Region", "Category"}, {
    {"OrderCount", each Table.RowCount(_), Int64.Type},
    {"TotalRevenue", each List.Sum([Amount]), type number},
    {"AvgOrderValue", each List.Average([Amount]), type number}
})

// テーブルの追加（UNION ALL 相当）
Table.Combine({Table1, Table2, Table3})

// ピボット（行→列）
Table.Pivot(Source, List.Distinct(Source[Category]), "Category", "Amount", List.Sum)

// アンピボット（列→行）
Table.UnpivotOtherColumns(Source, {"Region", "Year"}, "Metric", "Value")
```

#### 文字列・日付・数値関数

```m
// 文字列操作
Text.Upper("hello")          // "HELLO"
Text.Lower("HELLO")          // "hello"
Text.Trim("  hello  ")       // "hello"
Text.Contains("hello", "ell") // true
Text.Replace("2024-01-15", "-", "/")  // "2024/01/15"
Text.Start("hello", 3)       // "hel"
Text.Split("a,b,c", ",")     // {"a", "b", "c"}

// 日付操作
Date.Year(#date(2024, 6, 15))        // 2024
Date.Month(#date(2024, 6, 15))       // 6
Date.AddMonths(#date(2024, 1, 1), 3) // #date(2024, 4, 1)
Date.StartOfMonth(#date(2024, 6, 15)) // #date(2024, 6, 1)
DateTime.LocalNow()                    // 現在日時

// 数値操作
Number.Round(3.456, 2)        // 3.46
Number.RoundDown(3.9)         // 3
Number.Abs(-5)                // 5
```

### 4.2.3 クエリフォールディング

クエリフォールディングは M 言語の変換ステップをソース側（SQL Server, Azure SQL 等）にプッシュダウンする最適化機構。

```m
// フォールド可能な例: SQL に変換される
let
    Source = Sql.Database("server", "database"),
    Orders = Source{[Schema="dbo", Item="Orders"]}[Data],
    Filtered = Table.SelectRows(Orders, each [OrderDate] >= #date(2024, 1, 1)),
    Selected = Table.SelectColumns(Filtered, {"OrderId", "Amount", "OrderDate"})
in
    Selected
// → SELECT OrderId, Amount, OrderDate FROM dbo.Orders WHERE OrderDate >= '2024-01-01'
```

**フォールディングが壊れるステップの例**:
- `Table.AddColumn` で M 固有の関数を使った場合
- `Table.Buffer` の使用
- ソースがフォールドをサポートしない場合（CSV, Excel, Web API 等）

**確認方法**: クエリステップを右クリック → 「View Native Query」。表示可能ならフォールドされている。表示不可なら壊れている。

**試験ポイント**: 増分更新ではフォールディングが**推奨**される（ソース側でフィルタが適用されるため、全データの取得を回避）。フォールドが不可能な場合は Advanced 設定で許可するか、フォールド可能なクエリ設計に修正する。

### 4.2.4 パラメータ

```m
// パラメータの定義（Manage Parameters で GUI から作成も可能）
StartDate = #date(2024, 1, 1) meta [IsParameterQuery = true, Type = "Date"],

// パラメータの使用
let
    Source = Sql.Database("server", "database"),
    Orders = Source{[Schema="dbo", Item="Orders"]}[Data],
    Filtered = Table.SelectRows(Orders, each [OrderDate] >= StartDate)
in
    Filtered
```

パラメータはパイプラインの Dataflow アクティビティからも渡すことが可能。

---

## 4.3 デスティネーション設定

### 4.3.1 対応デスティネーション

| デスティネーション | 状態 | 更新方法 |
|----------------|------|---------|
| **Fabric Lakehouse（テーブル）** | GA | Replace / Append |
| **Fabric Warehouse** | GA | Replace / Append |
| **Azure SQL Database** | GA | Replace / Append |
| Azure Synapse Analytics (SQL DW) | GA | Replace / Append |
| Azure Data Explorer (Kusto) | GA | Replace / Append |
| **Fabric Lakehouse（ファイル: CSV）** | Preview | Replace |
| ADLS Gen2 | Preview | Replace |
| Snowflake | Preview | Replace / Append |
| SharePoint | GA | Replace |

### 4.3.2 設定手順

```
1. クエリを選択
2. 画面下部の「Data destination」で「+」をクリック
3. デスティネーションの種類を選択（例: Lakehouse）
4. 接続先を選択（ワークスペース → Lakehouse → テーブル）
5. 更新方法を選択:
   - Replace: 毎回テーブル全体を置換
   - Append: 既存データに追加
6. スキーママッピング:
   - Automatic settings ON: Dataflow が自動でマッピング管理
   - Automatic settings OFF: 手動でソース列→デスティネーション列を対応付け
```

### 4.3.3 Replace vs Append の選択

| 更新方法 | 動作 | ユースケース |
|---------|------|------------|
| **Replace** | デスティネーションのデータを完全に置換 | ディメンションテーブル（全量洗い替え）、小規模テーブル |
| **Append** | 既存データに追加（重複管理はしない） | ファクトテーブル（日次追加）、ログデータ |

**Append の注意点**: Dataflow Gen2 の Append は単純追加で、重複チェックや MERGE（upsert）は行わない。重複排除が必要な場合は:
1. Append で Lakehouse に着地 → 後段の Notebook で MERGE
2. 増分更新機能を使う

### 4.3.4 ステージングの有無

各クエリの「Enable staging」プロパティで制御する。

| シナリオ | ステージング | 理由 |
|---------|-----------|------|
| 大量データの JOIN / 集計 | **有効** | SQL コンピュートで高速化 |
| 複数ソースの結合 | **有効** | 各ソースをステージングしてから SQL で結合 |
| 単純なコピー（変換なし） | **無効** | ステージングへの書き込みが余分 |
| 小量データ（数千行以下） | **無効** | オーバーヘッドの方が大きい |
| デスティネーション未設定（参照クエリ） | 有効（他クエリの入力として使う場合） | ステージングされたデータを下流クエリが読む |

**制約**: 1 Dataflow あたり最大 **50 クエリ**（ステージング対象 + デスティネーション対象の合計）。

---

## 4.4 増分更新の構成

### 4.4.1 増分更新の概要

大規模テーブルで毎回全量を更新するのは非効率。増分更新は「前回更新以降に変更されたデータのみ」を処理する仕組み。

### 4.4.2 設定手順

```
1. クエリを右クリック → 「Incremental refresh」を選択
2. フィルタリング列の指定:
   - Date / DateTime / DateTimeZone 型の列を選択
   - 例: OrderDate
3. データ抽出期間の指定:
   - 「Extract data from the past」: 過去何日分を確認するか
   - 例: 30 days
4. バケットサイズ:
   - 並列処理の単位（1 day, 7 days, 30 days 等）
   - バケットが大きい → 並列度が下がるがバケットあたりのデータ量が増える
   - バケットが小さい → 並列度が上がるがオーバーヘッドが増える
5. 変更検出列の指定（オプション）:
   - この列の最大値が変わった場合のみバケット内のデータを更新
   - 例: ModifiedDate
6. Advanced 設定:
   - フォールディングを強制するか
   - 並行評価数
```

### 4.4.3 増分更新の動作

```
初回更新:
  全データを取得 → バケットに分割して処理 → デスティネーションに書き込み

2回目以降:
  1. デスティネーションの各バケットの最大値を確認
  2. ソース側で変更があったバケットのみを再取得
  3. 変更バケットのデータをデスティネーションの対応部分に Replace
  4. 変更がないバケットはスキップ
```

### 4.4.4 増分更新の制約

| 制約 | 詳細 |
|------|------|
| 対応デスティネーション | Lakehouse, Warehouse, Azure SQL, Azure Synapse のみ |
| VACUUM | 増分更新テーブルでは VACUUM **非推奨**（干渉のリスク） |
| 他ライターとの共存 | Spark 等の別ツールが同一テーブルに書き込むと干渉する可能性あり |
| テーブルメンテナンス | OPTIMIZE / REORG TABLE は増分更新テーブルでは非サポート |
| デフォルトデスティネーション | 増分更新と併用不可 |
| フォールディング | 推奨（ソース側でフィルタをプッシュダウンして効率化） |

---

## 4.5 Dataflow Gen2 のスケジュールと監視

### 4.5.1 更新のトリガー方法

| 方法 | 説明 |
|------|------|
| オンデマンド | ワークスペースビューから手動で Refresh |
| スケジュール | 日時指定（最大 48 回/日） |
| パイプライン | Dataflow アクティビティから呼び出し |
| Publish 後の自動更新 | Dataflow を Publish すると自動的にオンデマンド更新が開始 |

### 4.5.2 パイプラインからの呼び出し

```
Pipeline:
  ├── Copy Activity (raw → staging)
  ├── Dataflow Activity (staging → silver)  ← Dataflow Gen2 を呼び出し
  └── Notebook Activity (silver → gold)
```

パイプラインの Dataflow アクティビティでは、Dataflow Gen2 のパラメータを動的式で上書き可能。

### 4.5.3 更新履歴の確認

```
ワークスペース → Dataflow の「...」メニュー → Refresh history
  - 各更新のステータス（成功 / 失敗）
  - 所要時間
  - 処理行数
  - エラーメッセージ（失敗時）

Monitoring Hub → Dataflow タブ
  - ワークスペース内の全 Dataflow の実行状況
  - フィルタリング（ステータス、時間範囲、アイテム名）
```

---

## 4.6 データフローエラーの特定と解決 `[監視最適化]`

### 4.6.1 一般的なエラーパターン

| エラー | 原因 | 対処 |
|-------|------|------|
| 接続エラー | データソースの認証失敗 / ネットワーク断 | 接続設定の確認。ゲートウェイの稼働確認 |
| 型変換エラー | ソースデータに想定外の値（例: 数値列に文字列） | `try ... otherwise` で安全に変換、または事前フィルタ |
| NULL エラー | デスティネーション列が非 NULL だがソースに NULL あり | ソース側で NULL を除去 or デフォルト値で補完 |
| タイムアウト | 大量データの処理がタイムアウト | ステージングの有効化、増分更新の導入、クエリの最適化 |
| フォールディング警告 | 増分更新でクエリがフォールドされない | クエリの見直し。フォールドを壊すステップを修正 |
| スキーマ不一致 | ソースのスキーマが変更された | マッピングの再設定。列名/型の確認 |
| 50クエリ制限超過 | ステージング + デスティネーション対象が50超 | Dataflow を分割 |
| ゲートウェイエラー | オンプレミスゲートウェイの問題 | ゲートウェイバージョンの更新、ログの確認 |

### 4.6.2 型変換エラーの安全なハンドリング

```m
// try ... otherwise で変換エラーをデフォルト値に置換
Table.TransformColumns(Source, {
    {"Amount", each try Number.FromText(_) otherwise 0, type number}
})

// NULL の補完
Table.ReplaceValue(Source, null, 0, Replacer.ReplaceValue, {"Quantity"})
Table.ReplaceValue(Source, null, "Unknown", Replacer.ReplaceValue, {"CustomerName"})
```

### 4.6.3 トラブルシューティング手順

```
1. Refresh history でエラーメッセージを確認
   └── ワークスペース → Dataflow → Refresh history → 失敗した更新の詳細

2. エラーの種類を特定
   ├── データソース関連 → 接続設定 / 資格情報 / ゲートウェイを確認
   ├── 変換関連 → Power Query エディタでステップを順に確認
   ├── デスティネーション関連 → スキーマ不一致 / 権限 / 容量を確認
   └── パフォーマンス関連 → ステージングの有効化 / クエリの分割を検討

3. Monitoring Hub で詳細を確認
   └── CU 消費量、実行時間、処理行数を確認

4. ステージング Lakehouse のデータを確認
   └── StagingLakehouse にステージングされたデータが想定通りか確認
```

---

## 4.7 実践パターン集

### 4.7.1 複数 CSV ファイルの統合

```m
// フォルダ内の全 CSV を読み込み・統合
let
    Source = Folder.Files("https://myaccount.dfs.core.windows.net/container/raw/sales/"),
    FilteredCSV = Table.SelectRows(Source, each Text.EndsWith([Name], ".csv")),
    AddContent = Table.AddColumn(FilteredCSV, "Data", each
        Csv.Document([Content], [Delimiter = ",", Encoding = 65001])
    ),
    Expanded = Table.ExpandTableColumn(AddContent, "Data",
        {"Column1", "Column2", "Column3"}
    ),
    PromotedHeaders = Table.PromoteHeaders(Expanded, [PromoteAllScalars = true]),
    RemovedFileColumns = Table.RemoveColumns(PromotedHeaders,
        {"Content", "Name", "Extension", "Date accessed", "Date modified", "Date created",
         "Attributes", "Folder Path"}
    )
in
    RemovedFileColumns
```

### 4.7.2 動的日付フィルタ（当日の前日データを取得）

```m
let
    Yesterday = Date.AddDays(DateTime.Date(DateTime.LocalNow()), -1),
    Source = Sql.Database("server.database.windows.net", "mydb"),
    Orders = Source{[Schema="dbo", Item="Orders"]}[Data],
    Filtered = Table.SelectRows(Orders, each [OrderDate] = Yesterday)
in
    Filtered
```

### 4.7.3 JSON のネスト解除

```m
let
    Source = Json.Document(Web.Contents("https://api.example.com/orders")),
    ConvertedToTable = Table.FromList(Source, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
    Expanded = Table.ExpandRecordColumn(ConvertedToTable, "Column1",
        {"orderId", "customer", "items", "totalAmount"}
    ),
    ExpandedCustomer = Table.ExpandRecordColumn(Expanded, "customer",
        {"name", "email"}, {"CustomerName", "CustomerEmail"}
    ),
    ExpandedItems = Table.ExpandListColumn(ExpandedCustomer, "items"),
    ExpandedItemDetails = Table.ExpandRecordColumn(ExpandedItems, "items",
        {"productName", "quantity", "price"}, {"ProductName", "Quantity", "Price"}
    )
in
    ExpandedItemDetails
```

---

## 4.8 確認問題

### Q1. Dataflow Gen2 のステージング

以下のうち、ステージングを**無効**にした方が効率的なシナリオはどれか？

A) Azure SQL Database から100万行を取得し、別テーブルと JOIN して Lakehouse に出力する
B) ADLS Gen2 の CSV ファイル（1,000行）を型変換のみして Lakehouse に出力する
C) 3つの異なるデータソースからデータを取得し、統合して Warehouse に出力する
D) オンプレミスの SQL Server から500万行を集計して Lakehouse に出力する

<details>
<summary>解答</summary>

**B) ADLS Gen2 の CSV ファイル（1,000行）を型変換のみして Lakehouse に出力する**

1,000行と小量で、JOIN も集計も不要なシンプルな変換。ステージングに書き込む → SQL コンピュートで変換する、という二段階のオーバーヘッドの方が大きい。他の選択肢は大量データや複数ソースの結合でステージングの恩恵を受ける。
</details>

### Q2. クエリフォールディング

Power Query M で Azure SQL Database からデータを取得する以下のクエリで、フォールディングが壊れる可能性が最も高いステップはどれか？

A) `Table.SelectRows(Source, each [OrderDate] >= #date(2024, 1, 1))`
B) `Table.SelectColumns(Source, {"OrderId", "Amount"})`
C) `Table.AddColumn(Source, "Custom", each Text.Upper(Text.Middle([Name], 2, 3)))`
D) `Table.Sort(Source, {{"Amount", Order.Descending}})`

<details>
<summary>解答</summary>

**C) `Table.AddColumn(Source, "Custom", each Text.Upper(Text.Middle([Name], 2, 3)))`**

`Text.Middle`（部分文字列抽出）と `Text.Upper` の組み合わせはM固有の複合操作で、SQL に直接変換できない場合がある。A（WHERE句相当）、B（SELECT句相当）、D（ORDER BY相当）は一般的にフォールド可能。
</details>

### Q3. 増分更新

Dataflow Gen2 の増分更新について正しいものはどれか？

A) 増分更新は全てのデスティネーション型で使用できる
B) 増分更新が有効なテーブルでは OPTIMIZE を安全に実行できる
C) 増分更新には Date / DateTime / DateTimeZone 型のフィルタリング列が必要
D) 増分更新は Spark などの他ツールとの同時書き込みに完全対応している

<details>
<summary>解答</summary>

**C) 増分更新には Date / DateTime / DateTimeZone 型のフィルタリング列が必要**

- A は不正解: 増分更新は Lakehouse, Warehouse, Azure SQL, Azure Synapse のみ
- B は不正解: 増分更新テーブルでは OPTIMIZE / REORG TABLE は非サポート
- D は不正解: 他ツールの書き込みは干渉リスクがあり非推奨
</details>

### Q4. デスティネーションの更新方法

ファクトテーブルに日次で新しいデータを追加したい。既存データは保持しつつ、新規データのみを追加する場合、デスティネーションの更新方法として適切なものはどれか？

A) Replace
B) Append
C) Merge
D) Upsert

<details>
<summary>解答</summary>

**B) Append**

Replace はテーブル全体を毎回置換するため既存データが消える。Dataflow Gen2 のデスティネーション設定では Merge / Upsert は直接サポートされず、Replace か Append のみ。日次の新規データ追加には Append が適切。ただし重複排除が必要な場合は後段で MERGE を実行する設計が必要。
</details>

### Q5. エラー対処

Dataflow Gen2 の更新が「E104100: Couldn't refresh entity - We can't insert null data into a non-nullable column」で失敗した。原因と対処として正しいものはどれか？

A) デスティネーションの容量不足。F SKU を上げる
B) ソースデータに NULL が含まれるが、デスティネーションの列が非 NULL 制約。ソース側で NULL を補完する
C) ステージング Lakehouse が満杯。キャッシュをクリアする
D) データソースの接続タイムアウト。ゲートウェイを再起動する

<details>
<summary>解答</summary>

**B) ソースデータに NULL が含まれるが、デスティネーションの列が非 NULL 制約。ソース側で NULL を補完する**

エラーメッセージが明確に「null data into a non-nullable column」と示している。Power Query で `Table.ReplaceValue` や `Table.TransformColumns` を使って NULL をデフォルト値に変換するか、デスティネーション側の列を NULL 許容に変更する。
</details>

---

> **次章**: 第5章 パイプラインとオーケストレーション（Copy アクティビティ、制御フロー、パラメータと動的式、Data Workflow）
