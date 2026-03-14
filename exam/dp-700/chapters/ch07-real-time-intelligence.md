# 第7章 Real-Time Intelligence

> 📘 対応 MS Learn モジュール:
> - Get started with Real-Time Intelligence in Microsoft Fabric
> - Use real-time eventstreams in Microsoft Fabric
> - Work with real-time data in a Microsoft Fabric eventhouse
> - Create Real-Time Dashboards with Microsoft Fabric
>
> 試験タグ: `[取込変換]` `[監視最適化]`
>
> ⚠️ Snowflake / ADF 経験者の最大ギャップ領域。KQL は SQL と構文が大きく異なるため重点的に学習。

---

## 7.1 RTI アーキテクチャ

### 7.1.1 構成要素の関係

```
データソース                  Fabric Real-Time Intelligence                   消費
──────────                 ──────────────────────────                      ────
Event Hubs   ─┐
IoT Hub      ─┤            ┌──────────────┐
Custom App   ─┼── ソース ──→│ Eventstream  │──→ 変換ノード（フィルタ/集計/結合）
Kafka        ─┤            └──────┬───────┘
Azure SQL CDC─┘                   │
                    ┌─────────────┼─────────────┐
                    ▼             ▼             ▼
             ┌───────────┐ ┌──────────┐  ┌───────────┐
             │ Eventhouse │ │Lakehouse │  │ Activator │
             │ (KQL DB)   │ │(Delta)   │  │ (アラート) │
             └─────┬─────┘ └──────────┘  └───────────┘
                   │
            ┌──────┴──────┐
            │ KQL Queryset │ ──→ Real-Time Dashboard
            └─────────────┘      Power BI
```

| コンポーネント | 役割 |
|-------------|------|
| **Eventstream** | データソースからイベントを取り込み、変換し、デスティネーションにルーティング |
| **Eventhouse** | KQL DB のコンテナ。リアルタイムデータの格納・管理 |
| **KQL Database** | 時系列・イベントデータを格納するデータベース。KQL でクエリ |
| **KQL Queryset** | KQL クエリの保存・実行環境 |
| **Real-Time Dashboard** | KQL クエリベースのリアルタイム可視化 |
| **Activator** | 条件ベースのアラートとアクション自動実行 |
| **Real-Time Hub** | テナント全体のストリーミングデータの発見・管理の集約ポイント |

### 7.1.2 Eventhouse と KQL DB

- **Eventhouse** = KQL DB の**コンテナ**（データ自体は持たない）
- 1つの Eventhouse に**複数の KQL DB** を作成可能
- Eventhouse はコスト最適化のため非使用時に**自動サスペンド**する（再起動時に数秒のレイテンシ）
- **Always-On** を有効にすると常時起動（高時間感度のシステム向け）
- **Minimum consumption** 設定でオートスケールの下限を指定可能

### 7.1.3 OneLake 可用性

KQL DB のデータを OneLake に公開する機能。有効化するとデータが Delta Lake 形式で OneLake に格納され、他の Fabric エンジン（Spark, SQL, Power BI Direct Lake）からアクセス可能になる。

```
KQL DB (OneLake 可用性 ON)
  └── OneLake に Delta テーブルとして公開
        └── Lakehouse からショートカットで参照
              └── Spark / SQL 分析エンドポイント / Power BI からクエリ
```

---

## 7.2 Eventstream の設計 `[取込変換]`

### 7.2.1 ソースの構成

| ソース | 説明 |
|-------|------|
| Azure Event Hubs | 高スループットのイベント取り込み |
| Azure IoT Hub | IoT デバイスからのテレメトリ |
| Custom App / Endpoint | REST API / SDK 経由でのプッシュ |
| Azure SQL DB CDC | Change Data Capture によるリアルタイム変更ストリーム |
| Azure Cosmos DB CDC | Cosmos DB の Change Feed |
| PostgreSQL CDC | 論理レプリケーション |
| MySQL CDC | binlog ベース |
| Kafka | Kafka コンシューマー |
| サンプルデータ | テスト用の生成データ |
| Fabric イベント | ワークスペースアイテムイベント、OneLake イベント |

### 7.2.2 変換ノード（Event Processor）

Enhanced capabilities が有効な場合、Eventstream 内でストリーム処理を実行可能。

| 変換 | 動作 | SQL 相当 |
|------|------|---------|
| **Filter** | 条件に基づいて行をフィルタ | WHERE |
| **Manage fields** | 列の選択・削除・名前変更 | SELECT / AS |
| **Aggregate** | ウィンドウ集計（後述） | GROUP BY + ウィンドウ関数 |
| **Group by** | グループ化 | GROUP BY |
| **Union** | 複数ストリームの統合 | UNION ALL |
| **Expand** | 配列/JSON のフラット化 | LATERAL FLATTEN (Snowflake) |
| **Join** | 2つのストリームの結合 | JOIN |

### 7.2.3 ウィンドウ集計

Eventstream の Aggregate 変換で使用するウィンドウの種類:

```
タンブリング ウィンドウ（固定長、重複なし）:
  |---5min---|---5min---|---5min---|
  00:00      00:05      00:10      00:15

ホッピング ウィンドウ（固定長、一定間隔でスライド、重複あり）:
  |------10min------|
       |------10min------|
            |------10min------|
  スライド: 5min

セッション ウィンドウ（アクティビティベース、タイムアウトで閉じる）:
  |--event--event--event--|  (タイムアウト: 5min, 5分間イベントなしで閉じる)
                              |--event--event--|
```

### 7.2.4 デスティネーション

| デスティネーション | 用途 |
|---------------|------|
| **Eventhouse (KQL DB)** | リアルタイム分析クエリ。最も一般的 |
| **Lakehouse (Delta)** | バッチ分析・メダリオンアーキテクチャの Bronze |
| **Derived Stream** | 別の Eventstream や処理への中間出力 |
| **Activator** | 条件ベースのアラート |
| **Custom Endpoint** | 外部システムへのプッシュ |
| **Fabric Reflex** | ルールベースの自動アクション |

**試験ポイント**: 1つの Eventstream に**複数のデスティネーション**を設定可能。同じストリームデータを Eventhouse と Lakehouse の両方に同時出力できる。

---

## 7.3 KQL 基礎 `[取込変換]`

KQL（Kusto Query Language）は SQL と設計思想が異なる。SQL が宣言的（何が欲しいかを記述）なのに対し、KQL は**パイプライン的**（データを変換ステップで順に処理）。

### 7.3.1 基本構文

```kql
// KQL の基本形: テーブル | 演算子 | 演算子 | ...
SensorReadings
| where Timestamp > ago(1h)
| where DeviceType == "TemperatureSensor"
| project Timestamp, DeviceId, Location, Temperature
| order by Timestamp desc
| take 100
```

**SQL との対応**:

```sql
-- 上記 KQL と同等の SQL
SELECT TOP 100 Timestamp, DeviceId, Location, Temperature
FROM SensorReadings
WHERE Timestamp > DATEADD(HOUR, -1, GETUTCDATE())
  AND DeviceType = 'TemperatureSensor'
ORDER BY Timestamp DESC;
```

### 7.3.2 テーブル演算子リファレンス

| KQL 演算子 | SQL 相当 | 説明 |
|-----------|---------|------|
| `where` | WHERE | 行のフィルタ |
| `project` | SELECT (列選択) | 列の選択・リネーム |
| `project-away` | (除外 SELECT) | 指定列を除外 |
| `project-rename` | AS | 列名変更のみ |
| `extend` | SELECT *, 計算列 | 列の追加（既存列は保持） |
| `summarize` | GROUP BY + 集計関数 | グループ化と集計 |
| `order by` / `sort by` | ORDER BY | ソート |
| `top` | TOP / LIMIT + ORDER BY | 上位N件 |
| `take` / `limit` | TOP / LIMIT | N件取得（順序不定） |
| `join` | JOIN | テーブル結合 |
| `union` | UNION ALL | テーブル結合（行追加） |
| `distinct` | DISTINCT | 重複排除 |
| `count` | COUNT(*) | 行数カウント |
| `render` | (可視化) | チャート描画 |
| `let` | (変数宣言) | クエリ内変数 |
| `lookup` | LEFT JOIN (軽量) | ディメンションテーブル参照 |
| `mv-expand` | LATERAL FLATTEN | 配列の展開 |
| `parse` | REGEXP 抽出 | 文字列からの構造化抽出 |
| `serialize` | (順序保証) | ウィンドウ関数に必要 |

### 7.3.3 where（フィルタ）

```kql
// 基本フィルタ
Events | where EventType == "Error"
Events | where Temperature > 30.0
Events | where Timestamp > ago(24h)
Events | where Timestamp between (datetime(2024-01-01) .. datetime(2024-01-31))

// 文字列フィルタ
Events | where Message has "timeout"          // 単語単位の部分一致（高速、推奨）
Events | where Message contains "time"        // 部分文字列一致（has より遅い）
Events | where Message startswith "Error:"
Events | where Message endswith ".exe"
Events | where Message matches regex @"\d{3}-\d{4}"

// NULL チェック
Events | where isnotnull(ErrorCode)
Events | where isempty(Message)               // 空文字列または NULL
Events | where isnotempty(Message)

// 複合条件
Events | where EventType == "Error" and Severity >= 3
Events | where Region in ("East", "West")
Events | where Region !in ("Unknown", "Test")
```

**試験ポイント**: `has` vs `contains` の違い。`has` は単語境界での一致（トークンベース、インデックス使用、**高速**）。`contains` は任意の部分文字列一致（**低速**）。試験では「パフォーマンスを改善するために何を変えるか」→ `contains` を `has` に変更、が頻出。

### 7.3.4 project / extend

```kql
// project: 指定列のみ返す
Events
| project Timestamp, DeviceId, Temperature

// project-away: 指定列を除外
Events
| project-away InternalId, DebugFlag

// project-rename: 列名変更のみ
Events
| project-rename EventTime = Timestamp, Device = DeviceId

// extend: 列を追加（既存列はすべて保持）
Events
| extend TempFahrenheit = Temperature * 9.0 / 5.0 + 32.0
| extend DayOfWeek = dayofweek(Timestamp)
| extend IsHigh = Temperature > 30.0
```

### 7.3.5 summarize（集計）

```kql
// 基本集計
Events
| summarize
    EventCount = count(),
    AvgTemp = avg(Temperature),
    MaxTemp = max(Temperature),
    MinTemp = min(Temperature),
    UniqueDevices = dcount(DeviceId)

// グループ化
Events
| summarize EventCount = count(), AvgTemp = avg(Temperature)
    by Region, DeviceType

// bin() による時間バケット集計（最頻出）
Events
| where Timestamp > ago(24h)
| summarize AvgTemp = avg(Temperature), EventCount = count()
    by bin(Timestamp, 1h), Region
| order by Timestamp asc

// 時間バケットの種類
// bin(Timestamp, 5m)    → 5分間隔
// bin(Timestamp, 1h)    → 1時間間隔
// bin(Timestamp, 1d)    → 1日間隔

// percentile（パーセンタイル）
Events
| summarize p50 = percentile(ResponseTime, 50),
            p95 = percentile(ResponseTime, 95),
            p99 = percentile(ResponseTime, 99)
    by ServiceName

// make_list / make_set（配列への集約）
Events
| summarize Devices = make_set(DeviceId), Readings = make_list(Temperature)
    by Region

// arg_max / arg_min（最大/最小値を持つ行の他の列を取得）
Events
| summarize arg_max(Timestamp, *) by DeviceId    // 各デバイスの最新レコード
```

### 7.3.6 join

```kql
// inner join（デフォルト）
Events
| join kind=inner (
    Devices | project DeviceId, Location, DeviceType
) on DeviceId

// left outer join
Events
| join kind=leftouter (Devices) on DeviceId

// lookup（ディメンション参照用の軽量 join。right 側が小さい場合に最適）
Events
| lookup (Devices) on DeviceId

// anti join（右側に存在しない行）
Events
| join kind=leftanti (KnownDevices) on DeviceId
// → 未登録デバイスからのイベントを検出

// 時間ベースの join
Events
| join kind=inner (
    Alerts
    | where AlertType == "Threshold"
) on DeviceId, $left.Timestamp == $right.AlertTime
```

| join kind | 動作 | SQL 相当 |
|----------|------|---------|
| `inner` | 両方に存在 | INNER JOIN |
| `leftouter` | 左の全行 | LEFT JOIN |
| `rightouter` | 右の全行 | RIGHT JOIN |
| `fullouter` | 両方の全行 | FULL OUTER JOIN |
| `leftanti` | 右に**ない**左の行 | NOT EXISTS |
| `leftsemi` | 右に**ある**左の行 | EXISTS |
| `innerunique` | デフォルト。右側の重複を排除して inner join | - |

### 7.3.7 let（変数宣言）

```kql
// スカラー変数
let threshold = 30.0;
let timeRange = 24h;
let startDate = datetime(2024-01-01);

Events
| where Temperature > threshold
| where Timestamp > ago(timeRange)

// テーブル式変数
let RecentErrors = Events
    | where Timestamp > ago(1h)
    | where EventType == "Error";

RecentErrors
| summarize count() by DeviceId
| top 10 by count_

// 関数定義
let CelsiusToFahrenheit = (c: real) { c * 9.0 / 5.0 + 32.0 };

Events
| extend TempF = CelsiusToFahrenheit(Temperature)
```

### 7.3.8 render（可視化）

```kql
Events
| where Timestamp > ago(24h)
| summarize AvgTemp = avg(Temperature) by bin(Timestamp, 1h)
| render timechart             // 時系列グラフ

// その他のチャート種類
// | render barchart            // 棒グラフ
// | render columnchart         // 縦棒グラフ
// | render piechart            // 円グラフ
// | render scatterchart        // 散布図
// | render areachart           // 面グラフ
// | render ladderchart         // はしごグラフ
```

---

## 7.4 KQL 関数 `[取込変換]`

### 7.4.1 文字列関数

```kql
// 長さ・変換
strlen("hello")                    // 5
toupper("hello")                   // "HELLO"
tolower("HELLO")                   // "hello"
trim(" hello ")                    // "hello" (前後の空白除去)
trim_start(" hello")               // "hello" (先頭のみ)
trim_end("hello ")                 // "hello" (末尾のみ)

// 連結・置換
strcat("hello", " ", "world")      // "hello world"
replace_string("2024-01-15", "-", "/")  // "2024/01/15"

// 部分文字列
substring("hello world", 6, 5)     // "world" (開始位置, 長さ)

// 分割
split("a,b,c", ",")               // ["a", "b", "c"]
split("a,b,c", ",")[1]            // "b" (0-indexed)

// 正規表現抽出
extract(@"(\d+)-(\d+)", 1, "order-12345-abc")  // "12345"

// parse（構造化抽出）
// ログ行: "2024-01-15 Error: Connection timeout from 192.168.1.1"
Events
| parse Message with EventDate " " Level ": " ErrorMsg " from " IPAddress
| project EventDate, Level, ErrorMsg, IPAddress

// parse_json
Events
| extend Payload = parse_json(RawData)
| extend UserId = tostring(Payload.user.id)
| extend Action = tostring(Payload.action)
```

### 7.4.2 数値関数

```kql
round(3.456, 2)       // 3.46
floor(3.9, 1)         // 3 (floor は第2引数で丸め単位を指定)
ceiling(3.1)          // 4
abs(-5)               // 5
log(100)              // 4.605... (自然対数)
log10(100)            // 2
pow(2, 10)            // 1024
sqrt(144)             // 12
isnan(0.0/0.0)        // true
isinf(1.0/0.0)        // true
```

### 7.4.3 日時・タイムスパン関数

```kql
// 現在時刻・相対時刻
now()                                      // 現在の UTC 時刻
ago(1h)                                    // 1時間前
ago(7d)                                    // 7日前

// 日時リテラル
datetime(2024-06-15T10:30:00Z)
datetime(2024-06-15)

// タイムスパンリテラル
1h          // 1時間
30m         // 30分
7d          // 7日
1s          // 1秒
100ms       // 100ミリ秒

// 差分・加算
datetime_diff("day", datetime(2024-06-15), datetime(2024-06-01))   // 14
datetime_add("month", 3, datetime(2024-01-01))                     // 2024-04-01

// フォーマット
format_datetime(now(), "yyyy-MM-dd HH:mm:ss")   // "2024-06-15 10:30:00"

// 日時の切り捨て
startofday(now())              // 今日の 00:00:00
startofweek(now())             // 今週月曜の 00:00:00
startofmonth(now())            // 今月1日の 00:00:00
startofyear(now())             // 今年1月1日の 00:00:00
endofday(now())                // 今日の 23:59:59.9999999

// 型変換
todatetime("2024-06-15")       // datetime 型に変換
totimespan("01:30:00")         // timespan 型に変換
toint("42")                    // int 型に変換
toreal("3.14")                 // real 型に変換
tostring(42)                   // "42"
tobool("true")                 // true

// bin() による時間の丸め
bin(datetime(2024-06-15T10:37:00Z), 5m)    // 2024-06-15T10:35:00Z
bin(datetime(2024-06-15T10:37:00Z), 1h)    // 2024-06-15T10:00:00Z
```

### 7.4.4 動的型（dynamic）

KQL の `dynamic` 型は JSON オブジェクト・配列を扱う。Snowflake の `VARIANT` に相当。

```kql
// JSON の操作
Events
| extend Payload = parse_json(RawData)
| extend UserId = tostring(Payload.user.id)
| extend Tags = Payload.tags               // 配列を取得

// 配列操作
Events
| extend Tags = parse_json(RawData).tags
| mv-expand Tag = Tags                    // 配列の各要素を行に展開
| summarize count() by tostring(Tag)

// mv-apply（配列内で計算してフィルタ）
Events
| extend Items = parse_json(RawData).items
| mv-apply Item = Items on (
    where toint(Item.quantity) > 5
    | project ItemName = tostring(Item.name), Qty = toint(Item.quantity)
)

// bag（辞書/オブジェクト）操作
Events
| extend Props = parse_json(Properties)
| extend Keys = bag_keys(Props)           // キーの一覧
| extend Merged = bag_merge(Props, dynamic({"source": "fabric"}))

// array_length
Events
| extend TagCount = array_length(parse_json(RawData).tags)

// dynamic リテラル
print d = dynamic({"name": "sensor1", "values": [1, 2, 3]})
| extend name = tostring(d.name)
| extend first_value = toint(d.values[0])
```

---

## 7.5 KQL ウィンドウ関数と時系列 `[取込変換]`

### 7.5.1 ウィンドウ関数

KQL でウィンドウ関数を使うには、事前に `serialize` で行の順序を確定させる必要がある。

```kql
// row_number
Events
| serialize
| extend RowNum = row_number()

// rank (同値タイ対応)
Events
| serialize
| extend Rnk = rank()
| order by Temperature desc

// prev / next（lag / lead 相当）
Events
| order by Timestamp asc
| serialize
| extend PrevTemp = prev(Temperature, 1)
| extend NextTemp = next(Temperature, 1)
| extend TempChange = Temperature - PrevTemp

// パーティション付きの row_number（重複排除）
Events
| order by Timestamp desc
| serialize
| extend rn = row_number(1, DeviceId != prev(DeviceId))
| where rn == 1
// 注意: KQL の row_number は SQL の PARTITION BY と異なる記法。
// 代替: summarize arg_max(Timestamp, *) by DeviceId の方がシンプル
```

**試験ポイント**: KQL でパーティション付きランキングは SQL ほど直感的ではない。重複排除には `summarize arg_max(Timestamp, *) by Key` が最もシンプル。

### 7.5.2 make-series（時系列生成）

```kql
// 時系列データの生成（欠損時間バケットを補完）
Events
| make-series AvgTemp = avg(Temperature) default=0.0
    on Timestamp from ago(24h) to now() step 1h
    by Region

// 結果: 各 Region × 1時間ごとに AvgTemp の配列が生成
// Region | Timestamp (配列) | AvgTemp (配列)
// East   | [T0, T1, T2,...] | [22.1, 23.5, 21.0,...]

// 時系列分析関数
Events
| make-series AvgTemp = avg(Temperature) default=0.0
    on Timestamp from ago(7d) to now() step 1h
    by DeviceId
| extend (anomalies, score, baseline) = series_decompose_anomalies(AvgTemp)
| mv-expand Timestamp to typeof(datetime), AvgTemp to typeof(real),
            anomalies to typeof(int), score to typeof(real)
| where anomalies != 0    // 異常値のみ
```

`make-series` は `summarize` と異なり、データのない時間バケットも生成する（`default` で指定）。これは時系列分析やチャート描画で欠損のないデータが必要な場合に重要。

---

## 7.6 ストリーミングエンジンの選択 `[取込変換]`

| 観点 | Eventstream | Spark 構造化ストリーミング |
|------|-----------|------------------------|
| 開発スタイル | **ノーコード**（GUI ドラッグ&ドロップ） | **コード**（PySpark） |
| レイテンシ | **ミリ秒〜秒** | **秒〜分**（マイクロバッチ） |
| 変換の複雑さ | 低〜中（フィルタ, 集計, 結合） | **高**（任意の PySpark ロジック） |
| デスティネーション | Eventhouse, Lakehouse, Activator, Custom | Lakehouse (Delta テーブル) |
| ステート管理 | Eventstream が内部管理 | checkpointLocation で管理 |
| 長時間実行 | 常時稼働（Eventstream が管理） | Spark Job Definition で常時稼働可能 |
| コスト | Eventstream 処理は CU 消費 | Spark セッションの CU 消費 |
| ユースケース | IoT テレメトリ, ログ分析, CDC ストリーム | 複雑な ETL, ML パイプライン, カスタム変換 |

**選択フローチャート**:

```
ストリーミングデータの処理が必要
  │
  ├── サブ秒のレイテンシが必要 → Eventstream → Eventhouse
  │
  ├── ノーコードで処理したい → Eventstream
  │
  ├── 複雑な変換（ML, 複雑な JOIN, UDF）が必要
  │     → Spark 構造化ストリーミング → Lakehouse
  │
  └── Eventhouse (KQL) でアドホック分析したい → Eventstream → Eventhouse
```

---

## 7.7 ストレージとショートカットの選択 `[取込変換]`

### 7.7.1 ネイティブストレージ vs ミラー化ストレージ

| | ネイティブストレージ | OneLake 可用性（ミラー化） |
|--|-----------------|----------------------|
| 格納場所 | KQL DB の内部ストレージ | OneLake（Delta Lake 形式） |
| クエリ速度 | **最速**（KQL エンジン最適化） | KQL からは同等。他エンジンからも読める |
| 他エンジンからのアクセス | × | **○**（Spark, SQL, Power BI） |
| 追加コスト | なし | OneLake ストレージ分 |

### 7.7.2 RTI のショートカット

KQL DB にもショートカットを作成可能。

| ショートカット種類 | 動作 | クエリ速度 |
|---------------|------|---------|
| **高速ショートカット** | データがインデックス化される | **高速**（ネイティブに近い） |
| **非高速ショートカット** | ゼロコピー参照のみ | **低速**（外部ストレージから都度読み込み） |

**選択基準**:

| シナリオ | 推奨 |
|---------|------|
| 頻繁にクエリされるデータ | 高速ショートカット or ネイティブ取り込み |
| たまにしか参照しないルックアップテーブル | 非高速ショートカット |
| Lakehouse の Delta データを KQL で分析したい | OneLake ショートカット |
| コストを最小化したい | 非高速ショートカット |

---

## 7.8 リアルタイムダッシュボードとアラート `[監視最適化]`

### 7.8.1 Real-Time Dashboard

KQL クエリをベースにしたダッシュボード。Power BI ではなく RTI 固有のアイテム。

```kql
// ダッシュボードタイルの KQL クエリ例

// タイル1: 直近1時間のイベント数
Events
| where Timestamp > ago(1h)
| summarize EventCount = count()
| project EventCount

// タイル2: 地域別の平均温度（時系列チャート）
Events
| where Timestamp > ago(24h)
| summarize AvgTemp = avg(Temperature) by bin(Timestamp, 15m), Region
| render timechart

// タイル3: デバイス別のエラー率
Events
| where Timestamp > ago(1h)
| summarize TotalEvents = count(), ErrorEvents = countif(EventType == "Error")
    by DeviceId
| extend ErrorRate = round(100.0 * ErrorEvents / TotalEvents, 2)
| top 10 by ErrorRate desc
| render barchart
```

パラメータ付きダッシュボード:

```kql
// パラメータ: _timeRange (デフォルト: 1h)
Events
| where Timestamp > ago(_timeRange)
| summarize count() by bin(Timestamp, _timeRange / 24), Region
| render timechart
```

### 7.8.2 Activator によるアラート

Activator はイベント駆動のルールエンジン。条件を満たしたときにアクションを実行。

```
アラートの構成:
  1. データソース: Eventstream / Eventhouse / Real-Time Dashboard
  2. 条件: Temperature > 40.0 が 5分間継続
  3. アクション:
     ├── メール通知
     ├── Teams メッセージ
     ├── Power Automate フロー起動
     └── Fabric アイテムの実行（パイプライン, ノートブック等）
```

---

## 7.9 Eventstream / Eventhouse エラーの特定と解決 `[監視最適化]`

### 7.9.1 Eventstream のエラー

| エラー | 原因 | 対処 |
|-------|------|------|
| ソース接続断 | Event Hubs / IoT Hub への接続失敗 | 接続文字列・認証情報の確認。ネットワーク設定の確認 |
| スキーマ変更 | ソースのスキーマが変更された | Eventstream のスキーマ設定を更新 |
| バックプレッシャー | デスティネーションの処理が追いつかない | デスティネーションのスケールアップ。バッチング設定の調整 |
| シリアライゼーションエラー | データフォーマットの不一致（JSON / Avro 等） | ソース設定のフォーマットを確認 |
| 変換エラー | Aggregate / Filter ノードでのランタイムエラー | 変換ロジックの確認。NULL 値の処理 |

### 7.9.2 Eventhouse / KQL DB のエラー

```kql
// 取り込み失敗の診断
.show ingestion failures
| where FailedOn > ago(24h)
| project FailedOn, OperationId, Database, Table, FailureKind, Details

// 取り込み結果の確認
.show ingestion results
| where IngestionCompletionTime > ago(1h)
| summarize count() by Status
```

| エラー | 原因 | 対処 |
|-------|------|------|
| マッピング不一致 | 取り込み時の列マッピングがテーブルスキーマと不一致 | マッピング定義を更新。テーブルスキーマを確認 |
| フォーマットエラー | CSV / JSON のパースエラー | ソースデータのフォーマットを確認 |
| クォータ超過 | 取り込みレートがキャパシティを超過 | Eventhouse のスケールアップ。バッチング間隔の調整 |
| テーブル未存在 | デスティネーションテーブルが削除された | テーブルを再作成。Eventstream のデスティネーション設定を確認 |

---

## 7.10 Eventstream / Eventhouse の最適化 `[監視最適化]`

### 7.10.1 取り込みの最適化

| 設定 | 効果 |
|------|------|
| **バッチサイズ** | 大きくするとスループット向上。レイテンシは増加 |
| **バッチ間隔** | 短くするとレイテンシ低減。スループットは低下 |
| **取り込みマッピング** | 必要な列のみマッピングしてデータ転送量を削減 |
| **データ保持ポリシー** | 古いデータを自動削除してストレージコストを削減 |

### 7.10.2 KQL クエリの最適化

```kql
// ✅ 推奨: 時間フィルタを先に適用
Events
| where Timestamp > ago(1h)     // 最初に時間でフィルタ
| where DeviceType == "Sensor"
| summarize count() by Region

// ❌ 非推奨: 時間フィルタが後
Events
| where DeviceType == "Sensor"
| where Timestamp > ago(1h)     // 全データスキャン後にフィルタ
| summarize count() by Region
```

**最適化チェックリスト**:

| 原則 | 説明 |
|------|------|
| **時間フィルタ先行** | `where Timestamp > ago(...)` を最初に置く。時間パーティションプルーニングが効く |
| **`has` > `contains`** | 文字列検索は `has` を優先（インデックス使用）。`contains` は最終手段 |
| **`project` で列を絞る** | 不要列を早期に除外して I/O 削減 |
| **`materialize()`** | 同一サブクエリを複数回使う場合にキャッシュ |
| **`summarize` のキー数を最小化** | by 句のキーが多いとメモリ消費増 |
| **`join` は小さいテーブルを右側** | KQL の join は右側をメモリに保持 |
| **`lookup` を `join` の代わりに** | 小さなディメンションテーブルの参照は `lookup` が高速 |
| **`take` で探索時のデータ量制限** | 開発中は `take 100` で結果を制限 |

---

## 7.11 確認問題

### Q1. RTI アーキテクチャ

Eventhouse について正しいものはどれか？

A) Eventhouse 自体がデータを格納する
B) 1つの Eventhouse には 1つの KQL DB しか作成できない
C) Eventhouse は非使用時に自動サスペンドし、再起動時に数秒のレイテンシがある
D) Eventhouse は Lakehouse と同じ Delta Parquet 形式でデータを格納する

<details>
<summary>解答</summary>

**C) Eventhouse は非使用時に自動サスペンドし、再起動時に数秒のレイテンシがある**

- A は不正解: Eventhouse はコンテナであり、データは KQL DB に格納
- B は不正解: 1つの Eventhouse に**複数の** KQL DB を作成可能
- D は不正解: KQL DB はネイティブ形式で格納。OneLake 可用性を有効にすると Delta 形式でも公開
</details>

### Q2. KQL 構文

以下の KQL クエリと同等の SQL はどれか？

```kql
Events
| where Timestamp > ago(1h)
| summarize EventCount = count() by bin(Timestamp, 5m), Region
| order by Timestamp asc
```

A) `SELECT COUNT(*) AS EventCount, DATEPART(mi, Timestamp)/5 AS TimeBucket, Region FROM Events WHERE Timestamp > DATEADD(HOUR, -1, GETUTCDATE()) GROUP BY Region ORDER BY Timestamp`

B) `SELECT COUNT(*) AS EventCount, DATEADD(MINUTE, DATEDIFF(MINUTE, 0, Timestamp) / 5 * 5, 0) AS TimeBucket, Region FROM Events WHERE Timestamp > DATEADD(HOUR, -1, GETUTCDATE()) GROUP BY DATEADD(MINUTE, DATEDIFF(MINUTE, 0, Timestamp) / 5 * 5, 0), Region ORDER BY TimeBucket`

C) `SELECT TOP 100 * FROM Events WHERE Timestamp > '2024-01-01' ORDER BY Region`

D) `SELECT DISTINCT Region, COUNT(*) FROM Events GROUP BY Region`

<details>
<summary>解答</summary>

**B)**

`bin(Timestamp, 5m)` は5分間隔のバケットに切り捨てる操作。SQL では `DATEADD(MINUTE, DATEDIFF(MINUTE, 0, Timestamp) / 5 * 5, 0)` で同等の丸め処理を行う。A は GROUP BY に TimeBucket が含まれていない。C, D は全く異なるクエリ。
</details>

### Q3. has vs contains

KQL クエリのパフォーマンスを改善する方法として正しいものはどれか？

```kql
Events
| where Message contains "error"
| where Timestamp > ago(24h)
| summarize count() by DeviceId
```

A) `contains` を `has` に変更する
B) `summarize` を `project` に変更する
C) `ago(24h)` を `ago(1d)` に変更する
D) `count()` を `dcount()` に変更する

<details>
<summary>解答</summary>

**A) `contains` を `has` に変更する**

`has` はトークンベースの検索でインデックスを活用でき、`contains` よりも大幅に高速。さらに、`where Timestamp > ago(24h)` を先に移動すると、時間パーティションプルーニングにより更にパフォーマンスが向上する。
</details>

### Q4. ストリーミングエンジンの選択

IoT デバイスから毎秒数千件のテレメトリデータを受信し、サブ秒のレイテンシでダッシュボードに表示したい。コーディング不要な方法を希望する。最適な構成はどれか？

A) Spark 構造化ストリーミング → Lakehouse → Power BI
B) Eventstream → Eventhouse → Real-Time Dashboard
C) Pipeline (Copy Activity) → Warehouse → Power BI
D) Dataflow Gen2 → Lakehouse → Power BI

<details>
<summary>解答</summary>

**B) Eventstream → Eventhouse → Real-Time Dashboard**

サブ秒のレイテンシ + ノーコード + リアルタイムダッシュボードの要件をすべて満たす。Spark 構造化ストリーミングは秒〜分のレイテンシでコードが必要。Pipeline と Dataflow はバッチ処理向き。
</details>

### Q5. make-series vs summarize

`make-series` と `summarize` の違いについて正しいものはどれか？

A) `make-series` は `summarize` より高速である
B) `make-series` はデータのない時間バケットを自動的に生成し、`summarize` は生成しない
C) `summarize` は時系列データに使用できない
D) `make-series` はグループ化（by 句）をサポートしない

<details>
<summary>解答</summary>

**B) `make-series` はデータのない時間バケットを自動的に生成し、`summarize` は生成しない**

`make-series` はギャップのない時系列配列を生成する（`default` で欠損値を指定）。`summarize` はデータが存在するバケットのみを返す。時系列分析やチャート描画で連続した時間軸が必要な場合は `make-series` を使用。
</details>

---

> **次章**: 第8章 セキュリティとガバナンス（ワークスペースロール、アイテムレベル権限、秘密度ラベル、承認）
