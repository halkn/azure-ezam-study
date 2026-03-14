# 付録 B：KQL クイックリファレンス

> Kusto Query Language (KQL) の試験対策チートシート。SQL との対応表付き。

---

## B.1 SQL → KQL 対応表

| SQL | KQL | 例 |
|-----|-----|----|
| `SELECT col1, col2` | `project col1, col2` | `T \| project Name, Age` |
| `SELECT *, col+1 AS new` | `extend new = col+1` | `T \| extend AgePlus = Age+1` |
| `SELECT TOP 10 *` | `take 10` or `top 10 by col` | `T \| take 10` |
| `SELECT DISTINCT col` | `distinct col` | `T \| distinct Region` |
| `WHERE condition` | `where condition` | `T \| where Age > 30` |
| `ORDER BY col DESC` | `sort by col desc` | `T \| sort by Age desc` |
| `GROUP BY col` + `COUNT(*)` | `summarize count() by col` | `T \| summarize count() by Region` |
| `HAVING count > 5` | `summarize ... \| where ...` | `T \| summarize c=count() by R \| where c>5` |
| `JOIN` | `join kind=inner (...) on key` | `T1 \| join kind=inner (T2) on Id` |
| `LEFT JOIN` | `join kind=leftouter` | `T1 \| join kind=leftouter (T2) on Id` |
| `NOT EXISTS` | `join kind=leftanti` | `T1 \| join kind=leftanti (T2) on Id` |
| `UNION ALL` | `union T1, T2` | `T1 \| union T2` |
| `CASE WHEN` | `case(cond, val, ...)` or `iff(cond, true, false)` | `extend s = iff(Age>30,"Sr","Jr")` |
| `DATEADD(HOUR,-1,GETUTCDATE())` | `ago(1h)` | `T \| where ts > ago(1h)` |
| `DATEDIFF(DAY, a, b)` | `datetime_diff("day", b, a)` | `datetime_diff("day", now(), ts)` |
| `CAST(x AS INT)` | `toint(x)` | `extend n = toint(str_col)` |
| `LIKE '%text%'` | `contains "text"` | `T \| where Msg contains "err"` |
| `LIKE '%text%'` (高速) | `has "text"` | `T \| where Msg has "error"` (**推奨**) |
| `LATERAL FLATTEN` | `mv-expand` | `T \| mv-expand Tags` |
| `LAG(col,1)` | `prev(col,1)` (要 serialize) | `T \| serialize \| extend p=prev(val)` |
| `ROW_NUMBER()` | `row_number()` (要 serialize) | `T \| serialize \| extend rn=row_number()` |
| 変数宣言 | `let x = ...;` | `let t = 1h; T \| where ts > ago(t)` |

---

## B.2 テーブル演算子

```kql
// ── フィルタ ──
T | where Timestamp > ago(1h)
T | where Status == "Error" and Severity >= 3
T | where Region in ("East", "West")
T | where Region !in ("Unknown")
T | where Message has "timeout"              // 単語一致（高速）
T | where Message contains "time"            // 部分一致（低速）
T | where Message startswith "Error:"
T | where Message matches regex @"\d{3}-\d{4}"
T | where isnotnull(ErrorCode)
T | where Timestamp between (datetime(2024-01-01) .. datetime(2024-01-31))

// ── 列操作 ──
T | project Col1, Col2, NewName = OldName    // 選択 + リネーム
T | project-away InternalId, DebugFlag       // 除外
T | project-rename EventTime = Timestamp     // リネームのみ
T | extend TempF = TempC * 9.0/5.0 + 32.0   // 列追加

// ── ソート / 制限 ──
T | sort by Timestamp desc
T | top 10 by Amount desc                    // ORDER BY + LIMIT
T | take 100                                 // 順序不定で N 件

// ── 重複排除 ──
T | distinct Region, Category
T | summarize arg_max(Timestamp, *) by DeviceId   // 各デバイスの最新行（最も実用的）

// ── 結合 ──
T1 | join kind=inner (T2 | project Id, Name) on Id
T1 | join kind=leftouter (T2) on $left.CustId == $right.CustomerId
T1 | join kind=leftanti (T2) on Id           // T2 にない T1 の行
T1 | lookup (DimTable) on Key                // 小テーブル参照（join より高速）
T1 | union T2                                // 行の結合（UNION ALL）

// ── 変数 / 関数 ──
let threshold = 30.0;
let timeRange = 24h;
let MyFunc = (x: real) { x * 1.8 + 32.0 };
T | where Temperature > threshold | extend TempF = MyFunc(Temperature)
```

---

## B.3 集計（summarize）

```kql
T | summarize
    Count = count(),
    Total = sum(Amount),
    Avg = avg(Amount),
    Min = min(Amount),
    Max = max(Amount),
    UniqueCount = dcount(CustomerId),
    P50 = percentile(ResponseTime, 50),
    P95 = percentile(ResponseTime, 95),
    Items = make_list(ProductName),
    UniqueItems = make_set(ProductName)
  by Region, bin(Timestamp, 1h)

// arg_max / arg_min（最大/最小値の行の他列を取得）
T | summarize arg_max(Timestamp, *) by DeviceId
T | summarize arg_min(Price, ProductName) by Category

// countif / sumif（条件付き集計）
T | summarize
    TotalEvents = count(),
    ErrorCount = countif(EventType == "Error"),
    HighValueSum = sumif(Amount, Amount > 1000)
  by Region
```

**bin() のバリエーション**:
`bin(Timestamp, 5m)` / `bin(Timestamp, 1h)` / `bin(Timestamp, 1d)` / `bin(Amount, 100)`

---

## B.4 時系列（make-series）

```kql
// 時系列生成（欠損バケットを default で補完）
T | make-series AvgTemp = avg(Temperature) default=0.0
    on Timestamp from ago(24h) to now() step 1h
    by Region

// summarize との違い: make-series はデータのない時間バケットも生成する

// 異常検出
T | make-series Val = avg(Metric) default=0.0 on Timestamp from ago(7d) to now() step 1h
  | extend (anomalies, score, baseline) = series_decompose_anomalies(Val)

// 線形回帰
T | make-series Val = avg(Metric) on Timestamp from ago(30d) to now() step 1d
  | extend (slope, intercept, r2) = series_fit_line(Val)
```

---

## B.5 スカラー関数

```kql
// ── 文字列 ──
strlen("hello")              // 5
toupper("abc")               // "ABC"
tolower("ABC")               // "abc"
trim(" hello ")              // "hello"
strcat("a", "-", "b")        // "a-b"
substring("hello", 1, 3)     // "ell"
replace_string("a-b", "-", "/")  // "a/b"
split("a,b,c", ",")          // ["a","b","c"]
split("a,b,c", ",")[1]       // "b"
extract(@"(\d+)", 1, "id-42")  // "42"
parse_json('{"a":1}')        // dynamic

// ── 数値 ──
round(3.456, 2)    // 3.46
floor(3.9, 1)      // 3
ceiling(3.1)       // 4
abs(-5)            // 5
pow(2, 10)         // 1024
log10(100)         // 2

// ── 日時 ──
now()                          // 現在 UTC
ago(1h)                        // 1時間前
datetime(2024-06-15)           // リテラル
datetime_diff("day", now(), datetime(2024-01-01))
datetime_add("month", 3, datetime(2024-01-01))
format_datetime(now(), "yyyy-MM-dd HH:mm")
startofday(now())              // 今日 00:00
startofweek(now())             // 今週月曜
startofmonth(now())            // 今月1日
bin(now(), 1h)                 // 時間の丸め

// ── 型変換 ──
toint("42")       tolong("12345678901")
toreal("3.14")    todecimal("9.99")
tostring(42)      tobool("true")
todatetime("2024-06-15")    totimespan("01:30:00")

// ── 動的型（JSON / 配列）──
parse_json('{"name":"test","tags":["a","b"]}')
T | extend obj = parse_json(JsonCol)
  | extend name = tostring(obj.name)
  | extend first_tag = tostring(obj.tags[0])
  | mv-expand tag = obj.tags         // 配列を行に展開
  | extend key_list = bag_keys(obj)  // オブジェクトのキー一覧
```

---

## B.6 ウィンドウ関数

```kql
// serialize が必要（行の順序を確定）
T | order by Timestamp asc | serialize
  | extend rn = row_number()
  | extend prev_val = prev(Value, 1)
  | extend next_val = next(Value, 1)
  | extend change = Value - prev(Value, 1)

// パーティション付き重複排除の推奨パターン
T | summarize arg_max(Timestamp, *) by DeviceId
// ↑ row_number + partition より簡潔で高速
```

---

## B.7 管理コマンド（ドット付き）

```kql
// 取り込み失敗の確認
.show ingestion failures | where FailedOn > ago(24h)

// テーブル一覧
.show tables

// テーブルスキーマ
.show table TableName schema

// データベース統計
.show database datastats

// 保持ポリシー
.show table TableName policy retention

// 可視化
T | summarize count() by bin(Timestamp, 1h) | render timechart
T | summarize count() by Region | render barchart
T | summarize count() by Category | render piechart
```
