# 付録 D：Power Query M クイックリファレンス

> Dataflow Gen2 で使用する Power Query M 言語の主要関数

---

## D.1 クエリ構造

```m
let
    Step1 = <expression>,          // 各ステップは前のステップを参照
    Step2 = Table.SelectRows(Step1, each [Amount] > 0),
    Step3 = Table.AddColumn(Step2, "Tax", each [Amount] * 0.1, type number)
in
    Step3                           // 最終結果を返す
```

---

## D.2 Table 関数

```m
// ── 行フィルタ ──
Table.SelectRows(Source, each [Status] <> "Cancelled")
Table.SelectRows(Source, each [Amount] > 100 and [Region] = "East")
Table.SelectRows(Source, each List.Contains({"East","West"}, [Region]))

// ── 列選択 / 除外 / 名前変更 ──
Table.SelectColumns(Source, {"Col1", "Col2", "Col3"})
Table.RemoveColumns(Source, {"TempCol", "DebugCol"})
Table.RenameColumns(Source, {{"OldName","NewName"}, {"Old2","New2"}})

// ── 列追加 ──
Table.AddColumn(Source, "Revenue", each [Price] * [Qty], type number)
Table.AddColumn(Source, "Size",
    each if [Amount] >= 10000 then "Large"
         else if [Amount] >= 1000 then "Medium"
         else "Small", type text)

// ── 型変換 ──
Table.TransformColumnTypes(Source, {
    {"OrderDate", type date}, {"Amount", type number}, {"Qty", Int64.Type}
})

// ── 値変換 ──
Table.TransformColumns(Source, {
    {"Name", Text.Upper, type text},
    {"Email", Text.Trim, type text}
})

// ── NULL / 値置換 ──
Table.ReplaceValue(Source, null, 0, Replacer.ReplaceValue, {"Amount"})
Table.ReplaceValue(Source, null, "Unknown", Replacer.ReplaceValue, {"Name"})

// ── ヘッダー昇格 ──
Table.PromoteHeaders(Source, [PromoteAllScalars = true])

// ── ソート ──
Table.Sort(Source, {{"Amount", Order.Descending}, {"Name", Order.Ascending}})

// ── 先頭/末尾行 ──
Table.FirstN(Source, 100)
Table.LastN(Source, 10)
Table.Skip(Source, 5)

// ── 重複排除 ──
Table.Distinct(Source, {"OrderId"})
```

---

## D.3 結合と集計

```m
// ── JOIN（NestedJoin → Expand パターン）──
// Inner Join
Joined = Table.NestedJoin(Orders, {"CustId"}, Customers, {"CustId"}, "CustData", JoinKind.Inner),
Expanded = Table.ExpandTableColumn(Joined, "CustData", {"Name","Region"}, {"CustName","CustRegion"})

// Left Join
Table.NestedJoin(Orders, {"CustId"}, Customers, {"CustId"}, "CustData", JoinKind.LeftOuter)

// Anti Join (NOT EXISTS)
Table.NestedJoin(Orders, {"CustId"}, Customers, {"CustId"}, "CustData", JoinKind.LeftAnti)

// JoinKind: Inner, LeftOuter, RightOuter, FullOuter, LeftAnti, RightAnti

// ── UNION（テーブル結合）──
Table.Combine({Table1, Table2, Table3})

// ── GROUP BY + 集計 ──
Table.Group(Source, {"Region", "Category"}, {
    {"Count", each Table.RowCount(_), Int64.Type},
    {"Total", each List.Sum([Amount]), type number},
    {"Avg",   each List.Average([Amount]), type number},
    {"Min",   each List.Min([Amount]), type number},
    {"Max",   each List.Max([Amount]), type number}
})

// ── PIVOT / UNPIVOT ──
Table.Pivot(Source, List.Distinct(Source[Category]), "Category", "Amount", List.Sum)
Table.UnpivotOtherColumns(Source, {"Region","Year"}, "Metric", "Value")
```

---

## D.4 スカラー関数

```m
// ── 文字列 ──
Text.Upper("hello")                       // "HELLO"
Text.Lower("HELLO")                       // "hello"
Text.Trim("  hello  ")                    // "hello"
Text.TrimStart(" hello")                  // "hello"
Text.TrimEnd("hello ")                    // "hello"
Text.Contains("hello world", "world")     // true
Text.StartsWith("hello", "hel")           // true
Text.EndsWith("hello", "llo")             // true
Text.Replace("2024-01-15", "-", "/")      // "2024/01/15"
Text.Start("hello", 3)                    // "hel"
Text.End("hello", 3)                      // "llo"
Text.Middle("hello", 1, 3)               // "ell"
Text.Length("hello")                       // 5
Text.Split("a,b,c", ",")                 // {"a","b","c"}
Text.Combine({"a","b","c"}, "-")         // "a-b-c"

// ── 数値 ──
Number.Round(3.456, 2)                    // 3.46
Number.RoundDown(3.9)                     // 3
Number.RoundUp(3.1)                       // 4
Number.Abs(-5)                            // 5
Number.Power(2, 10)                       // 1024
Number.Sqrt(144)                          // 12

// ── 日付 ──
Date.Year(#date(2024,6,15))               // 2024
Date.Month(#date(2024,6,15))              // 6
Date.Day(#date(2024,6,15))                // 15
Date.DayOfWeek(#date(2024,6,15))          // 6 (Saturday)
Date.AddDays(#date(2024,1,1), 30)         // #date(2024,1,31)
Date.AddMonths(#date(2024,1,1), 3)        // #date(2024,4,1)
Date.StartOfMonth(#date(2024,6,15))       // #date(2024,6,1)
Date.EndOfMonth(#date(2024,6,15))         // #date(2024,6,30)
DateTime.LocalNow()                        // 現在日時
Duration.Days(#duration(5,3,0,0))         // 5

// ── 型変換 ──
Number.FromText("42")                     // 42
Text.From(42)                             // "42"
Date.FromText("2024-06-15")               // #date(2024,6,15)
```

---

## D.5 エラーハンドリング

```m
// try ... otherwise（型変換エラーの安全な処理）
Table.TransformColumns(Source, {
    {"Amount", each try Number.FromText(_) otherwise 0, type number}
})

// エラー行のフィルタリング
Table.RemoveRowsWithErrors(Source, {"Amount", "OrderDate"})
```

---

## D.6 フォールディング確認

```
クエリステップを右クリック → "View Native Query"
  → 表示可能: フォールドされている（ソース側で処理）
  → 表示不可（グレーアウト）: フォールドが壊れている（Mashup エンジンで処理）
```

フォールドが壊れやすい操作: `Table.Buffer`, M 固有の複雑な関数, ソースが CSV/Excel/Web API の場合
