# Azure 試験学習教材

Microsoft Azure / Microsoft Fabric 認定資格の学習教材リポジトリ。
教材はすべて日本語の Markdown ファイルで構成されています。

## 収録試験一覧

| 試験コード | 認定資格名 | ステータス |
|-----------|-----------|----------|
| [DP-700](exam/dp-700/) | Microsoft Certified: Fabric Data Engineer Associate | 作成中 |

> 今後、他の Azure 認定試験の教材も順次追加予定。

## ディレクトリ構造

```
exam/
└── dp-700/
    ├── DP-700-textbook-outline.md   # 教科書の章立て一覧（目次）
    ├── DP-700-study-plan.md         # 公式ラーニングパスと試験範囲のマッピング
    ├── chapters/                    # 章ファイル（ch00〜ch10）
    │   ├── ch00-fabric-architecture.md      # Microsoft Fabric アーキテクチャ
    │   ├── ch01-onelake-shortcuts.md        # OneLake とショートカット
    │   ├── ch02-lakehouse-delta-lake.md     # レイクハウスと Delta Lake
    │   ├── ch03-spark-notebooks.md          # Spark ノートブック
    │   ├── ch04-dataflow-gen2.md            # Dataflow Gen2
    │   ├── ch05-pipelines-orchestration.md  # パイプラインとオーケストレーション
    │   ├── ch06-data-warehouse.md           # データウェアハウス
    │   ├── ch07-real-time-intelligence.md   # リアルタイムインテリジェンス
    │   ├── ch08-security-governance.md      # セキュリティとガバナンス
    │   ├── ch09-lifecycle-management.md     # ライフサイクル管理
    │   └── ch10-monitoring-troubleshooting.md # 監視とトラブルシューティング
    └── appendix/
        ├── appendix-a-pyspark-reference.md       # PySpark リファレンス
        ├── appendix-b-kql-reference.md            # KQL リファレンス
        ├── appendix-c-tsql-reference.md           # T-SQL リファレンス
        ├── appendix-d-powerquery-m-reference.md   # Power Query M リファレンス
        └── appendix-e-exam-skill-lookup.md        # 試験スキル逆引きインデックス
```

## DP-700 について

**Microsoft Certified: Fabric Data Engineer Associate**

Microsoft Fabric を使用したデータエンジニアリングソリューションの設計・実装・管理スキルを問う試験。

### 試験ドメイン

| ドメイン | 出題割合 | 主な章 |
|---------|---------|-------|
| 分析ソリューションの実装と管理 | 30〜35% | ch01, ch08, ch09 |
| データの取り込みと変換 | 30〜35% | ch02〜ch07 |
| 分析ソリューションの監視と最適化 | 30〜35% | ch10（+ 各章の最適化節） |

### 学習の進め方

1. **[学習プラン](exam/dp-700/DP-700-study-plan.md)** で全体像を把握する
2. **[目次](exam/dp-700/DP-700-textbook-outline.md)** で章立てを確認する
3. 各章ファイルを順番に読み進める（ch00 → ch10）
4. **[付録E](exam/dp-700/appendix/appendix-e-exam-skill-lookup.md)** で試験スキル項目と章の対応を確認する
5. 付録A〜Dのリファレンスを適宜参照する

## ライセンス

[MIT License](LICENSE)
