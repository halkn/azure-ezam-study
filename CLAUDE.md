# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## リポジトリ概要

Microsoft Fabric データエンジニア試験 **DP-700**（Microsoft Certified: Fabric Data Engineer Associate）の学習教材リポジトリ。教材はすべて日本語の Markdown ファイルで構成されている。ビルド・テスト・リントのコマンドはない。

## コンテンツ構造

```
exam/dp-700/
├── DP-700-textbook-outline.md   # 教科書の章立て一覧（目次）
├── DP-700-study-plan.md         # 公式ラーニングパスと試験範囲のマッピング
├── chapters/                    # 章ファイル（ch00〜ch10）
│   ├── ch00-fabric-architecture.md
│   ├── ch01-onelake-shortcuts.md
│   ├── ch02-lakehouse-delta-lake.md
│   ├── ch03-spark-notebooks.md
│   ├── ch04-dataflow-gen2.md
│   ├── ch05-pipelines-orchestration.md
│   ├── ch06-data-warehouse.md
│   ├── ch07-real-time-intelligence.md
│   ├── ch08-security-governance.md
│   ├── ch09-lifecycle-management.md
│   └── ch10-monitoring-troubleshooting.md
└── appendix/
    ├── appendix-a-pyspark-reference.md
    ├── appendix-b-kql-reference.md
    ├── appendix-c-tsql-reference.md
    ├── appendix-d-powerquery-m-reference.md
    └── appendix-e-exam-skill-lookup.md   # 試験スキル → 章の逆引き表
```

## 試験範囲と章の対応

DP-700 の試験は3ドメインで均等構成（各 30–35%）。

| 試験ドメイン | タグ | 主な章 |
|------------|------|-------|
| 分析ソリューションの実装と管理 | `[実装管理]` | ch01, ch08, ch09 |
| データの取り込みと変換 | `[取込変換]` | ch02〜ch07 |
| 分析ソリューションの監視と最適化 | `[監視最適化]` | ch10（+ 各章の最適化節） |

各章ファイルの節見出しには `[実装管理]` などのタグが付いており、どの試験ドメインに該当するかがわかる。

**付録E（appendix-e-exam-skill-lookup.md）** が試験スキル項目から教科書の節番号を逆引きするインデックスとして機能する。章の追加・更新時はここのマッピングも合わせて更新する。

## コンテンツ規約

- **言語**: 日本語
- **コードブロック**: PySpark は `python`、KQL は `kql`、T-SQL は `sql`、Power Query M は `m`、パイプライン式は `json` のシンタックスハイライトを使用
- **試験ポイント**: 太字や「**試験ポイント**:」見出しで強調
- **公式モジュールのカバーが薄い箇所**: `> ⚠️ 公式ドキュメント補完推奨` のコールアウトで明示
- **確認問題**: `<details><summary>解答</summary>` の折りたたみ形式で解答を収録
- **MS Learn 対応**: 各章の冒頭に `> 📘 対応 MS Learn モジュール:` を記載

## ブランチ運用

`main` ブランチがメインブランチ。章単位で `feat/dp-700` のようなブランチを切って作業する。
