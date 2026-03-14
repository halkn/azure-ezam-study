# 第9章 ライフサイクル管理（CI/CD）

> 📘 対応 MS Learn モジュール:
> - Implement continuous integration and continuous delivery (CI/CD) in Microsoft Fabric
> - Administer a Microsoft Fabric environment
>
> 試験タグ: `[実装管理]`

---

## 9.1 バージョンコントロールの構成

### 9.1.1 Git 連携の概要

Fabric の Git 連携は、ワークスペース内の Fabric アイテムの定義（JSON / コード）を Git リポジトリと双方向同期する仕組み。

```
開発フロー:

Developer → Fabric ワークスペース（Dev）
  ↕ 同期（Commit / Update）
Git リポジトリ（Azure DevOps or GitHub）
  → PR → レビュー → Merge
    → Release Pipeline or Deployment Pipeline
      → Test ワークスペース → Prod ワークスペース
```

### 9.1.2 対応 Git プロバイダー

| プロバイダー | 状態 | 認証 |
|------------|------|------|
| **Azure DevOps** | GA（推奨） | Microsoft Entra ID（SSO）/ サービスプリンシパル |
| **GitHub** | GA（一部制限あり） | Fine-grained token or Classic token（repo スコープ） |

**Azure DevOps が推奨**される理由: GitHub より制限が少なく、クロステナント接続もサポート。

### 9.1.3 Git 連携の接続手順

```
1. ワークスペース設定 → Git integration
2. Git プロバイダーを選択（Azure DevOps or GitHub）
3. 接続情報を入力:
   Azure DevOps: Organization → Project → Repository → Branch → Folder
   GitHub: Repository URL → Branch → Folder
4. Connect and sync
```

**前提条件**:
- **ワークスペース Admin のみ**が Git 接続を管理可能
- テナント設定で Git 連携が有効化されていること
- Power BI Premium ライセンスまたは Fabric 容量が必要

### 9.1.4 Git 連携でサポートされるアイテム

| アイテム | Git サポート | 備考 |
|---------|------------|------|
| Notebook | ○ | .py ファイルとして格納 |
| Pipeline | ○ | JSON 定義 |
| Dataflow Gen2 | ○ | mashup ドキュメント |
| Lakehouse | ○ | メタデータのみ（データは同期されない） |
| Warehouse | ○ | メタデータのみ |
| Environment | ○ | 構成ファイル |
| Semantic Model | ○（プレビュー） | TMDL 形式 |
| Report | ○（プレビュー） | PBIR 形式 |
| KQL Queryset | ○ | |
| Spark Job Definition | ○ | |
| Copy Job | ○ | |
| Apache Airflow Job | ○ | DAG ファイル |
| Variable Library | ○ | |

**重要**: Git 連携は**アイテムの定義（メタデータ / コード）のみ**を同期する。**データは同期されない**。Lakehouse のテーブルデータや Warehouse の行データは Git の管理対象外。

### 9.1.5 コミットと同期

```
ワークスペースでの変更 → Git に反映:
  Source control パネル → Changes タブ → 変更を選択 → Commit

Git での変更 → ワークスペースに反映:
  Source control パネル → Updates タブ → 更新を選択 → Update
```

**競合解決**:
- ワークスペースと Git の両方で同じアイテムが変更された場合、競合が発生
- 競合時は「ワークスペースの変更を優先」または「Git の変更を優先」を選択
- 複雑な競合は Git 側（PR 上）で解決するのが推奨

### 9.1.6 ブランチ戦略

**パターン1: Trunk-based（推奨）**

```
main（本番）
  └── feature/add-new-pipeline ← 開発ブランチ
        └── PR → main にマージ → Dev ワークスペースに同期
```

シンプルで小規模チーム向き。全環境が同じ main ブランチから派生。

**パターン2: Gitflow（大規模向き）**

```
main（本番）
  ├── release/v1.2 ← リリースブランチ
  │     └── Test ワークスペースと接続
  └── develop ← 開発ブランチ
        ├── Dev ワークスペースと接続
        └── feature/xxx ← 機能ブランチ
```

各環境（Dev / Test / Prod）が異なるブランチに接続。PR でブランチ間を昇格。

### 9.1.7 Git 連携の制約

| 制約 | 詳細 |
|------|------|
| ワークスペースアイテム上限 | Git 接続ワークスペースは**最大 1,000 アイテム** |
| 1回のコミットサイズ | Azure DevOps: 最大 **125 MB**、GitHub: 最大 **50 MB** |
| MyWorkspace | Git 接続不可 |
| テンプレートアプリ | Git 接続不可 |
| オンプレミス GitHub | 非サポート |
| データの同期 | **非サポート**（メタデータ/コードのみ） |

---

## 9.2 デプロイパイプラインの作成と構成

### 9.2.1 デプロイパイプラインとは

Fabric 組み込みのデプロイメントツール。ワークスペース間でアイテムを段階的にデプロイする。

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│ Development │ ──→ │    Test     │ ──→ │ Production  │
│ (ワークスペース) │     │ (ワークスペース) │     │ (ワークスペース) │
└─────────────┘     └─────────────┘     └─────────────┘
      ↑ Git 連携         デプロイ            デプロイ
```

### 9.2.2 構成手順

```
1. ワークスペース → New → Deployment pipeline
2. パイプライン名を入力（例: "Sales-ETL-Deploy"）
3. ステージを構成:
   ├── Development ← ワークスペース "Sales-Dev" を割り当て
   ├── Test        ← ワークスペース "Sales-Test" を割り当て
   └── Production  ← ワークスペース "Sales-Prod" を割り当て
4. デプロイルールの設定（オプション）
5. デプロイの実行
```

### 9.2.3 デプロイルール

環境ごとに異なる設定を適用するためのルール。

```
例: データソース接続の切り替え

Development:
  Pipeline の Copy アクティビティ → ソース: dev-sql-server.database.windows.net
  Lakehouse → lh-sales-dev

Test:
  Pipeline の Copy アクティビティ → ソース: test-sql-server.database.windows.net
  Lakehouse → lh-sales-test

Production:
  Pipeline の Copy アクティビティ → ソース: prod-sql-server.database.windows.net
  Lakehouse → lh-sales-prod

→ デプロイルールでデータソース / Lakehouse の参照先を環境ごとに上書き
```

設定可能なデプロイルール:
- **データソースルール**: 接続文字列 / サーバー名 / データベース名を環境ごとに切り替え
- **パラメータルール**: パイプラインパラメータの値を環境ごとに変更

### 9.2.4 Variable Library

Variable Library は環境ごとの変数値を一元管理する Fabric アイテム。

```
Variable Library: "sales-config"
  ├── Variable: source_connection_id
  │     ├── Dev:  conn-dev-sql-12345
  │     ├── Test: conn-test-sql-67890
  │     └── Prod: conn-prod-sql-abcde
  ├── Variable: lakehouse_id
  │     ├── Dev:  lh-id-dev
  │     ├── Test: lh-id-test
  │     └── Prod: lh-id-prod
  └── Variable: notification_email
        ├── Dev:  dev-team@contoso.com
        ├── Test: qa-team@contoso.com
        └── Prod: ops-team@contoso.com
```

サポートされるアイテム:
- **Data Pipeline**: 接続 ID、パラメータ値
- **Copy Job**: 接続 ID
- **Lakehouse ショートカット**: ショートカット構成のパラメータ化
- **Notebook**: 変数参照（IntelliSense 対応、2025年7月以降）

### 9.2.5 Git 連携とデプロイパイプラインの併用パターン

**パターン A: Git → Dev のみ。Dev → Test → Prod はデプロイパイプライン**

```
Git repo (main branch)
  ↕ 同期
Dev ワークスペース → [デプロイパイプライン] → Test → Prod
```

シンプル。Git は Dev のソースコントロールのみに使い、環境間のデプロイは Fabric の組み込みツール。

**パターン B: Git がすべてのソース。各環境は Git からデプロイ**

```
Git repo
  ├── main branch → Release pipeline → Prod ワークスペース
  ├── release branch → Release pipeline → Test ワークスペース
  └── develop branch → Git sync → Dev ワークスペース
```

Git が Single Source of Truth。fabric-cicd Python ライブラリや Fabric REST API を使って各環境にデプロイ。

**試験ポイント**: パターン A が最もシンプルで試験でも問われやすい。Git 連携は Dev のみ + デプロイパイプラインで Test → Prod。

### 9.2.6 デプロイの比較と履歴

- デプロイ前にステージ間の**差分比較**が可能（新規 / 変更 / 削除されたアイテムを確認）
- デプロイ後は**履歴**で過去のデプロイを確認可能

---

## 9.3 データベースプロジェクト

### 9.3.1 概要

データベースプロジェクトは、Warehouse のスキーマ（テーブル、ビュー、ストアドプロシージャ等）をコードとして管理し、ソースコントロールと CI/CD に統合する仕組み。

```
Warehouse のスキーマ定義
  → データベースプロジェクト（SQL プロジェクト）として抽出
    → Git リポジトリに格納
      → 別環境の Warehouse にデプロイ（スキーマ比較 + 差分適用）
```

### 9.3.2 ワークフロー

```
1. Warehouse → Settings → Source control → Database project
2. スキーマ定義を SQL プロジェクトとしてエクスポート
3. Git にコミット
4. 別環境で:
   a. スキーマ比較（ソース: Git のプロジェクト vs ターゲット: Warehouse）
   b. 差分スクリプトの生成
   c. スクリプトの適用（テーブル追加、列変更、SP 更新等）
```

### 9.3.3 データベースプロジェクトのメリット

- Warehouse のスキーマをバージョン管理
- 環境間でスキーマの一貫性を保証
- PR レビューでスキーマ変更を事前確認
- 自動化されたスキーマデプロイ（CI/CD パイプラインから）

---

## 9.4 ワークスペース設定の構成 `[実装管理]`

試験スキル「Microsoft Fabric ワークスペース設定を構成する」は4つのサブスキルで構成される。これまでの章で個別に扱ったものを一覧で整理する。

### 9.4.1 Spark ワークスペース設定

→ **第3章 3.1.2** で詳述

```
ワークスペース設定 → Data Engineering/Science → Spark settings
  ├── Pool タブ: デフォルトプール（Starter / Custom）、Custom Pool の作成
  ├── Environment タブ: デフォルト環境、Runtime バージョン
  ├── High Concurrency: 高並行性モードの有効化
  └── Automatic Log: Spark イベントログの自動保存
```

| 設定項目 | 影響範囲 | 変更者 |
|---------|---------|-------|
| デフォルト Spark プール | ワークスペース内の全 Spark ジョブ | ワークスペース Admin |
| Runtime バージョン | デフォルト環境なしの場合の Spark バージョン | ワークスペース Admin |
| Customize compute for items | 個別アイテムが環境でコンピュートをオーバーライド可能か | ワークスペース Admin |
| Custom Spark プール作成 | ワークスペースで利用可能なプール | ワークスペース Admin（容量管理者の設定が前提） |

### 9.4.2 ドメインワークスペース設定

```
ドメイン = ワークスペースの論理グループ（データメッシュの単位）

テナント管理ポータル → Domains
  ├── ドメインの作成（例: Sales, Marketing, Finance）
  ├── ワークスペースの割り当て（手動 or ルールベース）
  ├── ドメイン管理者の指定
  └── ドメインレベルのポリシー:
        ├── 承認ポリシー（Certification の認定者指定）
        └── コントリビューターの設定

ワークスペース設定 → General → Domain
  └── このワークスペースのドメイン割り当てを確認/変更
```

**試験ポイント**: ドメインは**テナント管理者**が作成する。ワークスペースのドメインへの割り当ては、テナント管理者またはドメイン管理者が行う。

### 9.4.3 OneLake ワークスペース設定

→ **第1章 1.4** で詳述

```
ワークスペース設定 → OneLake
  ├── ショートカットキャッシュ: ON/OFF
  ├── キャッシュ保持期間: 1〜28日
  ├── キャッシュのリセット
  └── OneLake 診断: 有効化/保持期間
```

### 9.4.4 データワークフロー ワークスペース設定

→ **第5章 5.5.2** で詳述

```
ワークスペース設定 → Data Factory → Data Workflow Settings
  ├── Default Data Workflow Setting: Starter Pool / Custom Pool
  └── Custom Pool 構成:
        ├── Name
        ├── Compute node size: Small / Large
        ├── Enable auto-pause
        └── Auto-pause delay
        └── Triggerers: 有効化（deferrable operator 用）
```

### 9.4.5 設定の横断整理

| 試験スキル | 設定場所 | 詳細参照 |
|-----------|---------|---------|
| Spark ワークスペース設定を構成する | Data Engineering/Science → Spark settings | 3.1.2 |
| ドメイン ワークスペース設定を構成する | テナント管理ポータル → Domains | 9.4.2 |
| OneLake ワークスペース設定を構成する | ワークスペース設定 → OneLake | 1.4 |
| データワークフロー ワークスペース設定を構成する | Data Factory → Data Workflow Settings | 5.5.2 |

---

## 9.5 管理者タスク

### 9.5.1 容量の管理

```
Azure ポータル → Fabric 容量リソース
  ├── SKU の変更（スケールアップ/ダウン）
  ├── 一時停止 / 再開
  ├── リージョンの確認
  └── ワークスペースの割り当て

Fabric 管理ポータル → Capacity settings
  ├── Capacity Metrics アプリ（CU 使用状況の可視化）
  ├── スロットリングの監視
  └── ワークロードごとの CU 消費量
```

### 9.5.2 テナント設定

テナント管理者が制御する主要な設定:

| 設定カテゴリ | 主な項目 |
|------------|---------|
| Git integration | Git 連携の有効/無効、許可されるプロバイダー |
| Deployment pipelines | デプロイパイプラインの有効/無効 |
| Export data | データのエクスポート制限 |
| External sharing | 外部共有の許可/禁止 |
| Spark | Customized workspace pools の有効/無効 |
| Data workflows | Apache Airflow Job の有効/無効 |
| OneLake | OneLake 設定のデフォルト |

---

## 9.6 確認問題

### Q1. Git 連携

Fabric の Git 連携について正しいものはどれか？

A) ワークスペースの Member ロールがあれば Git 接続を構成できる
B) Lakehouse のテーブルデータも Git リポジトリに同期される
C) ワークスペース Admin のみが Git 接続を管理でき、データではなくアイテム定義のみが同期される
D) GitHub On-premises Enterprise Server がサポートされる

<details>
<summary>解答</summary>

**C) ワークスペース Admin のみが Git 接続を管理でき、データではなくアイテム定義のみが同期される**

- A は不正解: Git 接続の管理は **Admin のみ**
- B は不正解: データは同期されない。アイテムの定義（JSON / コード / メタデータ）のみ
- D は不正解: オンプレミスの GitHub Enterprise Server は非サポート
</details>

### Q2. デプロイパイプライン

デプロイパイプラインについて正しいものはどれか？

A) デプロイパイプラインはアイテムのデータも含めてデプロイする
B) デプロイルールを使って環境ごとにデータソース接続を切り替えられる
C) デプロイパイプラインは2ステージ（Dev → Prod）のみで構成される
D) デプロイパイプラインは Git 連携なしでは使用できない

<details>
<summary>解答</summary>

**B) デプロイルールを使って環境ごとにデータソース接続を切り替えられる**

- A は不正解: デプロイされるのはアイテムの定義。データは移動しない
- C は不正解: 通常3ステージ（Dev → Test → Prod）だがカスタマイズ可能
- D は不正解: デプロイパイプラインは Git 連携とは独立して使用可能（ただし併用が推奨）
</details>

### Q3. Variable Library

Variable Library について正しいものはどれか？

A) Variable Library はパイプラインの実行時にのみ参照可能で、デプロイ時には使えない
B) Variable Library を使うと、同一のパイプライン定義を Dev / Test / Prod で異なる接続先で実行できる
C) Variable Library の値はワークスペースロールに関係なく全ユーザーが閲覧できる
D) Variable Library は Notebook コード内で直接参照できない

<details>
<summary>解答</summary>

**B) Variable Library を使うと、同一のパイプライン定義を Dev / Test / Prod で異なる接続先で実行できる**

Variable Library はステージ（環境）ごとに異なる値セットを持てるため、パイプラインのハードコーディングを排除して環境間のデプロイを簡素化する。D は不正解（2025年7月以降 Notebook からの参照がサポート）。
</details>

### Q4. ワークスペース設定

Custom Spark Pool を作成するために必要な条件として正しいものはどれか？

A) ワークスペースの Member ロールがあれば作成可能
B) ワークスペースの Admin ロールが必要だが、他の前提条件はない
C) ワークスペースの Admin ロールが必要で、さらに容量管理者が「Customized workspace pools」を有効にしている必要がある
D) テナント管理者のみが作成可能

<details>
<summary>解答</summary>

**C) ワークスペースの Admin ロールが必要で、さらに容量管理者が「Customized workspace pools」を有効にしている必要がある**

Custom Pool の作成は2つの前提条件がある: ワークスペース Admin ロール + 容量管理者による「Customized workspace pools」の有効化。どちらか一方だけでは不足。
</details>

### Q5. データベースプロジェクト

データベースプロジェクトの主な目的として正しいものはどれか？

A) Warehouse のデータを環境間で移行する
B) Warehouse のスキーマ定義（テーブル、ビュー、SP）をバージョン管理し、環境間で一貫してデプロイする
C) Lakehouse の Delta テーブルスキーマを管理する
D) Power BI セマンティックモデルの定義を管理する

<details>
<summary>解答</summary>

**B) Warehouse のスキーマ定義（テーブル、ビュー、SP）をバージョン管理し、環境間で一貫してデプロイする**

データベースプロジェクトは Warehouse のスキーマをコードとして管理する仕組み。データの移行ではなくスキーマ定義の管理が目的。A は不正解（データは対象外）。C は不正解（Lakehouse は対象外）。D は不正解（セマンティックモデルは別の管理方法）。
</details>

---

> **次章**: 第10章 監視とトラブルシューティング（Monitoring Hub、データ取り込み/変換の監視、アラート、エラー特定の横断チェックリスト）
