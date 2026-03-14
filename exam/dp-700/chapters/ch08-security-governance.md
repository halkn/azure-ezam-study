# 第8章 セキュリティとガバナンス

> 📘 対応 MS Learn モジュール: Secure data access in Microsoft Fabric
> 試験タグ: `[実装管理]`

---

## 8.1 ワークスペースロール

第0章で概要を示したワークスペースロールをここで詳細に整理する。

### 8.1.1 ロール別の権限マトリクス

| 機能 | Admin | Member | Contributor | Viewer |
|------|-------|--------|-------------|--------|
| ワークスペースの削除・設定変更 | ○ | × | × | × |
| Admin の追加 | ○ | × | × | × |
| Member の追加 | ○ | ○ | × | × |
| OneLake セキュリティロールの編集 | ○ | ○ | × | × |
| アイテムの作成・編集 | ○ | ○ | ○ | × |
| アイテムの削除 | ○ | ○ | × | × |
| アイテムの共有（Reshare） | ○ | ○ | × | × |
| OneLake データの読み書き | ○ | ○ | ○ | **ロール依存** |
| SQL 分析エンドポイントへのクエリ | ○ | ○ | ○ | **権限依存** |
| Power BI レポートの閲覧 | ○ | ○ | ○ | ○ |

### 8.1.2 ロール割り当てのベストプラクティス

```
推奨: セキュリティグループ経由でロールを付与

  Entra ID セキュリティグループ           ワークスペースロール
  ──────────────────────              ──────────────
  sg-sales-admins         ────→       Admin
  sg-sales-engineers      ────→       Contributor
  sg-sales-analysts       ────→       Viewer
```

- **個人ユーザーへの直接割り当てを避ける**: グループベースで管理することで、異動・退職時のメンテナンスが容易
- **最小権限の原則**: 閲覧のみのユーザーには Viewer を付与し、OneLake セキュリティロールでデータ範囲を制限
- **サービスプリンシパル**: 自動化パイプラインには SPN をロールに割り当て（人のアカウントを使わない）

### 8.1.3 Viewer ロールとデータアクセス

Viewer は**コントロールプレーン上は最小権限**だが、データアクセスは別途構成が必要。

```
Viewer のデータアクセス経路:

1. OneLake 経由（Spark / OneLake API）
   └── ReadAll 権限 + OneLake セキュリティロール（DefaultReader または カスタムロール）

2. SQL 分析エンドポイント経由（T-SQL クエリ）
   └── ReadData 権限 + SQL ポリシー（GRANT/DENY、RLS、CLS）

3. Power BI 経由（レポート閲覧）
   └── Read 権限 → セマンティックモデルの Build/Read 権限
```

**試験ポイント**: Viewer に ReadAll と ReadData の両方を付与すると、OneLake 経由と SQL 経由で見えるデータが異なる可能性がある（SQL 側に RLS があるが OneLake 側にない場合など）。一貫したアクセス制御のため、どちらか一方のパスのみを付与することが推奨される。

---

## 8.2 アイテムレベルのアクセス制御

ワークスペースロールとは別に、個別アイテムに対して権限を付与できる。アイテムの「共有（Share）」機能で実現。

### 8.2.1 アイテム権限の種類

| 権限 | 説明 |
|------|------|
| **Read** | アイテムの存在を認識し、メタデータを読み取れる |
| **ReadAll** | OneLake 経由でアイテムのデータを読み取れる（Spark, OneLake API） |
| **ReadData** | SQL 分析エンドポイント経由でクエリを実行できる |
| **Build** | セマンティックモデルに基づいてレポートを構築できる |
| **Write** | アイテムのデータを書き込める |
| **Reshare** | 他のユーザーにアイテムを共有できる |

### 8.2.2 共有の方法

```
方法1: 共有リンク
  アイテム → Share → リンクを生成
  ├── 組織内の全員（テナント全体）
  ├── 既存のアクセス権を持つユーザー
  └── 特定のユーザー/グループ

方法2: 直接権限付与
  アイテム → Manage permissions → Grant access
  └── ユーザー/グループを指定 → 権限を選択

方法3: ワークスペースロール経由
  ワークスペースロール（Admin/Member/Contributor/Viewer）から継承
```

### 8.2.3 アイテム権限とワークスペースロールの関係

ワークスペースロールは**ベースライン**。アイテム権限はそれに**追加**。

```
例: ユーザー A
  ワークスペースロール: Viewer（ワークスペース内の全アイテムを認識可能）
  アイテム権限:
    Lakehouse_Sales → ReadAll（OneLake 経由でデータ読み取り可能）
    Warehouse_DWH → ReadData（SQL 経由でクエリ可能）
    Lakehouse_HR → 追加権限なし（メタデータのみ。データ読み取り不可）
```

---

## 8.3 行・列・オブジェクト・フォルダー/ファイルレベルのアクセス制御

各アイテムは固有のセキュリティモデルを持つ。以下に横断的に整理する。

### 8.3.1 セキュリティ粒度の比較

| セキュリティ粒度 | Lakehouse (OneLake) | Lakehouse (SQL EP) | Warehouse | Eventhouse (KQL DB) |
|---------------|--------------------|--------------------|-----------|-------------------|
| テーブル/フォルダレベル | ○ OneLake ロール | ○ GRANT/DENY | ○ GRANT/DENY | ○ テーブルレベルロール |
| 行レベル (RLS) | ○ (プレビュー) | ○ SQL ポリシー | **○** SECURITY POLICY | ○ restrict_access |
| 列レベル (CLS) | ○ (プレビュー) | ○ GRANT/DENY on columns | **○** GRANT/DENY | △ |
| 動的データマスク (DDM) | × | ○ | **○** MASKED WITH | × |
| オブジェクトレベル | ○ OneLake ロール | ○ GRANT/DENY on objects | **○** GRANT/DENY | ○ |
| フォルダ/ファイルレベル | **○** OneLake ロール | × | × | × |

### 8.3.2 各アイテムのセキュリティ実装まとめ

**Lakehouse（OneLake セキュリティ）**: → 第1章 1.3 参照

- フォルダ/テーブル単位の RBAC ロール
- DefaultReader の管理
- ReadWrite 権限（プレビュー）
- RLS / CLS（プレビュー）

**Warehouse**: → 第6章 6.6 参照

```sql
-- RLS の実装
CREATE FUNCTION dbo.fn_rls(@region NVARCHAR(50))
RETURNS TABLE WITH SCHEMABINDING
AS RETURN SELECT 1 AS result WHERE @region = USER_NAME();

CREATE SECURITY POLICY dbo.RegionPolicy
ADD FILTER PREDICATE dbo.fn_rls(region) ON dbo.fact_orders
WITH (STATE = ON);

-- CLS の実装
GRANT SELECT ON dbo.dim_customer (customer_id, customer_name) TO [analyst@contoso.com];
DENY SELECT ON dbo.dim_customer (ssn, phone) TO [analyst@contoso.com];

-- DDM の実装
ALTER TABLE dbo.employees ALTER COLUMN email ADD MASKED WITH (FUNCTION = 'email()');
GRANT UNMASK ON dbo.employees TO [hr@contoso.com];
```

**Eventhouse (KQL DB)**:

```kql
// KQL DB での行レベルセキュリティ
// restrict_access ポリシーでテーブルアクセスを制限
.alter table SensorData policy restricted_view_access true

// 行レベルのフィルタ関数
.create-or-alter function RegionFilter() {
    SensorData
    | where Region == current_principal_details().UserPrincipalName
}
```

---

## 8.4 秘密度ラベルの適用

### 8.4.1 秘密度ラベルとは

Microsoft Purview Information Protection の秘密度ラベルを Fabric アイテムに適用して、データの分類と保護を行う仕組み。

### 8.4.2 ラベルの種類（典型的な分類）

| ラベル | 説明 | 保護レベル |
|-------|------|-----------|
| Public | 公開情報 | なし |
| General | 社内一般情報 | 低 |
| Confidential | 機密情報 | 中（内部限定、監査ログ有効） |
| Highly Confidential | 高度機密 | 高（暗号化、外部共有禁止、アクセス制限） |

### 8.4.3 ラベルの適用方法

| 方法 | 説明 |
|------|------|
| **手動適用** | ユーザーがアイテムの設定から手動でラベルを選択 |
| **デフォルトラベル** | テナント設定で新規アイテムに自動適用されるデフォルトラベルを定義 |
| **ラベル継承** | ソースデータのラベルが下流アイテムに自動的に継承。例: Lakehouse に「Confidential」→ そこから作成した Power BI レポートにも「Confidential」が継承 |
| **プログラム的適用** | REST API / SDK でラベルを一括適用 |
| **自動ラベル付け** | Purview の ML ベースの自動分類（クレジットカード番号, SSN 等を検出） |

### 8.4.4 ラベルの継承と伝播

```
Bronze Lakehouse [Confidential]
  └── Notebook で変換 → Silver Lakehouse [Confidential] ← 継承
        └── Power BI レポート [Confidential] ← 継承
              └── Excel エクスポート [Confidential] ← エクスポート時にも保護が維持
```

**試験ポイント**: 秘密度ラベルは**ダウンストリームに継承**される。Bronze に付けたラベルが Silver → Gold → レポートへと伝播する。ラベルの変更は元のアイテムで行い、継承先に自動反映される。

### 8.4.5 保護ポリシー（Protection Policies）

秘密度ラベルに紐づく保護ポリシーを定義し、特定ラベルが付いたアイテムへのアクセスを自動的に制限。

```
例: 「Highly Confidential」ラベル付きアイテム
  → 保護ポリシー: sg-data-governance グループのメンバーのみアクセス可能
  → 対象: Lakehouse, Warehouse, KQL DB, セマンティックモデル
  → 効果: ラベルを付けるだけで自動的にアクセス制限が適用
```

---

## 8.5 項目の承認

### 8.5.1 承認ステータスの種類

| ステータス | 設定者 | 目的 | OneLake カタログでの表示 |
|-----------|-------|------|---------------------|
| **なし**（デフォルト） | - | 未承認 | なし |
| **Promoted（プロモート）** | ワークスペースの Member 以上 | 品質が確認されたアイテムの推奨 | 「Promoted」バッジ |
| **Certified（認定）** | テナント/ドメイン管理者が**指定した認定者** | 組織の品質基準を満たすアイテムの公式認定 | 「Certified」バッジ |
| **Master Data** | 管理者指定の認定者 | マスターデータとしての認定 | 「Master Data」バッジ |

### 8.5.2 Promoted vs Certified

```
Promoted:
  誰が設定できるか: ワークスペースの Member / Contributor / Admin
  設定場所: アイテムの Settings → Endorsement → Promoted
  意味: 「このアイテムは使用に値する品質です」（個人の推奨）

Certified:
  誰が設定できるか: テナント管理者またはドメイン管理者が指定した認定者のみ
  設定場所: アイテムの Settings → Endorsement → Certified
  意味: 「このアイテムは組織の品質基準を満たしています」（公式認定）
  前提: テナント設定 → Endorsement settings で認定を有効化し、認定者を指定
```

### 8.5.3 承認の効果

- OneLake カタログ / ワークスペースブラウザで承認済みアイテムに**バッジが表示**
- **検索結果で優先表示**される
- ユーザーが信頼できるデータソースを識別しやすくなる
- Purview Hub で承認カバレッジを監視可能

**試験ポイント**: Promoted は Member 以上が設定可能だが、Certified は**管理者が指定した認定者のみ**。Certified の設定権限は自動的には付与されない。

---

## 8.6 ワークスペースログの実装と活用 `[実装管理][監視最適化]`

### 8.6.1 監査ログの種類

| ログソース | 内容 | 格納先 |
|----------|------|-------|
| **Microsoft Purview Audit** | テナント全体のユーザーアクティビティ（Lakehouse アクセス, Power BI 操作, ログイン等） | Purview コンプライアンスポータル |
| **OneLake 診断ログ** | OneLake データへのアクセスログ（誰が、いつ、何に、どのようにアクセスしたか） | ワークスペース内の Lakehouse（JSON 形式） |
| **Warehouse SQL 監査ログ** | T-SQL クエリの実行ログ | 構成により OneLake に格納 |

### 8.6.2 OneLake 診断ログ

ワークスペース設定から有効化可能。

```
有効化:
  ワークスペース設定 → OneLake → Diagnostics → Enable

格納先:
  ワークスペース内の Lakehouse に JSON ファイルとして保存

保持期間:
  最大 9 年間で構成可能

分析方法:
  ├── Spark ノートブック（JSON → DataFrame → 集計）
  ├── SQL 分析エンドポイント（テーブルとしてクエリ）
  ├── Eventhouse（KQL でリアルタイム分析）
  └── Power BI（ダッシュボード化）
```

```python
# Spark でログを分析する例
df_logs = spark.read.json("Files/diagnostics/onelake/")
df_logs.filter(col("Operation") == "ReadFile") \
    .groupBy("UserPrincipalName", "ItemName") \
    .agg(count("*").alias("AccessCount")) \
    .orderBy(col("AccessCount").desc()) \
    .show(20)
```

### 8.6.3 Purview Audit

Microsoft Purview Audit はテナントレベルで全 Fabric アクティビティを記録する。

記録されるアクティビティの例:
- Lakehouse へのデータ書き込み
- Power BI レポートの閲覧
- パイプラインの実行
- Spark ジョブの実行
- 秘密度ラベルの変更
- アイテムの共有
- ログイン / ログアウト

```
アクセス方法:
  Microsoft Purview コンプライアンスポータル → Audit → 検索
  ├── 日時範囲の指定
  ├── アクティビティ種別のフィルタ
  ├── ユーザーのフィルタ
  └── 結果のエクスポート（CSV）
```

### 8.6.4 Warehouse SQL 監査ログ

```
有効化:
  Warehouse → Settings → SQL audit logs
  → Enable → 保持期間を指定（最大 9 年）
  → ログは OneLake に格納

分析:
  SQL クエリ実行ログを Power BI でダッシュボード化
  → 誰がどのクエリをいつ実行したかを可視化
```

---

## 8.7 セキュリティ設計パターン

### 8.7.1 メダリオンアーキテクチャにおけるセキュリティ

```
Bronze ワークスペース:
  ロール: データエンジニア → Contributor
         他全員 → アクセスなし（生データは非公開）

Silver ワークスペース:
  ロール: データエンジニア → Contributor
         データアナリスト → Viewer（OneLake セキュリティで制限）

Gold ワークスペース:
  ロール: データエンジニア → Contributor
         BI チーム → Viewer
         ビジネスユーザー → Viewer（Power BI レポート閲覧のみ）
  セキュリティ:
    Warehouse: RLS（部門別フィルタ）+ CLS（PII 列の制限）+ DDM（メールのマスク）
    Lakehouse: OneLake セキュリティロール（テーブル単位のアクセス制御）
```

### 8.7.2 セキュリティチェックリスト

| チェック項目 | 実装 |
|------------|------|
| ワークスペースロールは最小権限か | Viewer をデフォルトに。必要な人のみ上位ロール |
| OneLake セキュリティロールは適切か | DefaultReader の管理。カスタムロールの作成 |
| Warehouse の RLS / CLS / DDM は設定済みか | SECURITY POLICY, GRANT/DENY, MASKED WITH |
| 秘密度ラベルは適用済みか | PII を含むアイテムに Confidential 以上 |
| 項目の承認は行われているか | Gold レイヤーのアイテムを Certified に |
| 監査ログは有効化されているか | OneLake 診断、Purview Audit、Warehouse SQL 監査 |
| サービスプリンシパルは適切に管理されているか | 証明書の有効期限監視、最小権限の付与 |
| 外部共有は制限されているか | テナント設定で外部共有ポリシーを確認 |

---

## 8.8 確認問題

### Q1. ワークスペースロール

ワークスペースの Contributor ロールについて正しいものはどれか？

A) OneLake セキュリティロールの影響を受け、アクセスできるデータが制限される
B) ワークスペース内のアイテムを削除できる
C) ワークスペース内のアイテムを作成・編集でき、OneLake データの読み書きが可能
D) 他のユーザーにアイテムを共有（Reshare）できる

<details>
<summary>解答</summary>

**C) ワークスペース内のアイテムを作成・編集でき、OneLake データの読み書きが可能**

- A は不正解: Admin / Member / Contributor は OneLake セキュリティロールの影響を**受けない**（全データ読み書き可）
- B は不正解: アイテム削除は Admin / Member のみ。Contributor は不可
- D は不正解: Reshare は Admin / Member のみ
</details>

### Q2. 秘密度ラベル

秘密度ラベルについて正しいものはどれか？

A) ラベルは手動でのみ適用でき、自動適用はサポートされない
B) Bronze Lakehouse に「Confidential」ラベルを付けると、そこから作成した Silver テーブルにも自動継承される
C) ラベルはワークスペース単位でのみ適用可能で、個別アイテムには適用できない
D) Promoted ステータスと秘密度ラベルは同じ機能である

<details>
<summary>解答</summary>

**B) Bronze Lakehouse に「Confidential」ラベルを付けると、そこから作成した Silver テーブルにも自動継承される**

秘密度ラベルはダウンストリームに継承される。A は不正解（自動ラベル付けがサポートされる）。C は不正解（アイテム単位で適用可能）。D は不正解（承認と秘密度ラベルは異なる機能）。
</details>

### Q3. Certified vs Promoted

以下のうち、アイテムの「Certified（認定）」ステータスについて正しいものはどれか？

A) ワークスペースの Contributor が設定できる
B) ワークスペースの Member 以上であれば誰でも設定できる
C) テナント管理者またはドメイン管理者が指定した認定者のみが設定できる
D) アイテムの作成者が自動的に設定される

<details>
<summary>解答</summary>

**C) テナント管理者またはドメイン管理者が指定した認定者のみが設定できる**

Certified は組織の公式認定であり、誰でも設定できるわけではない。テナント設定で認定を有効化し、認定者を明示的に指定する必要がある。Promoted は Member 以上が設定可能（A, B は Promoted の説明）。
</details>

### Q4. セキュリティ粒度

Warehouse テーブルの特定列に含まれるメールアドレスを、権限のないユーザーには `aXXX@XXXX.com` の形式で表示したい。最適なセキュリティ機能はどれか？

A) 行レベルセキュリティ（RLS）
B) 列レベルセキュリティ（CLS）
C) 動的データマスク（DDM）
D) OneLake セキュリティロール

<details>
<summary>解答</summary>

**C) 動的データマスク（DDM）**

DDM の `email()` マスク関数がまさにこのユースケースに対応。RLS は行の制限、CLS は列の完全非表示、OneLake ロールはフォルダ/テーブル単位の制限であり、「部分的に表示する」要件には DDM が最適。
</details>

### Q5. 監査ログ

OneLake 上のデータに「誰がいつアクセスしたか」を追跡する最適な方法はどれか？

A) Monitoring Hub でパイプライン実行を確認する
B) ワークスペース設定で OneLake 診断ログを有効化し、ログを分析する
C) Warehouse の DMV で実行クエリを確認する
D) Power BI の使用状況メトリクスを確認する

<details>
<summary>解答</summary>

**B) ワークスペース設定で OneLake 診断ログを有効化し、ログを分析する**

OneLake 診断ログは「誰が、いつ、何に、どのようにアクセスしたか」をファイルレベルで記録する。A はパイプライン実行の監視、C は Warehouse の T-SQL クエリのみ、D は Power BI レポートの閲覧状況のみ。OneLake 全体のデータアクセスを追跡するには OneLake 診断が最適。
</details>

---

> **次章**: 第9章 ライフサイクル管理（CI/CD）（Git 連携、デプロイパイプライン、データベースプロジェクト、ワークスペース設定）
