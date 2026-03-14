# 第1章 OneLake とショートカット

> 📘 対応 MS Learn モジュール: 各モジュールに分散（単独モジュールなし）
> ⚠️ 公式ドキュメント補完必須
> 試験タグ: `[実装管理]` `[取込変換]` `[監視最適化]`

---

## 1.1 OneLake のアーキテクチャ（詳細）

第0章で OneLake の概要を扱った。本章ではショートカット・セキュリティ・ワークスペース設定など、試験で深く問われる領域を掘り下げる。

### 1.1.1 OneLake の名前空間

OneLake は単一テナントに1つだけ存在する論理データレイクで、ADLS Gen2 の上に構築されている。物理的にはテナント内の全ワークスペースにまたがる1つの巨大なファイルシステムであり、各ワークスペースが ADLS のコンテナに、各アイテムがフォルダに相当する。

```
OneLake の名前空間（ADLS Gen2 互換ビュー）

https://onelake.dfs.fabric.microsoft.com/
  └── <workspace-name>/          ← ADLS のコンテナに相当
        └── <item-name>/         ← Lakehouse / Warehouse / KQL DB 等
              ├── Tables/        ← マネージド Delta テーブル
              │     ├── dim_customer/
              │     │     ├── _delta_log/
              │     │     └── part-00000-xxx.parquet
              │     └── fact_orders/
              └── Files/         ← 非構造化ファイル
                    ├── raw/
                    └── staging/
```

重要な制約:
- ワークスペース名とアイテム名に使える文字には制限がある（`%` や `+` はショートカットパスに使用不可）
- 同一テナント内のすべてのデータは OneLake 上に格納されるが、容量のリージョンが異なる場合はクロスリージョンアクセスになりレイテンシが増加する

### 1.1.2 OneLake のデータフォーマット

OneLake はオープンフォーマットを標準とする。

| アイテム | ストレージフォーマット | メタデータ |
|---------|-------------------|---------| 
| Lakehouse (Tables/) | Delta Parquet | _delta_log（JSON + Parquet チェックポイント） |
| Lakehouse (Files/) | 任意のファイル | なし（生ファイル） |
| Warehouse | Delta Parquet | 内部管理 |
| Eventhouse (KQL DB) | 独自列指向フォーマット | KQL エンジンが管理 |

**Delta Parquet が標準**であるため、Lakehouse に Spark で書き込んだテーブルを Warehouse の T-SQL（クロスデータベースクエリ）から読み取ることが可能。これは Snowflake のデータシェアリングに近い概念だが、同一 OneLake 上の物理データを共有するためコピーもシェアオブジェクトも不要。

### 1.1.3 OneLake API アクセス

OneLake は ADLS Gen2 互換の REST API をサポートするため、Fabric 外部のツールからもアクセスできる。

```python
# Azure Identity + ADLS Gen2 SDK で OneLake にアクセスする例
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient

credential = DefaultAzureCredential()
service_client = DataLakeServiceClient(
    account_url="https://onelake.dfs.fabric.microsoft.com",
    credential=credential
)

# ワークスペース = ファイルシステム（コンテナ）
file_system_client = service_client.get_file_system_client("my-workspace")

# アイテム内のパスを指定してファイルを読み取る
file_client = file_system_client.get_file_client("my-lakehouse/Files/raw/data.csv")
download = file_client.download_file()
content = download.readall()
```

```bash
# Azure CLI で OneLake 上のファイルを一覧表示
az storage fs file list \
  --account-name onelake \
  --file-system "my-workspace" \
  --path "my-lakehouse/Tables/" \
  --auth-mode login \
  --account-url "https://onelake.dfs.fabric.microsoft.com"
```

**試験ポイント**: OneLake の API アクセスには Microsoft Entra ID（旧 Azure AD）認証が必要。ストレージアカウントキーや SAS トークンによるアクセスは OneLake 本体ではサポートされない（外部ショートカット先の ADLS Gen2 では使用可能）。

---

## 1.2 ショートカットの種類と作成

ショートカットは OneLake 内に作成する「参照ポインタ」であり、データをコピーせずに外部または内部のデータにアクセスできる。Snowflake の外部テーブル（External Table）に概念的に近いが、ショートカットはデータの実体を持たず、読み取り時にソースから直接フェッチする点が異なる。

### 1.2.1 内部ショートカット（OneLake 内）

同一テナント内の別ワークスペース・別アイテムのデータを参照する。

```
作成元                          ショートカット先（ターゲット）
────────                       ──────────────────
Workspace-A / Lakehouse-Sales   → Workspace-B / Lakehouse-Marketing / Tables/dim_product
  └── Tables/
        └── dim_product (ショートカット)
```

特徴:
- **認証**: 呼び出しユーザーの ID（Entra ID）が使われる。ユーザーはターゲットに対する読み取り権限（ReadAll またはデータアクセスロール）が必要
- **データ移動なし**: ゼロコピー。物理データはターゲット側にのみ存在
- **OneLake セキュリティ**: 内部ショートカットに対して独自の OneLake セキュリティロールは定義できない。セキュリティはターゲット側のロールで制御される
- **リネージ**: ワークスペースのリネージビューでショートカットの関係を可視化可能（同一ワークスペース内のみ）

```python
# PySpark でのショートカット経由の読み取り（通常のテーブルと同じ構文）
df = spark.read.format("delta").load("Tables/dim_product")  # ショートカットか実テーブルかを意識しない

# 別ワークスペースの Lakehouse をフルパスで参照する場合
df = spark.read.format("delta").load(
    "abfss://workspace-b@onelake.dfs.fabric.microsoft.com/Lakehouse-Marketing/Tables/dim_product"
)
```

### 1.2.2 外部ショートカット（OneLake 外）

OneLake の外にあるデータソースを参照する。

| ソース | 認証方式 | キャッシュ対応 |
|-------|---------|-------------|
| ADLS Gen2 | 組織アカウント / サービスプリンシパル / SAS トークン / ワークスペース ID | × |
| Amazon S3 | アクセスキー / IAM ロール | ○ |
| S3 互換ストレージ | アクセスキー | ○ |
| Google Cloud Storage (GCS) | HMAC キー | ○ |
| Dataverse | 組織アカウント | × |
| オンプレミス（OPDG 経由） | ゲートウェイ認証 | ○ |

**認証モデル（委任認証）**:
外部ショートカットは**委任認証（Delegated Authorization）**を使用する。ショートカット作成者が指定した資格情報を使ってすべてのユーザーのアクセスが認可される。つまり、Lakehouse にアクセスできるユーザーは全員、ショートカット作成者の資格情報を通じてソースデータを読み取れる。

```
ショートカットの認証フロー（外部ショートカット）:

User A (Viewer) → Lakehouse の Tables/external_data (ショートカット)
  → OneLake が委任認証で接続（ショートカット作成者 B の資格情報）
    → ADLS Gen2 / S3 / GCS からデータをフェッチ
```

**試験ポイント**:
- 内部ショートカットは「呼び出しユーザーの ID」、外部ショートカットは「作成者の資格情報」で認証
- ADLS ショートカットは DFS エンドポイント（`https://accountname.dfs.core.windows.net/`）を使用する必要がある
- ADLS ショートカットのクロステナントアクセスにはサービスプリンシパルまたは SAS トークンが必要（組織アカウントは不可）

### 1.2.3 ショートカットの作成場所

ショートカットを作成できるアイテムと配置場所は決まっている。

| アイテム | Tables/ での作成 | Files/ での作成 |
|---------|-----------------|----------------|
| Lakehouse | ○（トップレベルのみ） | ○（任意の階層） |
| KQL DB | ○ | - |
| Warehouse | ×（ショートカット非サポート） | - |

**重要な制約**:
- Lakehouse の Tables/ 配下では**トップレベルにのみ**ショートカットを作成可能（サブディレクトリ不可）
- Tables/ のショートカット先が Delta フォーマットの場合、自動的にテーブルとして認識され SQL 分析エンドポイントからクエリ可能
- Files/ のショートカットはテーブルとして自動登録されないが、Spark で直接読み取り可能
- **Warehouse はショートカットをサポートしない**（試験頻出）

### 1.2.4 ショートカットの制限

| 制限 | 値 |
|-----|---|
| 1アイテムあたりの最大ショートカット数 | 100,000 |
| 1パスあたりの最大ショートカット数 | 10 |
| ショートカットからショートカットへの直接リンク最大数 | 5（ショートカットチェーン） |
| パス内の禁止文字 | `%`, `+` |
| ADLS ショートカット内のサブショートカット | 作成不可 |

### 1.2.5 ショートカットの削除と影響

- ショートカットを削除しても**ターゲットデータは影響を受けない**（参照の削除のみ）
- ショートカット内のファイル/フォルダを削除すると、ターゲット側で書き込み権限がある場合は**ターゲット側のデータも削除される**

```
例:
MyLakehouse/Files/MyShortcut → ADLS Gen2 の Foo/Bar を参照

- MyShortcut を削除 → ADLS 側の Foo/Bar は無影響
- MyShortcut/Foo/Bar を削除 → ADLS 側の Bar ディレクトリも削除（書き込み権限がある場合）
```

---

## 1.3 OneLake セキュリティ `[実装管理]`

OneLake セキュリティは、データプレーン（誰がどのデータを見られるか）を制御する仕組みである。ワークスペースロール（コントロールプレーン）と組み合わせて多層的なアクセス制御を実現する。

### 1.3.1 セキュリティの階層

```
コントロールプレーン（何ができるか）           データプレーン（何が見えるか）
──────────────────────                    ────────────────────
ワークスペースロール                         OneLake セキュリティロール
  ├── Admin    ─→ 全データ読み書き可          ├── DefaultReader（自動作成）
  ├── Member   ─→ 全データ読み書き可          ├── カスタムロール A（Tables/dim_customer のみ）
  ├── Contributor ─→ 全データ読み書き可       └── カスタムロール B（Tables/fact_orders のみ）
  └── Viewer   ─→ OneLake セキュリティに依存

アイテムレベル権限
  ├── Read       ─→ アイテムの存在を認識
  ├── ReadAll    ─→ OneLake 経由のデータ読み取り（※DefaultReader に依存）
  ├── ReadData   ─→ SQL 分析エンドポイント経由のデータ読み取り
  ├── Build      ─→ セマンティックモデルの構築
  └── Write      ─→ データの書き込み
```

**重要**: Admin / Member / Contributor は OneLake セキュリティロールの影響を**受けない**。これらのロールはコントロールプレーンの Write 権限を持ち、全データに読み書きできる。OneLake セキュリティロールが効くのは **Viewer ロールのユーザー**（または Read/ReadAll のみ持つユーザー）に対してのみ。

### 1.3.2 OneLake セキュリティロールの構成要素

OneLake セキュリティロールは以下の4つの要素で構成される。

| 要素 | 説明 |
|------|------|
| **Type** | GRANT のみ（DENY は未サポート） |
| **Permission** | Read（読み取り）, ReadWrite（読み書き、プレビュー） |
| **Scope** | アクセス対象のテーブル / フォルダ / スキーマ |
| **Members** | Microsoft Entra ID のユーザー / グループ / サービスプリンシパル |

### 1.3.3 DefaultReader ロール

OneLake セキュリティを有効化すると、自動的に **DefaultReader** ロールが作成される。

- ReadAll 権限を持つすべてのユーザーを「仮想メンバー」として含む
- 全テーブル・全フォルダへの読み取りアクセスを付与
- **DefaultReader を削除または編集すると**、ReadAll を持つユーザーでもカスタムロールで明示的に許可されたデータにしかアクセスできなくなる

```
DefaultReader の動作:

OneLake セキュリティ有効化前:
  ReadAll を持つユーザー → 全データ読み取り可

OneLake セキュリティ有効化直後:
  ReadAll を持つユーザー → DefaultReader により全データ読み取り可（変化なし）

DefaultReader を削除した後:
  ReadAll を持つユーザー → カスタムロールに含まれていなければデータ見えない
```

**試験ポイント**: DefaultReader をそのまま残しつつカスタムロールにも同一ユーザーを追加すると、DefaultReader の全データアクセスが優先され、カスタムロールの制限が実質無効になる。カスタムロールで制限する場合は、対象ユーザーを DefaultReader から除外する必要がある。

### 1.3.4 カスタムロールの作成

```
例: 営業部門には dim_customer と fact_orders のみ、マーケ部門には dim_product と fact_campaigns のみ見せる

ロール: SalesDataReader
  Permission: Read
  Scope: Tables/dim_customer, Tables/fact_orders
  Members: sales-team@contoso.com (Entra ID グループ)

ロール: MarketingDataReader
  Permission: Read
  Scope: Tables/dim_product, Tables/fact_campaigns
  Members: marketing-team@contoso.com (Entra ID グループ)

→ DefaultReader は削除（または全メンバーを除外）
```

### 1.3.5 ReadWrite アクセス（プレビュー）

OneLake セキュリティの ReadWrite 権限を使うと、Viewer ロールのユーザーでも指定したテーブル/フォルダにデータを書き込める。

- 書き込みは Spark ノートブック / OneLake File Explorer / OneLake API 経由で実行可能
- Lakehouse UX（GUI）からの書き込みは Viewer には不可
- Contributor 以上のロールを付与せずに、最小権限の原則で書き込みアクセスを制御可能

```
ロール: ETLWriter
  Permission: ReadWrite
  Scope: Tables/bronze_events
  Members: etl-service-principal@contoso.com
```

### 1.3.6 行レベル・列レベルセキュリティ（OneLake セキュリティ内）

OneLake セキュリティロールでは、フォルダ/テーブルレベルの制御に加え、行レベルセキュリティ（RLS）と列レベルセキュリティ（CLS）も構成可能（プレビュー）。

- RLS/CLS が設定されたテーブルに対して、Fabric エンジン（Spark, SQL 等）はフィルタリングを適用する
- **OneLake API（ストレージレベル）での直接アクセスでは RLS/CLS のフィルタリングが適用されないため、アクセスがブロックされる**
- つまり、RLS/CLS が有効なテーブルでは、ユーザーは Fabric エンジン経由でのみデータにアクセスでき、生ファイルの直接ダウンロードは不可

### 1.3.7 ショートカットとセキュリティの関係

| ショートカット種別 | セキュリティの適用場所 |
|-----------------|-------------------|
| 内部ショートカット | **ターゲット側のアイテム**で定義されたロールが適用。ショートカット側では定義不可 |
| 外部ショートカット（ADLS, S3 等） | ショートカット側の OneLake セキュリティロールで制御可能。委任認証 + OneLake ロールの二重チェック |

```
外部ショートカットのセキュリティ評価フロー:

User → Lakehouse / Tables / s3_data (S3 ショートカット)
  1. OneLake セキュリティロールにユーザーが含まれるか？ → No → アクセス拒否
  2. S3 接続の委任認証（作成者の資格情報）は有効か？ → No → アクセス拒否
  3. 両方 Yes → データ返却
```

### 1.3.8 SQL 分析エンドポイントとの関係

OneLake セキュリティと SQL 分析エンドポイントのセキュリティは別系統で、ユーザーがどのエンジンでアクセスするかによって見えるデータが異なる可能性がある。

| アクセス経路 | 適用されるセキュリティ |
|------------|-------------------|
| Spark ノートブック → OneLake | OneLake セキュリティロール |
| SQL 分析エンドポイント → クエリ | SQL ポリシー（GRANT/DENY、RLS、CLS） |
| OneLake API → 直接ファイルアクセス | OneLake セキュリティロール |
| Power BI → Direct Lake | OneLake セキュリティロール（RLS/CLS があると DirectQuery フォールバック） |

**試験ポイント**: ReadAll と ReadData を両方付与すると、OneLake 経由と SQL 経由で異なるデータが見える可能性がある。最小権限の原則に従い、どちらか一方のパスのみを付与することが推奨される。

---

## 1.4 OneLake ワークスペース設定 `[実装管理]`

### 1.4.1 OneLake 設定の構成項目

ワークスペース設定の「OneLake」タブで構成できる項目:

| 設定 | 説明 |
|------|------|
| ショートカットキャッシュ | 外部ショートカットのデータをローカルキャッシュに保持 |
| キャッシュ保持期間 | 1〜28日（デフォルト: 7日） |
| キャッシュのリセット | キャッシュの手動クリア |

### 1.4.2 ショートカットキャッシュ

外部ショートカット（S3, S3 互換, GCS, OPDG）に対して、読み取ったデータを OneLake 上にキャッシュする機能。

```
キャッシュの動作:

1. User → ショートカット経由でデータ読み取り
2. OneLake がキャッシュを確認
   ├── キャッシュあり → キャッシュからデータ返却（高速）
   │     └── ただしリモートソースに新しいバージョンがあればリフレッシュ
   └── キャッシュなし → ソースからフェッチ → キャッシュに保存 → データ返却
3. 保持期間内にアクセスがない → キャッシュからパージ
   └── 最後のアクセスからタイマーリセット
```

**キャッシュの条件と制約**:

| 条件 | 詳細 |
|------|------|
| 対応ソース | S3, S3 互換, GCS, OPDG ショートカット |
| 非対応ソース | **ADLS Gen2**（キャッシュ不可。同一 Azure バックボーン内のためレイテンシが低い前提） |
| ファイルサイズ上限 | **1 GB 以下のファイルのみ**キャッシュされる |
| 保持期間 | 1〜28日。最後のアクセスでタイマーリセット |
| 鮮度チェック | リモートソースに新しいバージョンがあると自動リフレッシュ |

**キャッシュを有効にすべき場面**:
- 同じ外部ファイルを頻繁に読み取る
- エグレスコスト（S3/GCS からの転送料金）を削減したい
- データが比較的安定している（毎回最新でなくてよい）

**キャッシュを無効にすべき場面**:
- ファイルが 1 GB を超える（キャッシュされないため無意味）
- データが頻繁に変更され常に最新を参照する必要がある
- キャッシュのストレージコストが問題になる

### 1.4.3 OneLake 診断ログ

OneLake 診断（OneLake Diagnostics）はワークスペース設定から有効化でき、「誰が、いつ、何に、どのようにアクセスしたか」をログとして Lakehouse に格納する。

- ログは JSON 形式で OneLake に保存
- Spark / SQL / Eventhouse / Power BI 等で分析可能
- 保持期間は最大9年間で構成可能
- コンプライアンスや監査要件への対応に使用

```kql
// KQL でログを分析する例（Eventhouse に取り込んだ場合）
OneLakeDiagnostics
| where Timestamp > ago(7d)
| where Operation == "ReadFile"
| summarize AccessCount = count() by UserPrincipalName, ItemName
| order by AccessCount desc
| take 20
```

---

## 1.5 ショートカット vs ミラーリング vs コピー

試験では「適切なデータ取り込み方法を選択する」シナリオが頻出する。ショートカット・ミラーリング・コピー（Pipeline）の使い分けを整理する。

### 1.5.1 比較表

| 観点 | ショートカット | ミラーリング | コピー（Pipeline） |
|------|-------------|------------|-----------------|
| データの物理配置 | **ソース側のまま**（ゼロコピー） | **OneLake にレプリカ** | **OneLake にコピー** |
| レイテンシ | ソースの I/O + ネットワーク | OneLake ローカル（高速） | OneLake ローカル（高速） |
| 鮮度 | **リアルタイム**（常にソース最新） | **ほぼリアルタイム**（CDC による差分同期） | **パイプライン実行時点** |
| ストレージコスト | ソース側のみ（+ エグレス料金） | OneLake 側に追加容量必要 | OneLake 側に追加容量必要 |
| 変換 | 不可（参照のみ） | 不可（スキーマ変換は自動） | パイプライン内で変換可能 |
| 対応ソース | ADLS, S3, GCS, Dataverse, OneLake 内 | Azure SQL DB, Cosmos DB, Snowflake, PostgreSQL 等 | ほぼすべてのソース |
| セットアップ | 簡単（ポインタ作成のみ） | 中程度（CDC 設定） | 中〜高（パイプライン設計） |
| ユースケース | ルックアップテーブル参照、フェデレーテッドクエリ | ソース DB のリアルタイムレプリカ | ETL/ELT、大量一括ロード |

### 1.5.2 選択フローチャート

```
外部データを Fabric で使いたい
  │
  ├── ソースが RDBMS（SQL DB, Cosmos, Snowflake 等）？
  │     ├── ほぼリアルタイムで同期したい → ミラーリング
  │     └── 定期バッチで十分 → Pipeline でコピー（増分 or 全量）
  │
  ├── ソースがオブジェクトストレージ（ADLS, S3, GCS）？
  │     ├── データのコピーを避けたい → ショートカット
  │     ├── 変換が必要 → Pipeline でコピー + ノートブックで変換
  │     └── クエリ頻度が高い + エグレスコストが心配 → ショートカット（キャッシュ有効）
  │
  └── 同一テナント内の別ワークスペースのデータ？
        └── ショートカット（内部）が最適
```

---

## 1.6 ショートカットエラーの特定と解決 `[監視最適化]`

### 1.6.1 一般的なエラーパターン

| エラー | 原因 | 対処 |
|-------|------|------|
| 認証エラー | 外部ショートカットの資格情報が期限切れ / 無効化 | ショートカットを編集して接続を再設定。SAS トークンの有効期限を確認 |
| パス不正 | ターゲット側のフォルダ名変更 / 削除 | ショートカットの Manage shortcut から Target path を修正 |
| アクセス拒否（内部） | ターゲットの OneLake セキュリティロールからユーザーが除外 | ターゲット側のロールにユーザーを追加 |
| アクセス拒否（外部） | ADLS のファイアウォール / ネットワーク制限 | Trusted workspace access を構成、または VNET 設定を確認 |
| データが表示されない | Tables/ のショートカット先が Delta フォーマットでない | Files/ に作成するか、ソースを Delta に変換 |
| クロステナントエラー | ADLS ショートカットで組織アカウント認証を使用 | サービスプリンシパルまたは SAS トークンに変更 |
| ショートカットチェーン超過 | ショートカット → ショートカット → ... が5段を超えた | 中間ショートカットを削除して直接参照に変更 |

### 1.6.2 トラブルシューティング手順

```
1. Monitoring Hub でショートカットを含むアイテムの状態を確認
2. ショートカットを右クリック → Properties でショートカットの種類とターゲットパスを確認
3. 外部ショートカットの場合:
   a. 接続設定を確認（Manage shortcut → Target connection）
   b. ソース側のアクセス権限を確認（ADLS の Storage Blob Data Reader 等）
   c. ファイアウォール / Private Endpoint の設定を確認
4. 内部ショートカットの場合:
   a. ターゲットアイテムの存在を確認（削除されていないか）
   b. ターゲット側の OneLake セキュリティロールを確認
   c. ユーザーがターゲットに ReadAll 権限を持っているか確認
5. キャッシュ関連の問題:
   a. ワークスペース設定 → OneLake → キャッシュの状態を確認
   b. 必要に応じて「Reset cache」でキャッシュをクリア
```

---

## 1.7 確認問題

### Q1. ショートカットの認証

データエンジニアが Lakehouse に S3 ショートカットを作成した。その後、Viewer ロールの別のユーザーがショートカットのデータを読み取ろうとしている。どの認証情報が使用されるか？

A) 読み取りユーザー自身の Entra ID 資格情報
B) ショートカット作成者が指定した S3 接続の資格情報
C) ワークスペース管理者の資格情報
D) OneLake のマネージド ID

<details>
<summary>解答</summary>

**B) ショートカット作成者が指定した S3 接続の資格情報**

外部ショートカットは委任認証モデルを使用する。ショートカット作成時に指定された接続情報（この場合は S3 のアクセスキー等）がすべてのユーザーのアクセスに使用される。内部ショートカットの場合は A（呼び出しユーザーの ID）が使用される。
</details>

### Q2. OneLake セキュリティ

Lakehouse で OneLake セキュリティを有効化した。DefaultReader ロールを削除せずに、カスタムロール「SalesReader」を作成し、Tables/fact_orders へのアクセスのみを許可した。sales-user@contoso.com をこのカスタムロールに追加した。sales-user は ReadAll 権限も持っている。sales-user はどのテーブルにアクセスできるか？

A) fact_orders のみ
B) 全テーブル
C) アクセス不可
D) fact_orders と DefaultReader に含まれるテーブル

<details>
<summary>解答</summary>

**B) 全テーブル**

DefaultReader ロールは ReadAll 権限を持つ全ユーザーを仮想メンバーとして含む。sales-user は ReadAll を持っているため、DefaultReader により全テーブルにアクセス可能。カスタムロールの制限を有効にするには、DefaultReader を削除するか、sales-user を DefaultReader から除外する必要がある。
</details>

### Q3. ショートカットの作成場所

以下のうち、ショートカットの作成について**正しい**ものはどれか？

A) Warehouse の Tables/ にショートカットを作成できる
B) Lakehouse の Tables/ にはサブフォルダ内にショートカットを作成できる
C) KQL DB にショートカットを作成できる
D) Lakehouse の Files/ にはトップレベルにのみショートカットを作成できる

<details>
<summary>解答</summary>

**C) KQL DB にショートカットを作成できる**

- A は不正解: Warehouse はショートカットをサポートしない
- B は不正解: Tables/ のショートカットはトップレベルのみ（サブフォルダ不可）
- D は不正解: Files/ では任意の階層にショートカットを作成可能
</details>

### Q4. キャッシュの対象

ワークスペース設定でショートカットキャッシュを有効化した。以下のうち、キャッシュが適用されるのはどれか？

A) ADLS Gen2 ショートカット経由の Parquet ファイル（500 MB）
B) S3 ショートカット経由の CSV ファイル（800 MB）
C) S3 ショートカット経由の Parquet ファイル（2 GB）
D) 内部 OneLake ショートカット経由の Delta テーブル

<details>
<summary>解答</summary>

**B) S3 ショートカット経由の CSV ファイル（800 MB）**

- A は不正解: ADLS Gen2 ショートカットはキャッシュ非対応
- B は正解: S3 ショートカットはキャッシュ対応、ファイルサイズ 800 MB < 1 GB で条件を満たす
- C は不正解: S3 ショートカットはキャッシュ対応だが、2 GB > 1 GB のためキャッシュされない
- D は不正解: 内部ショートカットはキャッシュの対象外（OneLake 内のデータは既にローカル）
</details>

### Q5. ショートカット vs ミラーリング

Azure SQL Database のデータをほぼリアルタイムで Fabric に取り込み、Lakehouse で Spark による変換処理を行いたい。最適なアプローチはどれか？

A) Azure SQL DB への ADLS Gen2 ショートカットを作成する
B) Azure SQL DB のミラーリングを構成する
C) Pipeline の Copy アクティビティを5分間隔でスケジュールする
D) Dataflow Gen2 で Azure SQL DB から増分読み込みを構成する

<details>
<summary>解答</summary>

**B) Azure SQL DB のミラーリングを構成する**

- A は不正解: ショートカットは ADLS / S3 / GCS 等のオブジェクトストレージが対象。Azure SQL DB にショートカットを作成することはできない
- B は正解: ミラーリングは Azure SQL DB を CDC でほぼリアルタイムに OneLake にレプリカする。レプリカは Delta テーブルとして格納され、Spark で直接処理可能
- C は不正解: 5分間隔のコピーは「ほぼリアルタイム」より遅延が大きく、コストも高い
- D は不正解: Dataflow Gen2 の増分更新はスケジュールベースで、リアルタイムではない
</details>

---

> **次章**: 第2章 Lakehouse と Delta Lake（Delta テーブル管理、メダリオンアーキテクチャ、ディメンションモデル、SCD 実装）
