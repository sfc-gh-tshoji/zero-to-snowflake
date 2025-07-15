/***************************************************************************************************       
Asset:        Zero to Snowflake - Governance with Horizon
Version:      v1     
Copyright(c): 2025 Snowflake Inc. All rights reserved.
****************************************************************************************************

Horizonによるガバナンス
1. ロールとアクセス制御の紹介
2. 自動タグ付けによるタグベース分類
3. マスキングポリシーによる列レベルセキュリティ
4. 行アクセスポリシーによる行レベルセキュリティ
5. データメトリック関数によるデータ品質監視
6. Trust Centerによるアカウントセキュリティ監視

****************************************************************************************************/

-- セッションクエリタグを設定
ALTER SESSION SET query_tag = '{"origin":"sf_sit-is","name":"tb_101_v2","version":{"major":1, "minor":1},"attributes":{"is_quickstart":0, "source":"tastybytes", "vignette": "governance_with_horizon"}}';

-- まず、ワークシートコンテキストを設定しましょう
USE ROLE useradmin;
USE DATABASE tb_101;
USE WAREHOUSE tb_dev_wh;

/*  1. ロールとアクセス制御の紹介    
    *************************************************************************
    ユーザーガイド:
    https://docs.snowflake.com/en/user-guide/security-access-control-overview
    *************************************************************************
    
    Snowflakeアクセス制御フレームワークは以下に基づいています：
      - ロールベースアクセス制御（RBAC）：アクセス権限がロールに割り当てられ、ロールがユーザーに割り当てられます。
      - 任意アクセス制御（DAC）：各オブジェクトには所有者がおり、所有者がそのオブジェクトへのアクセスを許可できます。
    
    Snowflakeでアクセス制御を理解するための主要概念：
      - セキュリティ保護可能オブジェクト：セキュリティ保護可能オブジェクトは、誰が使用または表示できるかを制御できるものです。
        具体的に許可されていない場合、アクセスできません。これらのオブジェクトは個人ではなくグループ（ロール）によって管理されます。
        データベース、テーブル、関数などはすべてセキュリティ保護可能オブジェクトです。
      - ロール：ロールは配布可能な権限のセットのようなものです。これらのロールを個々のユーザーや他のロールにも
        付与でき、権限のチェーンを作成できます。
      - 権限：権限はオブジェクトに対して何かを行う特定の許可です。多くの小さな権限を組み合わせて、
        誰かがどの程度のアクセス権を持つかを正確に制御できます。
      - ユーザー：ユーザーは単純にSnowflakeが認識するアイデンティティ（ユーザー名など）です。
        実際の人物またはコンピュータープログラムのいずれでも構いません。
    
      Snowflakeシステム定義ロールの定義：
       - ORGADMIN：組織レベルでの操作を管理するロール。
       - ACCOUNTADMIN：システムの最上位ロールで、アカウント内の限定された/制御されたユーザー数にのみ
          付与されるべきです。
       - SECURITYADMIN：グローバルにオブジェクトグラントを管理し、ユーザーとロールを作成、監視、管理できるロール。
       - USERADMIN：ユーザーとロール管理専用のロール。
       - SYSADMIN：アカウントでウェアハウスとデータベースを作成する権限を持つロール。
       - PUBLIC：PUBLICはすべてのユーザーとロールに自動的に付与される疑似ロールです。セキュリティ保護可能オブジェクトを
           所有でき、所有するものはアカウント内の他のすべてのユーザーとロールが利用可能になります。

                                +---------------+
                                | ACCOUNTADMIN  |
                                +---------------+
                                  ^    ^     ^
                                  |    |     |
                    +-------------+-+  |    ++-------------+
                    | SECURITYADMIN |  |    |   SYSADMIN   |<------------+
                    +---------------+  |    +--------------+             |
                            ^          |     ^        ^                  |
                            |          |     |        |                  |
                    +-------+-------+  |     |  +-----+-------+  +-------+-----+
                    |   USERADMIN   |  |     |  | CUSTOM ROLE |  | CUSTOM ROLE |
                    +---------------+  |     |  +-------------+  +-------------+
                            ^          |     |      ^              ^      ^
                            |          |     |      |              |      |
                            |          |     |      |              |    +-+-----------+
                            |          |     |      |              |    | CUSTOM ROLE |
                            |          |     |      |              |    +-------------+
                            |          |     |      |              |           ^
                            |          |     |      |              |           |
                            +----------+-----+---+--+--------------+-----------+
                                                 |
                                            +----+-----+
                                            |  PUBLIC  |
                                            +----------+

    このセクションでは、カスタムデータスチュワードロールを作成し、それに権限を関連付ける方法を見ていきます。                                 
*/
-- まず、アカウント上に既に存在するロールを確認しましょう。
SHOW ROLES;

-- データスチュワードロールを作成します。
CREATE OR REPLACE ROLE tb_data_steward
    COMMENT = 'カスタムロール';
-- ロールが作成されたので、SECURITYADMINロールに切り替えて新しいロールに権限を付与できます。

/*
    新しいロールが作成されたので、クエリを実行するためにウェアハウスを使用できるようにしたいと思います。
    続行する前に、ウェアハウス権限についてより良く理解しましょう。
     
    - MODIFY：サイズ変更を含め、ウェアハウスのプロパティを変更できます。
    - MONITOR：ウェアハウスで実行された現在および過去のクエリの表示と、そのウェアハウスの
       使用統計の表示ができます。
    - OPERATE：ウェアハウスの状態を変更（停止、開始、サスペンド、再開）できます。さらに、
       ウェアハウスで実行された現在および過去のクエリの表示と実行中のクエリの中止ができます。
    - USAGE：仮想ウェアハウスを使用し、結果としてウェアハウスでクエリを実行できます。
       ウェアハウスがSQL文が送信されたときに自動再開するよう設定されている場合、
       ウェアハウスは自動的に再開し、文を実行します。
    - ALL：ウェアハウス上のOWNERSHIP以外のすべての権限を付与します。

      ウェアハウス権限が理解できたので、新しいロールにoperateとusage権限を付与できます。
      まず、SECURITYADMINロールに切り替えます。
*/
USE ROLE securityadmin;
-- まず、ロールにウェアハウスtb_dev_whを使用する能力を付与します
GRANT OPERATE, USAGE ON WAREHOUSE tb_dev_wh TO ROLE tb_data_steward;

/*
     次に、Snowflakeデータベースとスキーマグラントを理解しましょう：
      - MODIFY：データベース設定の変更ができます。
      - MONITOR：DESCRIBEコマンドの実行ができます。
      - USAGE：データベースの使用ができ、SHOW DATABASESコマンド出力でデータベース詳細の
         返却が含まれます。データベース内のオブジェクトを表示したり操作したりするには追加の権限が必要です。
      - ALL：データベース上のOWNERSHIP以外のすべての権限を付与します。
*/

GRANT USAGE ON DATABASE tb_101 TO ROLE tb_data_steward;
GRANT USAGE ON ALL SCHEMAS IN DATABASE tb_101 TO ROLE tb_data_steward;

/*
    Snowflakeテーブルとビュー内のデータへのアクセスは、以下の権限によって管理されます：
        SELECT：データを取得する能力を付与します。
        INSERT：新しい行の追加を許可します。
        UPDATE：既存の行の変更を許可します。
        DELETE：行の削除を許可します。
        TRUNCATE：テーブル内のすべての行の削除を許可します。

      次に、raw_customerスキーマのテーブルでSELECTクエリを実行できることを確認します。
*/

-- RAW_CUSTOMERスキーマのすべてのテーブルにSELECT権限を付与
GRANT SELECT ON ALL TABLES IN SCHEMA raw_customer TO ROLE tb_data_steward;
-- governanceスキーマとgovernanceスキーマのすべてのテーブルにすべての権限を付与
GRANT ALL ON SCHEMA governance TO ROLE tb_data_steward;
GRANT ALL ON ALL TABLES IN SCHEMA governance TO ROLE tb_data_steward;

/*
    新しいロールを使用するために、現在のユーザーにもロールを付与する必要があります。
    次の2つのクエリを実行して、現在のユーザーに新しいデータスチュワードロールを使用する権限を付与します。
*/
SET my_user = CURRENT_USER();
GRANT ROLE tb_data_steward TO USER IDENTIFIER($my_user);

/*
    最後に、以下のクエリを実行して新しく作成したロールを使用しましょう！
    --> 代わりに、ワークシートUIの「ロールとウェアハウスを選択」ボタンをクリックし、
        「tb_data_steward」を選択してロールを使用することもできます。
*/
USE ROLE tb_data_steward;

-- お祝いに、作業するデータの種類のアイデアを得ましょう。
SELECT TOP 100 * FROM raw_customer.customer_loyalty;

/*
    顧客ロイヤリティデータが表示されているのは素晴らしいことです！ただし、詳しく調べると、
    このテーブルには個人識別可能情報（PII）の機密情報が満載であることが明らかです。
    次のセクションでは、これを軽減する方法をさらに詳しく見ていきます。
*/

/*  2. 自動タグ付けによるタグベース分類
    ******************************************************
    ユーザーガイド:
    https://docs.snowflake.com/en/user-guide/classify-auto
    ******************************************************

    前回のクエリで、Customer Loyaltyテーブルに保存されている多くの個人識別可能情報（PII）に
    気づきました。Snowflakeの自動タグ付け機能をタグベースマスキングと組み合わせて使用し、
    クエリ結果の機密データを難読化できます。

    Snowflakeは、データベーススキーマの列を継続的に監視することで、機密情報を自動的に
    発見してタグ付けできます。データエンジニアがスキーマに分類プロファイルを割り当てた後、
    そのスキーマのテーブル内のすべての機密データは、プロファイルのスケジュールに基づいて
    自動的に分類されます。
    
    分類プロファイルを作成し、列のセマンティックカテゴリに基づいて列に自動的に割り当てられる
    タグを指定しましょう。まず、accountadminロールに切り替えましょう。
*/
USE ROLE accountadmin;

/*
    ガバナンススキーマを作成し、その中にPII用のタグを作成し、新しいロールに
    データベースオブジェクトにタグを適用する権限を付与します。
*/
CREATE OR REPLACE TAG governance.pii;
GRANT APPLY TAG ON ACCOUNT TO ROLE tb_data_steward;

/*
    まず、ロールtb_data_stewardに適切な権限を付与して、データ分類を実行し、
    raw_customerスキーマで分類プロファイルを作成できるようにする必要があります。
*/
GRANT EXECUTE AUTO CLASSIFICATION ON SCHEMA raw_customer TO ROLE tb_data_steward;
GRANT DATABASE ROLE SNOWFLAKE.CLASSIFICATION_ADMIN TO ROLE tb_data_steward;
GRANT CREATE SNOWFLAKE.DATA_PRIVACY.CLASSIFICATION_PROFILE ON SCHEMA governance TO ROLE tb_data_steward;

-- データスチュワードロールに戻ります。
USE ROLE tb_data_steward;

/*
    分類プロファイルを作成します。スキーマに追加されたオブジェクトは即座に分類され、
    30日間有効で、自動的にタグ付けされます。
*/
CREATE OR REPLACE SNOWFLAKE.DATA_PRIVACY.CLASSIFICATION_PROFILE
  governance.tb_classification_profile(
    {
      'minimum_object_age_for_classification_days': 0,
      'maximum_classification_validity_days': 30,
      'auto_tag': true
    });

/*
    指定されたセマンティックカテゴリに基づいて列を自動的にタグ付けするタグマップを作成します。
    これは、semantic_categories配列内の値のいずれかで分類された列が、PIIタグで自動的に
    タグ付けされることを意味します。
*/
CALL governance.tb_classification_profile!SET_TAG_MAP(
  {'column_tag_map':[
    {
      'tag_name':'tb_101.governance.pii',
      'tag_value':'pii',
      'semantic_categories':['NAME', 'PHONE_NUMBER', 'POSTAL_CODE', 'DATE_OF_BIRTH', 'CITY', 'EMAIL']
    }]});

-- SYSTEM$CLASSIFYを呼び出して、分類プロファイルでcustomer_loyaltyテーブルを自動的に分類します。
CALL SYSTEM$CLASSIFY('tb_101.raw_customer.customer_loyalty', 'tb_101.governance.tb_classification_profile');

/*
    次のクエリを実行して、自動分類とタグ付けの結果を確認します。すべてのSnowflakeアカウントで
    利用可能な自動生成されたINFORMATION_SCHEMAからメタデータを取得します。各列がどのように
    タグ付けされ、以前のステップで作成した分類プロファイルとどのように関連しているかを
    確認してください。
    
    すべての列がPRIVACY_CATEGORYとSEMANTIC_CATEGORYタグでタグ付けされており、
    それぞれ独自の目的があることがわかります。PRIVACY_CATEGORYは列内の個人データの
    機密レベルを示し、SEMANTIC_CATEGORYはデータが表す現実世界の概念を記述します。
    
    最後に、分類タグマップ配列で指定したセマンティックカテゴリでタグ付けされた列が、
    カスタム「PII」タグでタグ付けされていることに注意してください。
*/
SELECT 
    column_name,
    tag_database,
    tag_schema,
    tag_name,
    tag_value,
    apply_method
FROM TABLE(INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS('raw_customer.customer_loyalty', 'table'));

/*  3. マスキングポリシーによる列レベルセキュリティ
    **************************************************************
    ユーザーガイド:
    https://docs.snowflake.com/en/user-guide/security-column-intro
    **************************************************************

    Snowflakeの列レベルセキュリティでは、マスキングポリシーを使用して列内のデータを保護できます。
    主な機能が2つあります：クエリ時に機密データを隠す、または変換できる動的データマスキングと、
    データがSnowflakeに入る前にトークン化し、クエリ時にデトークン化できる外部トークン化です。

    機密列がPIIとしてタグ付けされたので、そのタグに関連付けるマスキングポリシーをいくつか作成します。
    最初のものは、名前（姓と名）、メール、電話番号などの機密文字列データ用です。
    2番目は、誕生日などの機密DATE値用です。

    マスキングロジックは両方とも似ています：現在のロールがPIIタグ付き列をクエリし、アカウント管理者または
    TastyBytes管理者でない場合、文字列値は「MASKED」を表示します。日付値は元の年のみを表示し、
    月と日は01-01になります。
*/

-- 機密文字列データ用のマスキングポリシーを作成
CREATE OR REPLACE MASKING POLICY governance.mask_string_pii AS (original_value STRING)
RETURNS STRING ->
  CASE WHEN
    -- ユーザーの現在のロールが特権ロールの1つでない場合、列をマスクします。
    CURRENT_ROLE() NOT IN ('ACCOUNTADMIN', 'TB_ADMIN')
    THEN '****MASKED****'
    -- そうでなければ（タグが機密でない、またはロールが特権を持つ場合）、元の値を表示します。
    ELSE original_value
  END;

-- 機密DATEデータ用のマスキングポリシーを作成
CREATE OR REPLACE MASKING POLICY governance.mask_date_pii AS (original_value DATE)
RETURNS DATE ->
  CASE WHEN
    CURRENT_ROLE() NOT IN ('ACCOUNTADMIN', 'TB_ADMIN')
    THEN DATE_TRUNC('year', original_value) -- マスクされた場合、年のみが変更されず、月と日は01-01になります
    ELSE original_value
  END;

-- 顧客ロイヤリティテーブルに自動的に適用されたタグに両方のマスキングポリシーを添付
ALTER TAG governance.pii SET
    MASKING POLICY governance.mask_string_pii,
    MASKING POLICY governance.mask_date_pii;

/*
    publicロールに切り替えて、顧客ロイヤリティテーブルから最初の100行をクエリし、
    マスキングポリシーが機密データをどのように難読化するかを観察します。
*/
USE ROLE public;
SELECT TOP 100 * FROM raw_customer.customer_loyalty;

-- 今度は、TB_ADMINロールに切り替えて、マスキングポリシーが管理者ロールに適用されないことを観察
USE ROLE tb_admin;
SELECT TOP 100 * FROM raw_customer.customer_loyalty;

/*  4. 行アクセスポリシーによる行レベルセキュリティ
    ***********************************************************
    ユーザーガイド:
    https://docs.snowflake.com/en/user-guide/security-row-intro
    ***********************************************************

    Snowflakeは、行アクセスポリシーを使用した行レベルセキュリティをサポートして、
    クエリ結果で返される行を決定します。ポリシーはテーブルに添付され、定義したルールに対して
    各行を評価することで機能します。これらのルールは、現在のロールなど、
    クエリを実行するユーザーの属性をよく使用します。

    例えば、行アクセスポリシーを使用して、米国のユーザーが米国内の顧客のデータのみを
    表示することを確実にできます。

    まず、データスチュワードロールに切り替えましょう。
*/
USE ROLE tb_data_steward;

-- 行アクセスポリシーを作成する前に、行ポリシーマップを作成します。
CREATE OR REPLACE TABLE governance.row_policy_map
    (role STRING, country_permission STRING);

/*
    行ポリシーマップは、ロールを許可されたアクセス行値に関連付けます。
    例えば、ロールtb_data_engineerを国の値「United States」に関連付けると、
    tb_data_engineerは国の値が「United States」の行のみを表示します。
*/
INSERT INTO governance.row_policy_map
    VALUES('tb_data_engineer', 'United States');

/*
    行ポリシーマップが配置されたので、行アクセスポリシーを作成します。
    
    このポリシーは、管理者が無制限の行アクセスを持ち、ポリシーマップ内の他のロールは
    関連する国に一致する行のみを表示できることを述べています。
*/
CREATE OR REPLACE ROW ACCESS POLICY governance.customer_loyalty_policy
    AS (country STRING) RETURNS BOOLEAN ->
        CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SYSADMIN') 
        OR EXISTS 
            (
            SELECT 1
                FROM governance.row_policy_map rp
            WHERE
                UPPER(rp.role) = CURRENT_ROLE()
                AND rp.country_permission = country
            );

-- 'country'列の顧客ロイヤリティテーブルに行アクセスポリシーを適用。
ALTER TABLE raw_customer.customer_loyalty
    ADD ROW ACCESS POLICY governance.customer_loyalty_policy ON (country);

/*
    今度は、行ポリシーマップで「United States」に関連付けたロールに切り替えて、
    行アクセスポリシーがあるテーブルをクエリした結果を観察します。
*/
USE ROLE tb_data_engineer;

-- 米国の顧客のみが表示されるはずです。 
SELECT TOP 100 * FROM raw_customer.customer_loyalty;

/*
    お疲れ様でした！Snowflakeの列および行レベルセキュリティ戦略でデータをガバナンスし、
    セキュリティを確保する方法についてより良い理解が得られたはずです。個人識別可能情報を含む列を
    セキュリティ保護するためにマスキングポリシーと組み合わせて使用するタグの作成方法と、
    ロールが特定の列値のみにアクセスすることを確実にする行アクセスポリシーについて学びました。
*/

/*  5. データメトリック関数によるデータ品質監視
    ***********************************************************
    ユーザーガイド:
    https://docs.snowflake.com/en/user-guide/data-quality-intro
    ***********************************************************

    Snowflakeは、プラットフォーム内で直接品質チェックを自動化する強力な機能である
    データメトリック関数（DMF）を使用してデータの一貫性と信頼性を維持します。
    これらのチェックをテーブルやビューでスケジュールすることで、ユーザーはデータの
    整合性を明確に理解でき、より信頼性の高いデータに基づいた意思決定につながります。
    
    Snowflakeは、即座に使用できる事前構築されたシステムDMFと、独自のビジネスロジック用の
    カスタムDMFを作成する柔軟性の両方を提供し、包括的な品質監視を確保します。

    システムDMFのいくつかを見てみましょう！
*/

-- DMFの使用を開始するためにTasteBytesデータスチュワードロールに戻ります
USE ROLE tb_data_steward;

-- これにより、注文ヘッダーテーブルからのnull顧客IDの割合が返されます。
SELECT SNOWFLAKE.CORE.NULL_PERCENT(SELECT customer_id FROM raw_pos.order_header);

-- DUPLICATE_COUNTを使用して重複する注文IDをチェックできます。
SELECT SNOWFLAKE.CORE.DUPLICATE_COUNT(SELECT order_id FROM raw_pos.order_header); 

-- すべての注文の平均注文合計金額
SELECT SNOWFLAKE.CORE.AVG(SELECT order_total FROM raw_pos.order_header);

/*
    特定のビジネスルールに従ってデータ品質を監視するための独自のカスタムデータメトリック関数も
    作成できます。単価×数量と等しくない注文合計をチェックするカスタムDMFを作成します。
*/

-- カスタムデータメトリック関数を作成
CREATE OR REPLACE DATA METRIC FUNCTION governance.invalid_order_total_count(
    order_prices_t table(
        order_total NUMBER,
        unit_price NUMBER,
        quantity INTEGER
    )
)
RETURNS NUMBER
AS
'SELECT COUNT(*)
 FROM order_prices_t
 WHERE order_total != unit_price * quantity';

-- 合計が単価×数量と等しくない新しい注文をシミュレート
INSERT INTO raw_pos.order_detail
SELECT
    904745311,
    459520442,
    52,
    null,
    0,
    2, -- 数量
    5.0, -- 単価
    5.0, -- 合計価格（意図的に不正確）
    null;

-- 注文詳細テーブルでカスタムDMFを呼び出し。
SELECT governance.invalid_order_total_count(
    SELECT 
        price, 
        unit_price, 
        quantity 
    FROM raw_pos.order_detail
) AS num_orders_with_incorrect_price;

-- 変更時にトリガーするように注文詳細テーブルにデータメトリックスケジュールを設定
ALTER TABLE raw_pos.order_detail
    SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';

-- テーブルにカスタムDMFを割り当て 
ALTER TABLE raw_pos.order_detail
    ADD DATA METRIC FUNCTION governance.invalid_order_total_count
    ON (price, unit_price, quantity);

/*  6. Trust Centerによるアカウントセキュリティ監視
    **************************************************************
    ユーザーガイド:
    https://docs.snowflake.com/en/user-guide/trust-center/overview
    **************************************************************

    Trust Centerは、スキャナーを使用してアカウントのセキュリティリスクを評価・監視する
    自動チェックを可能にします。スキャナーは、アカウントのセキュリティリスクと違反をチェックし、
    その結果に基づいて推奨アクションを提供するスケジュールされたバックグラウンドプロセスです。
    これらはしばしばスキャナーパッケージにグループ化されます。
    
    Trust Centerの一般的なユースケース：
        - ユーザーの多要素認証が有効になっていることを確認
        - 過剰に特権化されたロールの発見
        - 最低90日間ログインしていない非アクティブユーザーの発見
        - リスクの高いユーザーの発見と軽減

    開始する前に、Trust Centerの管理者になるために必要な権限を管理者ロールに付与する必要があります。
*/
USE ROLE accountadmin;
GRANT APPLICATION ROLE SNOWFLAKE.TRUST_CENTER_ADMIN TO ROLE tb_admin;
USE ROLE tb_admin; -- TastyBytes管理者ロールに戻る

/*
    ナビゲーションメニューの「監視」ボタンをクリックし、次に「Trust Center」をクリックして
    Trust Centerにアクセスできます。必要に応じて別のブラウザタブでTrust Centerを開くことができます。
    Trust Centerを最初にロードすると、いくつかのペインとセクションが表示されます：
        1. タブ：検出事項、スキャナーパッケージ
        2. パスワード準備状況ペイン
        3. オープンセキュリティ違反
        4. フィルター付き違反リスト

    タブの下にCIS BenchmarksスキャナーパッケージIを有効にすることを促すメッセージが表示される場合があります。
    次にそれを実行します。

    「スキャナーパッケージ」タブをクリックします。ここにスキャナーパッケージのリストが表示されます。
    これらは、アカウントのセキュリティリスクをチェックするスキャナーまたはスケジュールされた
    バックグラウンドプロセスのグループです。各スキャナーパッケージについて、名前、プロバイダー、
    アクティブおよび非アクティブスキャナーの数、ステータスを確認できます。
    セキュリティエッセンシャルスキャナーパッケージを除き、すべてのスキャナーパッケージは
    デフォルトで無効になっています。
    
    「CIS Benchmarks」をクリックして、スキャナーパッケージの詳細を確認します。ここで、
    パッケージを有効にするオプションとともにスキャナーパッケージの名前と説明が表示されます。
    その下には、スキャナーパッケージ内のスキャナーのリストがあります。これらのスキャナーのいずれかを
    クリックして、スケジュール、最後に実行された時刻と日、説明などの詳細を確認できます。

    「パッケージを有効にする」ボタンをクリックして今すぐ有効にしましょう。これにより「スキャナーパッケージを有効にする」 
    モーダルが表示され、スキャナーパッケージのスケジュールを設定できます。このパッケージを
    月次スケジュールで実行するよう設定しましょう。

    「頻度」のドロップダウンをクリックし、「月次」のオプションを選択します。他のすべての値は
    そのままにしておきます。パッケージは有効化されたときと設定されたスケジュールで自動的に実行されます。
    
    オプションで、通知設定を設定できます。最小重要度トリガーレベルが「クリティカル」で、
    受信者で「管理者ユーザー」が選択されているデフォルト値を維持できます。
    「続行」を押します。
    スキャナーパッケージが完全に有効になるまで数分かかる場合があります。

    アカウントで「脅威インテリジェンス」スキャナーパッケージについてもう一度これを繰り返しましょう。
    前回のスキャナーパッケージと同じ設定を使用します。
    
    両方のパッケージが有効になったら、「検出事項」タブに戻って、スキャナーパッケージが
    発見した違反を確認しましょう。

    各重要度レベルでの違反数のグラフとともに、違反リストにより多くのエントリが表示されるはずです。
    違反リストでは、短い説明、重要度、スキャナーパッケージを含む、すべての違反に関する詳細情報を
    確認できます。違反を解決済みとしてマークするオプションもあります。
    さらに、個々の違反をクリックすると、要約や修復のオプションなど、違反に関するより詳細な
    情報を含む詳細ペインが表示されます。

    違反リストは、ドロップダウンオプションを使用してステータス、重要度、スキャナーパッケージで
    フィルタリングできます。違反グラフの重要度カテゴリをクリックすることで、その種類のフィルターも
    適用されます。
    
    現在アクティブなフィルターカテゴリの横にある「X」をクリックしてフィルターをキャンセルします。 
*/

-------------------------------------------------------------------------
--RESET--
-------------------------------------------------------------------------
USE ROLE accountadmin;

-- データスチュワード用ロールを削除
DROP ROLE IF EXISTS tb_data_steward;

-- マスキングポリシー
ALTER TAG IF EXISTS governance.pii UNSET
    MASKING POLICY governance.mask_string_pii,
    MASKING POLICY governance.mask_date_pii;
DROP MASKING POLICY IF EXISTS governance.mask_string_pii;
DROP MASKING POLICY IF EXISTS governance.mask_date_pii;

-- 自動分類
ALTER SCHEMA raw_customer UNSET CLASSIFICATION_PROFILE;
DROP SNOWFLAKE.DATA_PRIVACY.CLASSIFICATION_PROFILE IF EXISTS tb_classification_profile;

-- 行アクセスポリシー
ALTER TABLE raw_customer.customer_loyalty 
    DROP ROW ACCESS POLICY governance.customer_loyalty_policy;
DROP ROW ACCESS POLICY IF EXISTS governance.customer_loyalty_policy;

-- データメトリック関数
DELETE FROM raw_pos.order_detail WHERE order_detail_id = 904745311;
ALTER TABLE raw_pos.order_detail
    DROP DATA METRIC FUNCTION governance.invalid_order_total_count ON (price, unit_price, quantity);
DROP FUNCTION governance.invalid_order_total_count(TABLE(NUMBER, NUMBER, INTEGER));
ALTER TABLE raw_pos.order_detail UNSET DATA_METRIC_SCHEDULE;

-- タグの解除
ALTER TABLE raw_customer.customer_loyalty
  MODIFY
    COLUMN first_name UNSET TAG governance.pii, SNOWFLAKE.CORE.PRIVACY_CATEGORY, SNOWFLAKE.CORE.SEMANTIC_CATEGORY,
    COLUMN last_name UNSET TAG governance.pii, SNOWFLAKE.CORE.PRIVACY_CATEGORY, SNOWFLAKE.CORE.SEMANTIC_CATEGORY,
    COLUMN e_mail UNSET TAG governance.pii, SNOWFLAKE.CORE.PRIVACY_CATEGORY, SNOWFLAKE.CORE.SEMANTIC_CATEGORY,
    COLUMN phone_number UNSET TAG governance.pii, SNOWFLAKE.CORE.PRIVACY_CATEGORY, SNOWFLAKE.CORE.SEMANTIC_CATEGORY,
    COLUMN postal_code UNSET TAG governance.pii, SNOWFLAKE.CORE.PRIVACY_CATEGORY, SNOWFLAKE.CORE.SEMANTIC_CATEGORY,
    COLUMN marital_status UNSET TAG governance.pii, SNOWFLAKE.CORE.PRIVACY_CATEGORY, SNOWFLAKE.CORE.SEMANTIC_CATEGORY,
    COLUMN gender UNSET TAG governance.pii, SNOWFLAKE.CORE.PRIVACY_CATEGORY, SNOWFLAKE.CORE.SEMANTIC_CATEGORY,
    COLUMN birthday_date UNSET TAG governance.pii, SNOWFLAKE.CORE.PRIVACY_CATEGORY, SNOWFLAKE.CORE.SEMANTIC_CATEGORY,
    COLUMN country UNSET TAG governance.pii, SNOWFLAKE.CORE.PRIVACY_CATEGORY, SNOWFLAKE.CORE.SEMANTIC_CATEGORY,
    COLUMN city UNSET TAG governance.pii, SNOWFLAKE.CORE.PRIVACY_CATEGORY, SNOWFLAKE.CORE.SEMANTIC_CATEGORY;

-- PIIタグを削除
DROP TAG IF EXISTS governance.pii;
-- クエリタグの解除
ALTER SESSION UNSET query_tag;
ALTER WAREHOUSE tb_dev_wh SUSPEND;