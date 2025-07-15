/***************************************************************************************************       
Asset:        Zero to Snowflake - Getting Started with Snowflake
Version:      v1     
Copyright(c): 2025 Snowflake Inc. All rights reserved.
****************************************************************************************************

Zero to Snowflake - Getting Started with Snowflake
1. 仮想ウェアハウスと設定
2. 永続化クエリ結果の活用
3. 基本的なデータ変換技術
4. UNDROPによるデータ復旧
5. リソースモニター
6. 予算
7. ユニバーサル検索

****************************************************************************************************/

-- 開始前に、このクエリを実行してセッションクエリタグを設定します。
ALTER SESSION SET query_tag = '{"origin":"sf_sit-is","name":"tb_101_v2","version":{"major":1, "minor":1},"attributes":{"is_quickstart":0, "source":"tastybytes", "vignette": "getting_started_with_snowflake"}}';

-- ワークシートコンテキストを設定することから始めます。データベース、スキーマ、ロールを設定します。

USE DATABASE tb_101;
USE ROLE accountadmin;

/*   1. 仮想ウェアハウスと設定 
    **************************************************************
     ユーザーガイド:
     https://docs.snowflake.com/en/user-guide/warehouses-overview
    **************************************************************
    
    仮想ウェアハウスは、Snowflakeデータの分析を実行するために必要な動的で
    スケーラブル、かつコスト効率的な計算能力です。その目的は、基盤技術の詳細を
    気にする必要なく、すべてのデータ処理ニーズを処理することです。

    ウェアハウスパラメータ:
      > WAREHOUSE_SIZE: 
            サイズは、ウェアハウス内のクラスターごとに利用可能な計算リソースの量を指定します。
            利用可能なサイズはX-SmallからEX-Largeまでの範囲です。
            デフォルト: 'XSmall'
      > WAREHOUSE_TYPE:
            仮想ウェアハウスの種類を定義し、アーキテクチャと動作を決定します
            種類:
                'STANDARD' 汎用ワークロード用
                'SNOWPARK_OPTIMIZED' メモリ集約的なワークロード用
            デフォルト: 'STANDARD'
      > AUTO_SUSPEND:
            ウェアハウスが自動的にサスペンドする非アクティブ期間を指定します。
            デフォルト: 600秒
      > INITIALLY_SUSPENDED:
            作成直後にウェアハウスがサスペンド状態で開始するかどうかを決定します。
            デフォルト: TRUE
      > AUTO_RESUME:
            クエリが送信されたときにサスペンド状態から自動的にウェアハウスが再開するかどうかを決定します。
            デフォルト: TRUE

        それでは、最初のウェアハウスを作成しましょう！
*/

-- まず、アクセス権限のあるアカウント上の既存のウェアハウスを確認しましょう
SHOW WAREHOUSES;

/*
    これにより、ウェアハウスとその属性のリストが返されます：名前、状態（実行中またはサスペンド中）、
    タイプ、サイズなど多くの情報が含まれます。
    
    Snowsightですべてのウェアハウスを表示・管理することもできます。ウェアハウスページにアクセスするには、
    ナビゲーションメニューの管理ボタンをクリックし、拡張された管理カテゴリの「ウェアハウス」リンクを
    クリックします。
    
    ウェアハウスページでは、このアカウント上のウェアハウスとその属性のリストを確認できます。
*/

-- 簡単なSQLコマンドでウェアハウスを作成できます
CREATE OR REPLACE WAREHOUSE my_wh
    COMMENT = 'My TastyBytes warehouse'
    WAREHOUSE_TYPE = 'standard'
    WAREHOUSE_SIZE = 'xsmall'
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 2
    SCALING_POLICY = 'standard'
    AUTO_SUSPEND = 60
    INITIALLY_SUSPENDED = true,
    AUTO_RESUME = false;

/*
    ウェアハウスができたので、このワークシートがこのウェアハウスを使用することを指定する必要があります。
    これはSQLコマンドまたはUIで実行できます。
*/

-- ウェアハウスを使用
USE WAREHOUSE my_wh;

/*
    簡単なクエリを実行してみることができますが、結果ペインにエラーメッセージが表示され、
    MY_WH ウェアハウスがサスペンド中であることが通知されます。今すぐ試してみてください。
*/
SELECT * FROM raw_pos.truck_details;

/*    
    クエリの実行およびすべてのDML操作にはアクティブなウェアハウスが必要ですので、
    データから洞察を得るためにはウェアハウスを再開する必要があります。
    
    エラーメッセージには、SQLコマンド「ALTER warehouse MY_WH resume」を実行する
    提案も含まれていました。実行してみましょう！
*/
ALTER WAREHOUSE my_wh RESUME;

/* 
    また、ウェアハウスが再度サスペンドした場合に手動で再開する必要がないよう、
    AUTO_RESUMEをTRUEに設定します。
 */
ALTER WAREHOUSE my_wh SET AUTO_RESUME = TRUE;

-- ウェアハウスが実行中になったので、先ほどのクエリを再度実行してみましょう
SELECT * FROM raw_pos.truck_details;

-- これでデータに対してクエリを実行できるようになりました

/* 
    次に、Snowflakeでのウェアハウススケーラビリティの力を見てみましょう。
    
    Snowflakeのウェアハウスはスケーラビリティと弾力性のために設計されており、
    ワークロードのニーズに基づいて計算リソースを上下に調整する力を提供します。
    
    簡単なALTER WAREHOUSE文でウェアハウスを即座にスケールアップできます。
*/
ALTER WAREHOUSE my_wh SET warehouse_size = 'XLarge';

-- それでは、トラックごとの売上を見てみましょう。
SELECT
    o.truck_brand_name,
    COUNT(DISTINCT o.order_id) AS order_count,
    SUM(o.price) AS total_sales
FROM analytics.orders_v o
GROUP BY o.truck_brand_name
ORDER BY total_sales DESC;

/*
    <SQLワークシート利用時>
    結果パネルを開いて、右上のツールバーを見てください。ここには検索、列選択、クエリ詳細と
    実行時間統計の表示、列統計の表示、結果のダウンロードのオプションがあります。
    
    検索 - 検索語句を使用して結果をフィルタリング
    結果の列を選択 - 結果に表示する列を有効/無効にする
    クエリの詳細 - SQLテキスト、返された行数、クエリID、実行されたロールとウェアハウスなど、
                クエリに関連する情報が含まれます。
    クエリ期間 - コンパイル時間、プロビジョニング時間、実行時間によってクエリの
                    実行にかかった時間を分析します。
    列統計 - 結果パネルの列の分布に関するデータを表示します。
    結果ダウンロード - 結果をCSVとしてエクスポート・ダウンロードします。
*/

/*  2. 永続化クエリ結果の活用
    *******************************************************************
    ユーザーガイド:
    https://docs.snowflake.com/en/user-guide/querying-persisted-results
    *******************************************************************
    
    続行する前に、Snowflakeの別の強力な機能を実演するのに適した場所です：
    クエリ結果キャッシュ。
    
    上記のクエリを最初に実行したとき、XLウェアハウスでも完了まで数秒かかりました。

    上記の同じ「トラックごとの売上」クエリを実行し、クエリ実行時間ペインの
    総実行時間に注意してください。最初に実行したときは数秒かかったのが、
    次回は数百ミリ秒しかかからないことがわかります。これがクエリ結果キャッシュの効果です。

    クエリ履歴パネルを開いて、クエリが最初に実行されたときと2回目との実行時間を比較してください。
    
    クエリ結果キャッシュの概要:
    - すべてのクエリの結果は24時間保持されますが、クエリが実行されるたびにタイマーはリセットされます。
    - 結果キャッシュへのヒットはほとんど計算リソースを必要とせず、頻繁に実行される
      レポートやダッシュボードに理想的で、クレジット消費の管理に役立ちます。
    - キャッシュはクラウドサービス層に存在し、個々のウェアハウスから論理的に分離されています。
      これにより、同じアカウント内のすべての仮想ウェアハウスとユーザーがグローバルにアクセスできます。
*/

-- より小さなデータセットでの作業を開始するので、ウェアハウスを縮小できます
ALTER WAREHOUSE my_wh SET warehouse_size = 'XSmall';

/*  3. 基本的なデータ変換技術

    ウェアハウスが設定され実行中になったので、トラックメーカーの分布を理解したいと思いますが、
    この情報は年、メーカー、モデルに関する情報をVARIANTデータ型で格納する別の列
    「truck_build」に埋め込まれています。

    VARIANTデータ型は半構造化データの例です。OBJECT、ARRAY、その他のVARIANT値を含む
    あらゆる種類のデータを格納できます。今回の場合、truck_buildは年、メーカー、モデルの
    3つの異なるVARCHAR値を含む単一のOBJECTを格納しています。
    
    より簡単で分析しやすくするため、3つのプロパティすべてをそれぞれの列に分離します。
*/
SELECT truck_build FROM raw_pos.truck_details;

/*  ゼロコピークローニング

    truck_build列のデータは一貫して同じ形式に従っています。品質分析をより簡単に実行するために、
    「make」用の別の列が必要です。計画は、トラックテーブルの開発用コピーを作成し、年、メーカー、
    モデル用の新しい列を追加し、truck_build VARIANTオブジェクトから各プロパティを抽出して
    これらの新しい列に保存することです。
 
    Snowflakeの強力なゼロコピークローニングにより、追加のストレージスペースを使用することなく、
    データベースオブジェクトの同一で完全に機能する別のコピーを瞬時に作成できます。

    ゼロコピークローニングは、Snowflakeの独自のマイクロパーティションアーキテクチャを活用して、
    クローンオブジェクトと元のコピー間でデータを共有します。いずれかのテーブルへの変更により、
    変更されたデータのみの新しいマイクロパーティションが作成されます。これらの新しいマイクロパーティションは、
    クローンまたは元のクローンオブジェクトのいずれであっても、所有者が排他的に所有します。
    基本的に、一方のテーブルに加えられた変更は、元のテーブルまたはクローンコピーのどちらにも影響しません。
*/

-- truckテーブルのゼロコピークローンとしてtruck_devテーブルを作成
CREATE OR REPLACE TABLE raw_pos.truck_dev CLONE raw_pos.truck_details;

-- truck_devへのtruckテーブルクローンの成功を確認
SELECT TOP 15 * 
FROM raw_pos.truck_dev
ORDER BY truck_id;

/*
    truckテーブルの開発用コピーができたので、新しい列を追加することから始めます。
    注意：3つの文を一度に実行するには、それらを選択して画面右上の青い「実行」ボタンを
    クリックするか、キーボードを使用してください。
    
        Mac: command + return
        Windows: Ctrl + Enter
*/

ALTER TABLE raw_pos.truck_dev ADD COLUMN IF NOT EXISTS year NUMBER;
ALTER TABLE raw_pos.truck_dev ADD COLUMN IF NOT EXISTS make VARCHAR(255);
ALTER TABLE raw_pos.truck_dev ADD COLUMN IF NOT EXISTS model VARCHAR(255);

/*
    新しい列をtruck_build列から抽出したデータで更新しましょう。
    コロン（:）演算子を使用してtruck_build列の各キーの値にアクセスし、
    その値をそれぞれの列に設定します。
*/
UPDATE raw_pos.truck_dev
SET 
    year = truck_build:year::NUMBER,
    make = truck_build:make::VARCHAR,
    model = truck_build:model::VARCHAR;

-- 3つの列がテーブルに正常に追加され、truck_buildから抽出されたデータで入力されたことを確認
SELECT year, make, model FROM raw_pos.truck_dev;

-- TasteBytesフードトラック車両群の分布を把握するために、異なるメーカーをカウントできます。
SELECT 
    make,
    COUNT(*) AS count
FROM raw_pos.truck_dev
GROUP BY make
ORDER BY make ASC;

/*
    上記のクエリを実行した後、データセットに問題があることに気づきます。一部のトラックのメーカーが「Ford」で、
    一部が「Ford_」となっており、同じトラックメーカーに対して2つの異なるカウントが得られています。
*/

-- まずUPDATEを使用して「Ford_」のすべての出現を「Ford」に変更します
UPDATE raw_pos.truck_dev
    SET make = 'Ford'
    WHERE make = 'Ford_';

-- make列が正常に更新されたことを確認 
SELECT truck_id, make 
FROM raw_pos.truck_dev
ORDER BY truck_id;

/*
    make列が正常に見えるので、truckテーブルをtruck_devテーブルとSWAPしましょう
    このコマンドは2つのテーブル間でメタデータとデータをアトミックに交換し、truck_devテーブルを
    瞬時に新しい本番truckテーブルに昇格させます。
*/
ALTER TABLE raw_pos.truck_details SWAP WITH raw_pos.truck_dev; 

-- 正確なメーカーカウントを取得するために以前のクエリを実行
SELECT 
    make,
    COUNT(*) AS count
FROM raw_pos.truck_details
GROUP BY
    make
ORDER BY count DESC;
/*
    変更は良好に見えます。データを3つの別々の列に分割したので、本番データベースからtruck_build列を
    削除することでデータセットのクリーンアップを実行します。
    その後、もう必要ないのでtruck_devテーブルを削除できます。
*/

-- 簡単なALTER TABLE ... DROP COLUMNコマンドで古いtruck build列を削除できます
ALTER TABLE raw_pos.truck_details DROP COLUMN truck_build;

-- これでtruck_devテーブルを削除できます
DROP TABLE raw_pos.truck_details;

/*  4. UNDROPによるデータ復旧
	
    大変です！本番トラックテーブルを誤って削除してしまいました。😱

    幸い、UNDROPコマンドを使用してテーブルを削除前の状態に復元できます。
    UNDROPはSnowflakeの強力なTime Travel機能の一部で、設定されたデータ保持期間内
    （デフォルト24時間）で削除されたデータベースオブジェクトの復元を可能にします。

    UNDROPを使用して本番「truck」テーブルをASAPで復元しましょう！
*/

-- オプション：「truck」テーブルがもう存在しないことを確認するためにこのクエリを実行
    -- 注意：「Table TRUCK does not exist or not authorized.」エラーはテーブルが削除されたことを意味します。
DESCRIBE TABLE raw_pos.truck_details;

-- 本番「truck」テーブルでUNDROPを実行して、削除前の正確な状態に復元します
UNDROP TABLE raw_pos.truck_details;

-- テーブルが正常に復元されたことを確認
SELECT * from raw_pos.truck_details;

-- 今度は実際のtruck_devテーブルを削除
DROP TABLE raw_pos.truck_dev;

/*  5. リソースモニター
    ***********************************************************
    ユーザーガイド:                                   
    https://docs.snowflake.com/en/user-guide/resource-monitors
    ***********************************************************

    計算使用量と支出の監視は、クラウドベースのワークフローにとって重要です。Snowflakeは
    リソースモニターを使用してウェアハウスのクレジット使用量を追跡する簡単で分かりやすい方法を提供します。

    リソースモニターでは、クレジットクォータを定義し、定義された使用量しきい値に到達したときに
    関連するウェアハウスで特定のアクションをトリガーします。

    リソースモニターが実行できるアクション:
    -NOTIFY: 指定されたユーザーまたはロールにメール通知を送信します。
    -SUSPEND: しきい値に到達したときに関連するウェアハウスをサスペンドします。
              注意：実行中のクエリは完了が許可されます。
    -SUSPEND_IMMEDIATE: しきい値に到達したときに関連するウェアハウスをサスペンドし、
                        実行中のすべてのクエリをキャンセルします。

    今度は、ウェアハウスmy_wh用のリソースモニターを作成します

    Snowsightでアカウントレベルロールをaccountadminに素早く設定しましょう;
    手順:
    - 画面左下のユーザーアイコンをクリック
    - 「ロール切り替え」にホバー
    - ロールリストパネルで「ACCOUNTADMIN」を選択

   次に、ワークシートでaccountadminロールを使用します
*/
USE ROLE accountadmin;

-- SQLでリソースモニターを作成するために以下のクエリを実行
CREATE OR REPLACE RESOURCE MONITOR my_resource_monitor
    WITH CREDIT_QUOTA = 100
    FREQUENCY = MONTHLY -- DAILY、WEEKLY、YEARLY、またはNEVER（一回限りのクォータ用）も可能
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS ON 75 PERCENT DO NOTIFY
             ON 90 PERCENT DO SUSPEND
             ON 100 PERCENT DO SUSPEND_IMMEDIATE;

-- リソースモニターが作成されたので、my_whに適用
ALTER WAREHOUSE my_wh 
    SET RESOURCE_MONITOR = my_resource_monitor;

/*  6. 予算
    ****************************************************
      ユーザーガイド:                                   
      https://docs.snowflake.com/en/user-guide/budgets 
    ****************************************************
      
    前のステップでは、ウェアハウスのクレジット使用量を監視するリソースモニターを設定しました。
    このステップでは、Snowflakeでのコスト管理により包括的で柔軟なアプローチのために予算を作成します。
    
    リソースモニターはウェアハウスと計算使用量に特に結び付けられていますが、予算はあらゆるSnowflake
    オブジェクトまたはサービスのコストを追跡し、支出制限を課し、金額が指定されたしきい値に達したときに
    ユーザーに通知するために使用できます。
*/

-- まず予算を作成しましょう
CREATE OR REPLACE SNOWFLAKE.CORE.BUDGET my_budget()
    COMMENT = 'My Tasty Bytes Budget';

/*
    予算を設定する前に、アカウントでメールアドレスを確認する必要があります。

    メールアドレスを確認するには:
    - 画面左下のユーザーアイコンをクリック
    - 設定をクリック
    - メールフィールドにメールアドレスを入力
    - 「保存」をクリック
    - メールを確認し、指示に従ってメールを確認
        注意：数分経ってもメールが届かない場合は、「確認の再送信」をクリック
     
    新しい予算が設定され、メールが確認され、アカウントレベルロールがaccountadminに設定されたので、
    Snowsightの予算ページに移動して予算にリソースを追加しましょう。

    Snowsightで予算ページにアクセスするには:
    - ナビゲーションメニューの管理ボタンをクリック
    - 最初の項目「コスト管理」をクリック
    - 「予算」タブをクリック
    
    ウェアハウスの選択を求められた場合は、tb_dev_whを選択してください。そうでなければ、
    画面右上のウェアハウスパネルからウェアハウスがtb_dev_whに設定されていることを確認してください。
    
    予算ページでは、現在の期間の支出に関する指標が表示されます。
    画面中央には、予測支出と現在の支出のグラフが表示されます。
    画面下部には、以前に作成した「MY_BUDGET」予算が表示されます。
    それをクリックして予算ページを表示します
    
    画面右上の「<- 予算詳細」をクリックすると
    予算詳細パネルが表示されます。ここでは、予算とそれに付随するすべての
    リソースに関する情報を表示できます。監視されているリソースがないことがわかるので、今すぐ追加しましょう。
    「編集」ボタンをクリックして予算編集パネルを開きます；
    
    - 予算名は同じまま
    - 支出制限を100に設定
    - 以前に確認したメールを入力
    - リソースを追加するために「+ タグとリソース」ボタンをクリック
    - データベースを展開し、次にTB_101を展開し、ANALYTICSスキーマの横のボックスをチェック
    - 下にスクロールして「ウェアハウス」を展開
    - 「TB_DE_WH」のボックスをチェック
    - 「完了」をクリック
    - 予算編集メニューに戻り、「変更を保存」をクリック
*/

/*  7. ユニバーサル検索
    **************************************************************************
      ユーザーガイド                                                             
      https://docs.snowflake.com/en/user-guide/ui-snowsight-universal-search  
    **************************************************************************

    ユニバーサル検索を使用すると、アカウント内のあらゆるオブジェクトを簡単に見つけることができ、
    さらにMarketplaceのデータ製品、関連するSnowflakeドキュメント、コミュニティナレッジベース記事を
    探索できます。

    試してみましょう。 
    (ユニバーサル検索を使用する際、新規オブジェクトへのインデックス反映までは2-3時間かかる場合があります)

    - ユニバーサル検索を使用するには、ナビゲーションメニューの「検索」をクリックして開始
    - ここにユニバーサル検索UIが表示されます。最初の検索語句を入力しましょう。
    - 検索バーに「truck」と入力して結果を観察してください。上位セクションは、データベース、テーブル、
      ビュー、ステージなど、アカウント上の関連オブジェクトのカテゴリです。データベースオブジェクトの
      下には、関連するマーケットプレイスリストとドキュメントのセクションが表示されます。

    - 探しているものを説明するために自然言語で検索語句を提供することもできます。どのトラックフランチャイズが
    最も多くのリターン顧客を持っているかを回答するためにどこから調べ始めるかを知りたい場合、
    「どのトラックフランチャイズが最も忠実な顧客ベースを持っているか？」のような検索ができます。
    「テーブルとビュー」セクションの横にある「すべて表示 >」ボタンをクリックすると、
    クエリに関連するすべての関連テーブルとビューを表示できます。

    ユニバーサル検索は、異なるスキーマからいくつかのテーブルとビューを返します。各オブジェクトに対して
    関連する列がどのようにリストされているかにも注目してください。これらはすべて、リターン顧客に関する
    データ駆動の回答の優れた出発点です。
*/

-------------------------------------------------------------------------
--RESET--
-------------------------------------------------------------------------
-- 作成されたオブジェクトを削除
DROP RESOURCE MONITOR IF EXISTS my_resource_monitor;
DROP TABLE IF EXISTS raw_pos.truck_dev;

-- トラック詳細をリセット
CREATE OR REPLACE TABLE raw_pos.truck_details
AS 
SELECT * EXCLUDE (year, make, model)
FROM raw_pos.truck;

DROP WAREHOUSE IF EXISTS my_wh;
-- Unset Query Tag
ALTER SESSION UNSET query_tag;