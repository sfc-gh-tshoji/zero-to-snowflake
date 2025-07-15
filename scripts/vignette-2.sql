/***************************************************************************************************       
Asset:        Zero to Snowflake - Simple Data Pipeline
Version:      v1     
Copyright(c): 2025 Snowflake Inc. All rights reserved.
****************************************************************************************************

Zero to Snowflake - Simple Data Pipeline
1. 外部ステージからの取り込み
2. 半構造化データとVARIANTデータ型
3. 動的テーブル
4. 動的テーブルによるシンプルなパイプライン
5. 有向非循環グラフ（DAG）によるパイプライン可視化

****************************************************************************************************/

ALTER SESSION SET query_tag = '{"origin":"sf_sit-is","name":"tb_101_v2","version":{"major":1, "minor":1},"attributes":{"is_quickstart":0, "source":"tastybytes", "vignette": "data_pipeline"}}';

/*
    生のメニューデータでデータパイプラインを作成することを意図したTasteBytesデータエンジニアの
    役割を担うので、適切にコンテキストを設定しましょう。
*/
USE DATABASE tb_101;
USE ROLE tb_data_engineer;
USE WAREHOUSE tb_de_wh;

/*  1. 外部ステージからの取り込み
    ***************************************************************
    SQLリファレンス:
    https://docs.snowflake.com/en/sql-reference/sql/copy-into-table
    ***************************************************************

    現在、データはAmazon S3バケットにCSV形式で保存されています。この生のCSVデータを
    ステージにロードして、作業用のステージングテーブルにCOPYできるようにする必要があります。
    
    Snowflakeでは、ステージはデータファイルが保存される場所を指定する名前付きデータベースオブジェクトで、
    テーブルへのデータの読み込みとテーブルからのデータの書き出しを可能にします。

    ステージを作成するときに指定するもの:
        - データを取得するS3バケット
        - データを解析するファイル形式（この場合はCSV）
*/

-- menu_stage ステージを作成
CREATE OR REPLACE STAGE raw_pos.menu_stage
COMMENT = 'メニューデータ用ステージ'
URL = 's3://sfquickstarts/frostbyte_tastybytes/raw_pos/menu/'
FILE_FORMAT = public.csv_ff;

CREATE OR REPLACE TABLE raw_pos.menu_staging
(
    menu_id NUMBER(19,0),
    menu_type_id NUMBER(38,0),
    menu_type VARCHAR(16777216),
    truck_brand_name VARCHAR(16777216),
    menu_item_id NUMBER(38,0),
    menu_item_name VARCHAR(16777216),
    item_category VARCHAR(16777216),
    item_subcategory VARCHAR(16777216),
    cost_of_goods_usd NUMBER(38,4),
    sale_price_usd NUMBER(38,4),
    menu_item_health_metrics_obj VARIANT
);

-- ステージとテーブルが準備できたので、ステージから新しいmenu_stagingテーブルにデータをロードしましょう。
COPY INTO raw_pos.menu_staging
FROM @raw_pos.menu_stage;

-- オプション：ロードの成功を確認
SELECT * FROM raw_pos.menu_staging;

/*  2. Snowflakeでの半構造化データ
    *********************************************************************
    ユーザーガイド:
    https://docs.snowflake.com/en/sql-reference/data-types-semistructured
    *********************************************************************
    
    SnowflakeはVARIANTデータ型を使用してJSONなどの半構造化データの処理に優れています。
    このデータを自動的に解析、最適化、インデックス化し、ユーザーが標準SQLと専用関数を使用して
    簡単に抽出・分析できるようにします。Snowflakeは、JSON、Avro、ORC、Parquet、XMLなどの
    半構造化データ型をサポートしています。
    
    menu_item_health_metrics_obj列のVARIANTオブジェクトには、2つの主要なキー値ペアが含まれています：
        - menu_item_id: アイテムの一意識別子を表す数値
        - menu_item_health_metrics: 健康情報の詳細を保持するオブジェクトの配列
        
    menu_item_health_metrics配列内の各オブジェクトには：
        - 文字列の配列である原材料（ingredients）配列
        - 'Y'と'N'の文字列値を持つ複数の食事フラグ
*/
SELECT menu_item_health_metrics_obj FROM raw_pos.menu_staging;

/*
    このクエリは、データの内部のJSON様構造をナビゲートするために特別な構文を使用します。
    コロン演算子（:）はキー名でデータにアクセスし、角括弧（[]）は数値位置で配列から
    要素を選択します。これらの演算子は、ネストしたオブジェクトから原材料リストを抽出するために
    しばしば連鎖されます。
    
    VARIANTオブジェクトから取得された要素はVARIANT型のまま残ります。
    これらの要素を既知のデータ型にキャストすることで、クエリパフォーマンスが向上し、
    データ品質が向上します。キャストを実現する方法は2つあります：
        - CAST関数
        - 短縮構文の使用: <source_expr> :: <target_data_type>

    以下は、これらすべてのトピックを組み合わせて、メニュー項目名、メニュー項目ID、
    必要な原材料のリストを取得するクエリです。
*/
SELECT
    menu_item_name,
    CAST(menu_item_health_metrics_obj:menu_item_id AS INTEGER) AS menu_item_id, -- 'AS'を使用したキャスト
    menu_item_health_metrics_obj:menu_item_health_metrics[0]:ingredients::ARRAY AS ingredients -- ダブルコロン（::）構文を使用したキャスト
FROM raw_pos.menu_staging;

/*
    半構造化データを扱うときに活用できる別の強力な関数はFLATTENです。
    FLATTENにより、JSONや配列などの半構造化データを展開し、
    指定されたオブジェクト内のすべての要素に対して行を生成できます。

    これを使用して、トラックが使用するすべてのメニューからすべての原材料のリストを取得できます。
*/
SELECT
    i.value::STRING AS ingredient_name,
    m.menu_item_health_metrics_obj:menu_item_id::INTEGER AS menu_item_id
FROM
    raw_pos.menu_staging m,
    LATERAL FLATTEN(INPUT => m.menu_item_health_metrics_obj:menu_item_health_metrics[0]:ingredients::ARRAY) i;

/*  3. 動的テーブル
    **************************************************************
    ユーザーガイド:
    https://docs.snowflake.com/en/user-guide/dynamic-tables-about
    **************************************************************
    
    すべての原材料を構造化形式で保存して、個別に簡単にクエリ、フィルタリング、分析できれば良いでしょう。
    しかし、フードトラック事業者は新しくエキサイティングなメニュー項目を常にメニューに追加しており、
    その多くはデータベースにまだない独特な原材料を使用しています。
    
    これには、データ変換パイプラインを簡素化するために設計された強力なツールである動的テーブルを
    使用できます。動的テーブルは、いくつかの理由で使用事例に最適です：
        - 宣言的構文で作成され、指定されたクエリによってデータが定義されます。
        - 自動データ更新により、手動更新やカスタムスケジューリングなしにデータが新鮮に保たれます。
        - Snowflake動的テーブルによって管理されるデータの新鮮さは、動的テーブル自体だけでなく、
          それに依存する下流のデータオブジェクトにも及びます。

    これらの機能を実際に確認するために、シンプルな動的テーブルパイプラインを作成し、
    ステージングテーブルに新しいメニュー項目を追加して自動更新を実演します。

    原材料用の動的テーブルを作成することから始めます。
*/
CREATE OR REPLACE DYNAMIC TABLE harmonized.ingredient
    LAG = '1 minute'
    WAREHOUSE = 'TB_DE_WH'
AS
    SELECT
    ingredient_name,
    menu_ids
FROM (
    SELECT DISTINCT
        i.value::STRING AS ingredient_name, -- 重複しない原材料の値
        ARRAY_AGG(m.menu_item_id) AS menu_ids -- 原材料が使用されるメニューIDの配列
    FROM
        raw_pos.menu_staging m,
        LATERAL FLATTEN(INPUT => menu_item_health_metrics_obj:menu_item_health_metrics[0]:ingredients::ARRAY) i
    GROUP BY i.value::STRING
);

-- ingredient動的テーブルが正常に作成されたことを確認しましょう
SELECT * FROM harmonized.ingredient;

/*
    サンドイッチトラックの1つ「Better Off Bread」が新しいメニュー項目、バインミーサンドイッチを
    導入しました。このメニュー項目では、フランスパン、マヨネーズ、ピクルス大根などの新しい原材料が導入されます。
    
    動的テーブルの自動更新により、この新しいメニュー項目でmenu_stagingテーブルを更新すると、
    自動的にingredientテーブルに反映されます。
*/
INSERT INTO raw_pos.menu_staging 
SELECT 
    10101,
    15, -- トラックID
    'Sandwiches',
    'Better Off Bread', -- トラックブランド名
    157, -- メニュー項目ID
    'Banh Mi', -- メニュー項目名
    'Main',
    'Cold Option',
    9.0,
    12.0,
    PARSE_JSON('{
      "menu_item_health_metrics": [
        {
          "ingredients": [
            "French Baguette",
            "Mayonnaise",
            "Pickled Daikon",
            "Cucumber",
            "Pork Belly"
          ],
          "is_dairy_free_flag": "N",
          "is_gluten_free_flag": "N",
          "is_healthy_flag": "Y",
          "is_nut_free_flag": "Y"
        }
      ],
      "menu_item_id": 157
    }'
);

/*
    フランスパン、ピクルス大根が原材料テーブルに表示されていることを確認してください。
    「クエリは結果を生成しませんでした」と表示される場合があります。これは動的テーブルがまだ
    更新されていないことを意味します。動的テーブルのラグ設定が追いつくまで最大1分間お待ちください。
*/

SELECT * FROM harmonized.ingredient 
WHERE ingredient_name IN ('French Baguette', 'Pickled Daikon');

/* 4. 動的テーブルによるシンプルなパイプライン

    ingredient_to_menu_lookup動的テーブルを作成しましょう。これにより、
    特定の原材料を使用するメニュー項目を確認できます。その後、どのトラックにどの原材料が
    必要で、どのくらいの量が必要かを判断できます。このテーブルも動的テーブルなので、
    menu_stagingテーブルに追加されたメニュー項目で新しい原材料が使用された場合、
    自動的に更新されます。
*/
CREATE OR REPLACE DYNAMIC TABLE harmonized.ingredient_to_menu_lookup
    LAG = '1 minute'
    WAREHOUSE = 'TB_DE_WH'    
AS
SELECT
    i.ingredient_name,
    m.menu_item_health_metrics_obj:menu_item_id::INTEGER AS menu_item_id
FROM
    raw_pos.menu_staging m,
    LATERAL FLATTEN(INPUT => m.menu_item_health_metrics_obj:menu_item_health_metrics[0]:ingredients) f
JOIN harmonized.ingredient i ON f.value::STRING = i.ingredient_name;

-- ingredient_to_menu_lookup動的テーブルが正常に作成されたことを確認
SELECT * 
FROM harmonized.ingredient_to_menu_lookup
ORDER BY menu_item_id;

/*
    次の2つのinsertクエリを実行して、2022年1月27日にトラック#15で
    バインミーサンドイッチ2個の注文をシミュレートします。その後、
    トラック別の原材料使用量を示す別の下流動的テーブルを作成します。
*/
INSERT INTO raw_pos.order_header
SELECT 
    459520441, -- order_id
    15, -- トラックID
    1030, -- 位置ID
    101565,
    null,
    200322900,
    TO_TIMESTAMP_NTZ('08:00:00', 'hh:mi:ss'),
    TO_TIMESTAMP_NTZ('14:00:00', 'hh:mi:ss'),
    null,
    TO_TIMESTAMP_NTZ('2022-01-27 08:21:08.000'), -- 注文タイムスタンプ
    null,
    'USD',
    14.00,
    null,
    null,
    14.00;
    
INSERT INTO raw_pos.order_detail
SELECT
    904745311, -- 注文詳細ID
    459520441, -- 注文ID
    157, -- メニュー項目ID
    null,
    0,
    2, -- 注文数量
    14.00,
    28.00,
    null;

/*
    次に、米国の個々のフードトラックによる各原材料の月間使用量をまとめる別の動的テーブルを作成します。
    これにより、在庫最適化、コスト制御、メニュー計画とサプライヤー関係に関する情報に基づいた意思決定に
    重要な原材料消費を追跡できます。
    
    注文タイムスタンプから日付の部分を抽出するために使用される2つの異なる方法に注意してください：
      -> EXTRACT(<date part> FROM <datetime>)は、指定されたタイムスタンプから指定された日付部分を分離します。
      EXTRACT関数で使用できる日付と時刻の部分はいくつかあり、最も一般的なものはYEAR、MONTH、DAY、HOUR、MINUTE、SECONDです。
      -> MONTH(<datetime>)は1-12の月のインデックスを返します。YEAR(<datetime>)とDAY(<datetime>)は
      それぞれ年と日に対して同じことを行います。
*/

-- 次にテーブルを作成
CREATE OR REPLACE DYNAMIC TABLE harmonized.ingredient_usage_by_truck 
    LAG = '2 minute'
    WAREHOUSE = 'TB_DE_WH'  
    AS 
    SELECT
        oh.truck_id,
        EXTRACT(YEAR FROM oh.order_ts) AS order_year,
        MONTH(oh.order_ts) AS order_month,
        i.ingredient_name,
        SUM(od.quantity) AS total_ingredients_used
    FROM
        raw_pos.order_detail od
        JOIN raw_pos.order_header oh ON od.order_id = oh.order_id
        JOIN harmonized.ingredient_to_menu_lookup iml ON od.menu_item_id = iml.menu_item_id
        JOIN harmonized.ingredient i ON iml.ingredient_name = i.ingredient_name
        JOIN raw_pos.location l ON l.location_id = oh.location_id
    WHERE l.country = 'United States'
    GROUP BY
        oh.truck_id,
        order_year,
        order_month,
        i.ingredient_name
    ORDER BY
        oh.truck_id,
        total_ingredients_used DESC;
/*
    今度は、新しく作成したingredient_usage_by_truckビューを使用して、
    2022年1月のトラック#15の原材料使用量を表示しましょう。
*/
SELECT
    truck_id,
    ingredient_name,
    SUM(total_ingredients_used) AS total_ingredients_used,
FROM
    harmonized.ingredient_usage_by_truck
WHERE
    order_month = 1 -- 月は数値で1-12で表されます
    AND truck_id = 15
GROUP BY truck_id, ingredient_name
ORDER BY total_ingredients_used DESC;

/*  5. 有向非循環グラフ（DAG）によるパイプライン可視化

    最後に、パイプラインの有向非循環グラフ（DAG）を理解しましょう。
    DAGはデータパイプラインの可視化として機能します。これを使用して複雑なデータワークフローを視覚的に
    調整し、タスクが正しい順序で実行されることを確保できます。パイプライン内の各動的テーブルの
    ラグメトリックと設定を表示し、必要に応じてテーブルを手動で更新することもできます。

    DAGにアクセスするには：
    - ナビゲーションメニューの「データ」ボタンをクリックしてデータベース画面を開く
    - 「TB_101」の横の矢印「>」をクリックしてデータベースを展開
    - 「HARMONIZED」を展開し、次に「動的テーブル」を展開
    - 「INGREDIENT」テーブルをクリック
*/

-------------------------------------------------------------------------
--RESET--
-------------------------------------------------------------------------
USE ROLE accountadmin;
-- 動的テーブルを削除
DROP TABLE IF EXISTS raw_pos.menu_staging;
DROP TABLE IF EXISTS harmonized.ingredient;
DROP TABLE IF EXISTS harmonized.ingredient_to_menu_lookup;
DROP TABLE IF EXISTS harmonized.ingredient_usage_by_truck;

-- 挿入を削除
DELETE FROM raw_pos.order_detail
WHERE order_detail_id = 904745311;
DELETE FROM raw_pos.order_header
WHERE order_id = 459520441;

-- クエリタグを解除
ALTER SESSION UNSET query_tag;
-- Suspend warehouse
ALTER WAREHOUSE tb_de_wh SUSPEND;