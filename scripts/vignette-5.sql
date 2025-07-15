/***************************************************************************************************       
Asset:        Zero to Snowflake - Getting Started with Snowflake
Version:      v1     
Copyright(c): 2025 Snowflake Inc. All rights reserved.
****************************************************************************************************

アプリケーションとコラボレーション
1. Snowflake Marketplaceからの気象データ取得
2. アカウントデータと気象ソースデータの統合
3. Safegraph POIデータの探索
4. SnowflakeでのStreamlitの紹介

****************************************************************************************************/

-- まず、セッションクエリタグを設定
ALTER SESSION SET query_tag = '{"origin":"sf_sit-is","name":"tb_101_v2","version":{"major":1, "minor":1},"attributes":{"is_quickstart":0, "source":"tastybytes", "vignette": "apps_and_collaboration"}}';

-- ワークシートコンテキストを設定
USE DATABASE tb_101;
USE ROLE accountadmin;
USE WAREHOUSE tb_de_wh;

/*  1. Snowflake Marketplaceからの気象データ取得
    ***********************************************************
    ユーザーガイド:
    https://docs.snowflake.com/en/user-guide/data-sharing-intro
    ***********************************************************
    ジュニアアナリストのBenは、天気が米国のフードトラック売上にどのような影響を与えるかについて
    より良い洞察を得たいと考えています。このために、Snowflake Marketplaceを使用してアカウントに
    気象データを追加し、独自のTasteBytesデータと照会して全く新しい洞察を発見します。
    
    Snowflake Marketplaceは、第三者のデータ、アプリケーション、AI製品の幅広い種類を発見し、
    アクセスできる中央ハブを提供します。この安全なデータ共有により、重複なしにライブで
    クエリ準備済みのデータにアクセスできるようになります。
    
    Weather Sourceデータを取得する手順：
    1. アカウントレベルからaccountadminを使用していることを確認してください（左下角を確認）。
    2. ナビゲーションメニューから「データ製品」ページに移動します。必要に応じて新しいブラウザタブで開くことができます。
    3. 検索バーに「Weather Source frostbyte」と入力します。
    4. 「Weather Source LLC: frostbyte」リストを選択し、「取得」をクリックします。
    5. 「オプション」をクリックしてオプションセクションを展開します。
    6. データベース名を「ZTS_WEATHERSOURCE」に変更します。
    7. 「PUBLIC」にアクセスを許可します。
    8. 「完了」を押します。
    
    このプロセスにより、Weather Sourceデータにほぼ瞬時にアクセスできるようになります。
    従来のデータ複製とパイプラインの必要性を排除することで、アナリストはビジネス質問から
    実行可能な分析に直接移行できます。
    
    気象データがアカウントに追加されたので、TasteBytesアナリストは既存の位置データと
    即座に結合を開始できます。
*/

-- アナリストロールに切り替え
USE ROLE tb_analyst;

/*  2. アカウントデータと気象ソースデータの統合

    Weather Sourceシェアからのデータで生の位置データを調和させる前に、データシェアを
    直接クエリして、作業しているデータの感覚を得ることができます。気象データで利用可能な
    すべての異なる都市のリストを、その特定の都市の気象指標とともに取得することから始めます。
*/
SELECT 
    DISTINCT city_name,
    AVG(max_wind_speed_100m_mph) AS avg_wind_speed_mph,
    AVG(avg_temperature_air_2m_f) AS avg_temp_f,
    AVG(tot_precipitation_in) AS avg_precipitation_in,
    MAX(tot_snowfall_in) AS max_snowfall_in
FROM zts_weathersource.onpoint_id.history_day
WHERE country = 'US'
GROUP BY city_name;

-- 次に、生の国データをWeather Sourceデータシェアの歴史的日次気象データと結合するビューを作成しましょう。
CREATE OR REPLACE VIEW harmonized.daily_weather_v
COMMENT = 'TastyBytesサポート都市にフィルタリングされたWeather Source日次履歴'
    AS
SELECT
    hd.*,
    TO_VARCHAR(hd.date_valid_std, 'YYYY-MM') AS yyyy_mm,
    pc.city_name AS city,
    c.country AS country_desc
FROM zts_weathersource.onpoint_id.history_day hd
JOIN zts_weathersource.onpoint_id.postal_codes pc
    ON pc.postal_code = hd.postal_code
    AND pc.country = hd.country
JOIN raw_pos.country c
    ON c.iso_country = hd.country
    AND c.city = hd.city_name;

/*
    日次気象履歴ビューを使用して、Benは2022年2月のハンブルクの平均日次気象温度を
    見つけて、線グラフとして可視化したいと考えています。

    結果ペインで「チャート」をクリックして結果をグラフィカルに可視化します。チャートビューで、
    「チャートタイプ」と表示されている左側のセクションで、以下の設定を構成してください：
    
        チャートタイプ：線グラフ | X軸：DATE_VALID_STD | Y軸：AVERAGE_TEMP_F
*/
SELECT
    dw.country_desc,
    dw.city_name,
    dw.date_valid_std,
    AVG(dw.avg_temperature_air_2m_f) AS average_temp_f
FROM harmonized.daily_weather_v dw
WHERE dw.country_desc = 'Germany'
    AND dw.city_name = 'Hamburg'
    AND YEAR(date_valid_std) = 2022
    AND MONTH(date_valid_std) = 2 -- 2月
GROUP BY dw.country_desc, dw.city_name, dw.date_valid_std
ORDER BY dw.date_valid_std DESC;

/*
    日次気象ビューは素晴らしく機能しています！さらに一歩進んで、注文ビューと
    日次気象ビューを組み合わせて、気象による日次売上ビューを作成しましょう。
    これにより、売上と気象条件との間のトレンドと関係を発見できます。
*/
CREATE OR REPLACE VIEW analytics.daily_sales_by_weather_v
COMMENT = '日次気象指標と注文データ'
AS
WITH daily_orders_aggregated AS (
    SELECT
        DATE(o.order_ts) AS order_date,
        o.primary_city,
        o.country,
        o.menu_item_name,
        SUM(o.price) AS total_sales
    FROM
        harmonized.orders_v o
    GROUP BY ALL
)
SELECT
    dw.date_valid_std AS date,
    dw.city_name,
    dw.country_desc,
    ZEROIFNULL(doa.total_sales) AS daily_sales,
    doa.menu_item_name,
    ROUND(dw.avg_temperature_air_2m_f, 2) AS avg_temp_fahrenheit,
    ROUND(dw.tot_precipitation_in, 2) AS avg_precipitation_inches,
    ROUND(dw.tot_snowdepth_in, 2) AS avg_snowdepth_inches,
    dw.max_wind_speed_100m_mph AS max_wind_speed_mph
FROM
    harmonized.daily_weather_v dw
LEFT JOIN
    daily_orders_aggregated doa
    ON dw.date_valid_std = doa.order_date
    AND dw.city_name = doa.primary_city
    AND dw.country_desc = doa.country
ORDER BY 
    date ASC;

/*
    Benは気象による日次売上ビューを使用して、天気が売上にどのような影響を与えるかを発見できるようになりました。
    これは以前は探索されていなかった関係で、シアトル市場で大雨が売上数値にどのような影響を与えるかなどの
    質問に答え始めることができます。

    チャートタイプ：棒グラフ | X軸：MENU_ITEM_NAME | Y軸：DAILY_SALES
*/
SELECT * EXCLUDE (city_name, country_desc, avg_snowdepth_inches, max_wind_speed_mph)
FROM analytics.daily_sales_by_weather_v
WHERE 
    country_desc = 'United States'
    AND city_name = 'Seattle'
    AND avg_precipitation_inches >= 1.0
ORDER BY date ASC;

/*  3. Safegraph POIデータの探索

    Benは、フードトラックの位置での気象条件についてより多くの洞察を得たいと考えています。
    幸い、SafegraphはSnowflake Marketplaceで無料のPOI（関心地点）データを提供しています。
    
    このデータリストを使用するために、以前の気象データと同様の手順に従います：
        1. アカウントレベルからaccountadminを使用していることを確認してください（左下角を確認）。
        2. ナビゲーションメニューから「データ製品」ページに移動します。必要に応じて新しいブラウザタブで開くことができます。
        3. 検索バーに「safegraph frostbyte」と入力します。
        4. 「Safegraph: frostbyte」リストを選択し、「取得」をクリックします。
        5. 「オプション」をクリックしてオプションセクションを展開します。
        6. データベース名：「ZTS_SAFEGRAPH」
        7. 「PUBLIC」にアクセスを許可します。
        8. 「完了」を押します。
    
    SafegraphのPOIデータをFrostbyteの気象データセットや独自の内部`orders_v`テーブルと結合することで、
    高リスクな位置を特定し、外的要因による財務的影響を定量化できます。
*/
CREATE OR REPLACE VIEW harmonized.tastybytes_poi_v
AS 
SELECT 
    l.location_id,
    sg.postal_code,
    sg.country,
    sg.city,
    sg.iso_country_code,
    sg.location_name,
    sg.top_category,
    sg.category_tags,
    sg.includes_parking_lot,
    sg.open_hours
FROM raw_pos.location l
JOIN zts_safegraph.public.frostbyte_tb_safegraph_s sg 
    ON l.location_id = sg.location_id
    AND l.iso_country_code = sg.iso_country_code;

-- POIデータを気象データと照会して、2022年の米国で平均して最も風の強い上位3つの位置を見つけましょう。
SELECT TOP 3
    p.location_id,
    p.city,
    p.postal_code,
    AVG(hd.max_wind_speed_100m_mph) AS average_wind_speed
FROM harmonized.tastybytes_poi_v AS p
JOIN
    zts_weathersource.onpoint_id.history_day AS hd
    ON p.postal_code = hd.postal_code
WHERE
    p.country = 'United States'
    AND YEAR(hd.date_valid_std) = 2022
GROUP BY p.location_id, p.city, p.postal_code
ORDER BY average_wind_speed DESC;

/*
    前回のクエリからのlocation_idsを使用して、異なる気象条件下での売上パフォーマンスを
    直接比較したいと思います。共通テーブル式（CTE）を使用して、上記のクエリをサブクエリとして
    使用し、最高の平均風速を持つ上位3つの位置を見つけ、それらの特定の位置の売上データを
    分析できます。共通テーブル式は、複雑なクエリをより小さな異なるクエリに分割して、
    可読性とパフォーマンスを向上させるのに役立ちます。
    
    各トラックブランドの売上データを2つのバケットに分割します：その日の最大風速が
    20mph以下だった「穏やか」な日と、20mphを超えた「風の強い」日です。

    これのビジネスへの影響は、ブランドの気象に対する耐性を特定することです。
    これらの売上数値を並べて見ることで、どのブランドが「気象耐性」があり、
    どのブランドが強風で売上が大幅に低下するかを即座に特定できます。
    これにより、脆弱なブランドの「風の強い日」プロモーションの実行、
    在庫の調整、ブランドのメニューを位置の典型的な気象により適合させる
    将来のトラック配置戦略の情報提供など、より良い情報に基づいた運営決定が可能になります。
*/
WITH TopWindiestLocations AS (
    SELECT TOP 3
        p.location_id
    FROM harmonized.tastybytes_poi_v AS p
    JOIN
        zts_weathersource.onpoint_id.history_day AS hd
        ON p.postal_code = hd.postal_code
    WHERE
        p.country = 'United States'
        AND YEAR(hd.date_valid_std) = 2022
    GROUP BY p.location_id, p.city, p.postal_code
    ORDER BY AVG(hd.max_wind_speed_100m_mph) DESC
)
SELECT
    o.truck_brand_name,
    ROUND(
        AVG(CASE WHEN hd.max_wind_speed_100m_mph <= 20 THEN o.order_total END),
    2) AS avg_sales_calm_days,
    ZEROIFNULL(ROUND(
        AVG(CASE WHEN hd.max_wind_speed_100m_mph > 20 THEN o.order_total END),
    2)) AS avg_sales_windy_days
FROM analytics.orders_v AS o
JOIN
    zts_weathersource.onpoint_id.history_day AS hd
    ON o.primary_city = hd.city_name
    AND DATE(o.order_ts) = hd.date_valid_std
WHERE o.location_id IN (SELECT location_id FROM TopWindiestLocations)
GROUP BY o.truck_brand_name
ORDER BY o.truck_brand_name;

/*----------------------------------------------------------------------------------
 Reset Script
----------------------------------------------------------------------------------*/
USE ROLE accountadmin;

-- ビューの削除
DROP VIEW IF EXISTS harmonized.daily_weather_v;
DROP VIEW IF EXISTS analytics.daily_sales_by_weather_v;
DROP VIEW IF EXISTS harmonized.tastybytes_poi_v;

-- クエリタグの解除
ALTER SESSION UNSET query_tag;
-- ウェアハウスの一時停止
ALTER WAREHOUSE tb_de_wh SUSPEND;