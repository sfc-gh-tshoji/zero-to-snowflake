/*************************************************************************************************** 
Asset:        Zero to Snowflake - AISQL Functions
Version:      v1     
Copyright(c): 2025 Snowflake Inc. All rights reserved.
****************************************************************************************************

AISQL関数
1. SENTIMENT()を使用してトラックの顧客レビューをポジティブ、ネガティブ、ニュートラルとしてスコア化・ラベル付け
2. AI_CLASSIFY()を使用して食品品質やサービス体験などのテーマでレビューを分類
3. EXTRACT_ANSWER()を使用してレビューテキストから特定の苦情や称賛を抽出
4. AI_SUMMARIZE_AGG()を使用してトラックブランド名ごとの顧客センチメントの簡潔な要約を生成

****************************************************************************************************/

ALTER SESSION SET query_tag = '{"origin":"sf_sit-is","name":"tb_101_v2_aisql","version":{"major":1, "minor":1},"attributes":{"is_quickstart":0, "source":"tastybytes", "vignette": "aisql_functions"}}';

/*
    顧客レビューからインサイトを得るためにAISSQL関数を活用することを意図した
    TasteBytesデータアナリストの役割を担うので、適切にコンテキストを設定しましょう。
*/

USE ROLE tb_analyst;
USE DATABASE tb_101;
USE WAREHOUSE tb_analyst_wh;

/* 1. 大規模なセンチメント分析
    ***************************************************************
    すべてのフードトラックブランドの顧客センチメントを分析して、どのトラックが
    最高のパフォーマンスを発揮しているかを特定し、車両群全体の顧客満足度指標を作成します。
    Cortex Playgroundでは、個々のレビューを手動で分析しました。今度は
    SENTIMENT()関数を使用して、-1（ネガティブ）から+1（ポジティブ）まで、
    Snowflakeの公式センチメント範囲に従って顧客レビューを自動的にスコア化します。
    ***************************************************************/

-- ビジネス質問：「各トラックブランドに対して顧客は全体的にどのように感じているか？」
-- このクエリを実行してフードトラックネットワーク全体の顧客センチメントを分析し、フィードバックを分類してください

SELECT
    truck_brand_name,
    COUNT(*) AS total_reviews,
    AVG(CASE WHEN sentiment >= 0.5 THEN sentiment END) AS avg_positive_score,
    AVG(CASE WHEN sentiment BETWEEN -0.5 AND 0.5 THEN sentiment END) AS avg_neutral_score,
    AVG(CASE WHEN sentiment <= -0.5 THEN sentiment END) AS avg_negative_score
FROM (
    SELECT
        truck_brand_name,
        SNOWFLAKE.CORTEX.SENTIMENT (review) AS sentiment
    FROM harmonized.truck_reviews_v
    WHERE
        language ILIKE '%en%'
        AND review IS NOT NULL
    LIMIT 10000
)
GROUP BY
    truck_brand_name
ORDER BY total_reviews DESC;

/*
    主要なインサイト：
        Cortex Playgroundで一度に一つずつレビューを分析していたものから、
        数千件を体系的に処理することへの移行に注目してください。SENTIMENT()関数は                       
        すべてのレビューを自動的にスコア化し、ポジティブ、ネガティブ、ニュートラルに分類し、
        即座に車両群全体の顧客満足度指標を提供しました。
    センチメントスコア範囲：
        ポジティブ：  0.5 から 1
        ニュートラル： -0.5 から 0.5
        ネガティブ：  -0.5 から -1
*/

/* 2. 顧客フィードバックの分類
    ***************************************************************
    次に、すべてのレビューを分類して、顧客がサービスのどの側面について最も多く
    話しているかを理解しましょう。単純なキーワードマッチングではなく、AI理解に基づいて
    レビューをユーザー定義カテゴリに自動的に分類するAI_CLASSIFY()関数を使用します。
    この段階では、顧客フィードバックをビジネス関連の運営分野に分類し、
    それらの分布パターンを分析します。
    ***************************************************************/

-- ビジネス質問：「顧客は主に何についてコメントしているか - 食品品質、サービス、配達体験？」
-- 分類クエリを実行：

WITH classified_reviews AS (
  SELECT
    truck_brand_name,
    AI_CLASSIFY(
      review,
      ['Food Quality', 'Pricing', 'Service Experience', 'Staff Behavior']
    ):labels[0] AS feedback_category
  FROM
    harmonized.truck_reviews_v
  WHERE
    language ILIKE '%en%'
    AND review IS NOT NULL
    AND LENGTH(review) > 30
  LIMIT
    10000
)
SELECT
  truck_brand_name,
  feedback_category,
  COUNT(*) AS number_of_reviews
FROM
  classified_reviews
GROUP BY
  truck_brand_name,
  feedback_category
ORDER BY
  truck_brand_name,
  number_of_reviews DESC;
                
/*
    主要なインサイト：
        AI_CLASSIFY()が数千のレビューを食品品質、サービス体験などのビジネス関連テーマに
        自動的に分類したことを観察してください。食品品質がトラックブランド全体で最も議論される
        トピックであることがすぐにわかり、運営チームに顧客の優先事項についての明確で
        実行可能なインサイトを提供します。
*/

/* 3. 特定の運営インサイトの抽出
    ***************************************************************
    次に、非構造化テキストから正確な回答を得るために、EXTRACT_ANSWER()関数を
    活用します。この強力な関数により、顧客フィードバックについて特定のビジネス質問を
    投げかけ、直接的な回答を受け取ることができます。この段階では、顧客レビューで言及されている
    正確な運営上の問題を特定し、即座の注意が必要な特定の問題を強調することが目標です。
    ***************************************************************/

-- ビジネス質問：「各顧客レビュー内で言及されている特定の運営上の問題やポジティブな言及は何か？」
-- 次のクエリを実行しましょう：

  SELECT
    truck_brand_name,
    primary_city,
    LEFT(review, 100) || '...' AS review_preview,
    SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
        review,
        'What specific improvement or complaint is mentioned in this review?'
    ) AS specific_feedback
FROM 
    harmonized.truck_reviews_v
WHERE 
    language = 'en'
    AND review IS NOT NULL
    AND LENGTH(review) > 50
ORDER BY truck_brand_name, primary_city ASC
LIMIT 10000;

/*
    主要なインサイト：
        EXTRACT_ANSWER()が長い顧客レビューから具体的で実行可能なインサイトを
        抽出していることに注目してください。手動レビューではなく、この関数は
        「親切なスタッフが救いだった」や「ホットドッグは完璧に調理されている」などの
        具体的なフィードバックを自動的に特定します。結果は、密なテキストを運営チームが
        即座に活用できる具体的で引用可能なフィードバックに変換することです。
*/

/* 4. エグゼクティブサマリーの生成
    ***************************************************************
    最後に、顧客フィードバックの簡潔な要約を作成するために、SUMMARIZE()関数を使用します。
    この強力な関数は、長い非構造化テキストから短く一貫した要約を生成します。
    この段階では、各トラックブランドの顧客レビューの本質を消化しやすい要約に凝縮し、
    全体的なセンチメントとキーポイントの簡潔な概要を提供することが目標です。
    ***************************************************************/

-- ビジネス質問：「各トラックブランドのキーテーマと全体的なセンチメントは何か？」
-- 要約クエリを実行：

SELECT
  truck_brand_name,
  AI_SUMMARIZE_AGG (review) AS review_summary
FROM
  (
    SELECT
      truck_brand_name,
      review
    FROM
      harmonized.truck_reviews_v
    LIMIT
      100
  )
GROUP BY
  truck_brand_name;


/*
  主要なインサイト：
      AI_SUMMARIZE_AGG()関数は、長いレビューを明確なブランドレベルの要約に凝縮します。
      これらの要約は、反復テーマとセンチメントトレンドを強調し、決定者に各フードトラックの
      パフォーマンスの簡潔な概要を提供し、個々のレビューを読むことなく顧客認識をより迅速に
      理解できるようにします。
*/

/*************************************************************************************************** 
    私たちは、AI SQL関数の変革的な力を成功裏に実証し、顧客フィードバック分析を個々のレビュー処理から
    体系的で本格的な規模のインテリジェンスへと移行させました。これら4つのコア関数を通じた私たちの
    旅路は、それぞれが明確な分析目的を果たし、生の顧客の声を包括的なビジネスインテリジェンスに変換
    することを明確に示しています—体系的で、スケーラブルで、即座に実行可能です。かつて個々のレビュー分析を
    必要としていたものが、今では数秒で数千のレビューを処理し、データ駆動型の運営改善に不可欠な
    感情的コンテキストと具体的な詳細の両方を提供します。

    分析がより洗練されるにつれて、複雑なクエリで複数のAI関数を組み合わせることが困難になる場合があります。
    これはまさにSnowflake Copilotが強力な支援を提供する場面です。Copilotが自然言語を使用して
    複雑なクエリを作成し、作業を大幅に加速する方法を探ってみましょう。これにより、より複雑な
    分析ワークフローを簡単に構築できるようになります。
****************************************************************************************************/