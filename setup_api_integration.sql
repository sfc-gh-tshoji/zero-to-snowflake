-- コンテキスト設定
use role accountadmin;
use schema snowflake_learning_db.public; -- ここは自分のスキーマに変更してください

-- Git統合用のAPI統合
CREATE OR REPLACE API INTEGRATION handson_github_api_integration
   API_PROVIDER = git_https_api
   API_ALLOWED_PREFIXES = ('https://github.com/sfc-gh-tshoji/zero-to-snowflake')
   API_USER_AUTHENTICATION = (
      TYPE = snowflake_github_app
   )
   ENABLED = TRUE;