# Streamlit in Snowflakeへようこそ！

# 必要なライブラリをインポート
# streamlitはWebアプリのインターフェース作成に使用されます。
import streamlit as st
# pandasはデータ操作と分析に使用されます。
import pandas as pd
# altairはインタラクティブなデータ可視化の作成に使用されます。
import altair as alt
# snowflake.snowpark.contextはSnowflakeに接続してアクティブセッションを取得するために使用されます。
from snowflake.snowpark.context import get_active_session

# --- アプリのセットアップとデータ読み込み ---

# Snowflakeとやり取りするためのアクティブなSnowparkセッションを取得します。
session = get_active_session()

# ページの上部に表示されるStreamlitアプリケーションのタイトルを設定します。
st.title("2022年2月のメニュー売上")

st.write('---') # 区切り線を作成します

# Snowflakeからデータを読み込む関数を定義します。
# @st.cache_dataはこの関数の出力をキャッシュするStreamlitデコレータです。
# これにより、データはSnowflakeから一度だけ取得され、その後の実行や
# ユーザーがウィジェットと操作する際のパフォーマンスが向上します。
@st.cache_data()
def load_data():
    """
    Snowflakeテーブルに接続し、データを取得してPandas DataFrameとして返します。
    """
    # アクティブセッションを使用してSnowflakeのテーブルを参照し、Pandas DataFrameに変換します。
    # 注：元の変数名'germany_sales_df'はコンテキストを考えると混乱を招く可能性がありました。

    japan_sales_df = session.table("tb_101.analytics.japan_menu_item_sales_feb_2022").to_pandas()
    return japan_sales_df

# データを読み込む関数を呼び出します。キャッシュのおかげで、初回実行後は高速になります。
japan_sales = load_data()


# --- ウィジェットによるユーザーインタラクション ---

# DataFrameからメニュー項目名のユニークなリストを取得し、ドロップダウンに使用します。
menu_item_names = japan_sales['MENU_ITEM_NAME'].unique().tolist()

# Streamlitのサイドバーまたはメインページにドロップダウンメニュー（selectbox）を作成します。
# ユーザーの選択は'selected_menu_item'変数に格納されます。
selected_menu_item = st.selectbox("メニューを選択してください", options=menu_item_names)


# --- データセットアップ ---

# メインのDataFrameをフィルタリングして、ユーザーが選択したメニュー項目に一致する行のみを含めます。
menu_item_sales = japan_sales[japan_sales['MENU_ITEM_NAME'] == selected_menu_item]

# フィルタリングされたデータを'DATE'でグループ化し、各日の'ORDER_TOTAL'の合計を計算します。
daily_totals = menu_item_sales.groupby('DATE')['ORDER_TOTAL'].sum().reset_index()


# --- チャートセットアップ ---

# 動的なy軸スケールを設定するために売上値の範囲を計算します。
min_value = daily_totals['ORDER_TOTAL'].min()
max_value = daily_totals['ORDER_TOTAL'].max()

# チャート上で最小値/最大値の上下に追加するマージンを計算します。
chart_margin = (max_value - min_value) / 2
y_margin_min = min_value - chart_margin
y_margin_max = max_value + chart_margin

# 線グラフを作成します。
chart = alt.Chart(daily_totals).mark_line(
    point=True,     
    tooltip=True
).encode(
    x=alt.X('DATE:T',
            axis=alt.Axis(title='Date', format='%b %d'),
            title='日付'),
    y=alt.Y('ORDER_TOTAL:Q',
            axis=alt.Axis(title='売上合計 ($)'), 
            title='日次売上合計',
# y軸のカスタムドメイン（範囲）を設定して、動的にパディングを追加します。
            scale=alt.Scale(domain=[y_margin_min, y_margin_max]))
).properties(
    title=f'{selected_menu_item}の日次売上合計',
    height=500
)


# --- チャートの表示 ---

# StreamlitアプリでAltairチャートをレンダリングします。
# 'use_container_width=True'により、チャートがコンテナの全幅に拡張されます。
st.altair_chart(chart, use_container_width=True)