import streamlit as st
import altair as alt
import pandas as pd
from snowflake.snowpark.context import get_active_session

st.set_page_config(
    page_title="STADIA360 統合マーケティングダッシュボード",
    page_icon="📡",
    layout="wide",
)

st.title("STADIA360 統合マーケティングダッシュボード")
st.caption("電通 STADIA360 デモ｜テレビ実視聴データ×多様なKPIデータによる広告効果検証")

session = get_active_session()
DB_SCHEMA = "KFUKAMORI_GEN_DB.STADIA360"


# =====================================================
# データロード
# =====================================================
@st.cache_data(ttl=600)
def load_campaigns():
    return session.sql(f"SELECT * FROM {DB_SCHEMA}.CAMPAIGNS ORDER BY START_DATE").to_pandas()


@st.cache_data(ttl=600)
def load_tv_viewing():
    return session.sql(f"""
        SELECT VIEWING_ID, HOUSEHOLD_ID, CHANNEL, PROGRAM_NAME,
               DATE(VIEWING_START) AS VIEWING_DATE,
               HOUR(VIEWING_START) AS VIEWING_HOUR,
               DAYNAME(VIEWING_START) AS VIEWING_DOW,
               VIEWING_SECONDS, CM_EXPOSED, CAMPAIGN_ID, AREA, DEVICE_TYPE,
               CREATIVE_NAME
        FROM {DB_SCHEMA}.TV_VIEWING_LOG
    """).to_pandas()


@st.cache_data(ttl=600)
def load_cm_cv_joined():
    """CM接触ログとサイトCVを結合したデータ"""
    return session.sql(f"""
        SELECT
            tv.VIEWING_ID,
            tv.HOUSEHOLD_ID,
            tv.CHANNEL,
            tv.PROGRAM_NAME,
            tv.CREATIVE_NAME,
            DATE(tv.VIEWING_START) AS CM_DATE,
            HOUR(tv.VIEWING_START) AS CM_HOUR,
            DAYNAME(tv.VIEWING_START) AS CM_DOW,
            tv.CAMPAIGN_ID,
            tv.AREA,
            cl.CUSTOMER_ID,
            sv.CONVERSION_FLAG,
            DATE(sv.VISIT_TIMESTAMP) AS CV_DATE,
            DATEDIFF('day', DATE(tv.VIEWING_START), DATE(sv.VISIT_TIMESTAMP)) AS DAYS_TO_CV
        FROM {DB_SCHEMA}.TV_VIEWING_LOG tv
        JOIN {DB_SCHEMA}.CUSTOMER_LOYALTY cl ON tv.HOUSEHOLD_ID = cl.HOUSEHOLD_ID
        LEFT JOIN {DB_SCHEMA}.SITE_VISIT_LOG sv ON cl.CUSTOMER_ID = sv.CUSTOMER_ID
        WHERE tv.CM_EXPOSED = TRUE
    """).to_pandas()


@st.cache_data(ttl=600)
def load_loyalty():
    return session.sql(f"SELECT * FROM {DB_SCHEMA}.CUSTOMER_LOYALTY").to_pandas()


@st.cache_data(ttl=600)
def load_attitude():
    return session.sql(f"SELECT * FROM {DB_SCHEMA}.ATTITUDE_CHANGE").to_pandas()


@st.cache_data(ttl=600)
def load_site():
    return session.sql(f"""
        SELECT SESSION_ID, CUSTOMER_ID, DATE(VISIT_TIMESTAMP) AS VISIT_DATE,
               REFERRER_TYPE, REFERRER_DETAIL, PAGE_VIEWS, DURATION_SECONDS,
               CONVERSION_FLAG, CONVERSION_TYPE, CAMPAIGN_ID, DEVICE
        FROM {DB_SCHEMA}.SITE_VISIT_LOG
    """).to_pandas()


@st.cache_data(ttl=600)
def load_purchase():
    return session.sql(f"SELECT * FROM {DB_SCHEMA}.OFFLINE_PURCHASE").to_pandas()


@st.cache_data(ttl=600)
def load_store():
    return session.sql(f"SELECT * FROM {DB_SCHEMA}.STORE_VISIT_LOG").to_pandas()


@st.cache_data(ttl=600)
def load_app_dl():
    return session.sql(f"SELECT * FROM {DB_SCHEMA}.APP_DOWNLOAD_LOG").to_pandas()


@st.cache_data(ttl=600)
def load_app_launch():
    return session.sql(f"""
        SELECT LAUNCH_ID, CUSTOMER_ID, APP_NAME,
               DATE(LAUNCH_TIMESTAMP) AS LAUNCH_DATE,
               SESSION_SECONDS, FEATURES_USED, OS_TYPE, CAMPAIGN_ID
        FROM {DB_SCHEMA}.APP_LAUNCH_LOG
    """).to_pandas()


df_campaigns = load_campaigns()
df_tv = load_tv_viewing()
df_loyalty = load_loyalty()
df_attitude = load_attitude()
df_site = load_site()
df_purchase = load_purchase()
df_store = load_store()
df_app_dl = load_app_dl()
df_app_launch = load_app_launch()
df_cm_cv = load_cm_cv_joined()

# =====================================================
# サイドバーフィルター
# =====================================================
with st.sidebar:
    st.header("フィルター")

    campaign_options = ["全キャンペーン"] + df_campaigns["CAMPAIGN_NAME"].tolist()
    selected_campaign = st.selectbox("キャンペーン", campaign_options)

    area_options = sorted(df_tv["AREA"].unique())
    selected_areas = st.multiselect("エリア", area_options, default=area_options)

    st.markdown("---")
    st.markdown("**データ概要**")
    st.markdown(f"- TV視聴ログ: {len(df_tv):,}件")
    st.markdown(f"- 顧客数: {len(df_loyalty):,}件")
    st.markdown(f"- 態度変容: {len(df_attitude):,}件")
    st.markdown(f"- サイト来訪: {len(df_site):,}件")
    st.markdown(f"- オフライン購買: {len(df_purchase):,}件")
    st.markdown(f"- 来店: {len(df_store):,}件")
    st.markdown(f"- アプリDL: {len(df_app_dl):,}件")
    st.markdown(f"- アプリ起動: {len(df_app_launch):,}件")

# フィルター適用
campaign_id = None
if selected_campaign != "全キャンペーン":
    campaign_id = df_campaigns[df_campaigns["CAMPAIGN_NAME"] == selected_campaign]["CAMPAIGN_ID"].iloc[0]

def filter_by_campaign(df, col="CAMPAIGN_ID"):
    if campaign_id is None:
        return df
    return df[df[col] == campaign_id]

def filter_by_area(df, col="AREA"):
    if col in df.columns:
        return df[df[col].isin(selected_areas)]
    return df

df_tv_f = filter_by_area(filter_by_campaign(df_tv))
df_attitude_f = filter_by_campaign(df_attitude)
df_site_f = filter_by_campaign(df_site)
df_purchase_f = filter_by_area(filter_by_campaign(df_purchase), "STORE_AREA")
df_store_f = filter_by_area(filter_by_campaign(df_store), "STORE_AREA")
df_app_dl_f = filter_by_campaign(df_app_dl)
df_app_launch_f = filter_by_campaign(df_app_launch)
df_cm_cv_f = filter_by_area(filter_by_campaign(df_cm_cv))


# =====================================================
# 1. 概況サマリー（KPI）
# =====================================================
st.header("概況サマリー")

col1, col2, col3, col4 = st.columns(4)

with col1:
    cm_count = df_tv_f[df_tv_f["CM_EXPOSED"] == True].shape[0]
    total_tv = df_tv_f.shape[0]
    cm_rate = (cm_count / total_tv * 100) if total_tv > 0 else 0
    st.metric("CM接触件数", f"{cm_count:,}", f"接触率 {cm_rate:.1f}%")

with col2:
    cv_count = df_site_f[df_site_f["CONVERSION_FLAG"] == True].shape[0]
    total_sessions = df_site_f.shape[0]
    cvr = (cv_count / total_sessions * 100) if total_sessions > 0 else 0
    st.metric("サイトCV数", f"{cv_count:,}", f"CVR {cvr:.1f}%")

with col3:
    store_visits = df_store_f.shape[0]
    avg_stay = df_store_f["STAY_MINUTES"].mean() if store_visits > 0 else 0
    st.metric("来店件数", f"{store_visits:,}", f"平均滞在 {avg_stay:.0f}分")

with col4:
    dl_count = df_app_dl_f.shape[0]
    launch_count = df_app_launch_f.shape[0]
    st.metric("アプリDL数", f"{dl_count:,}", f"起動 {launch_count:,}回")

col5, col6, col7, col8 = st.columns(4)

with col5:
    total_purchase = df_purchase_f["AMOUNT"].sum()
    st.metric("購買総額", f"¥{total_purchase:,.0f}")

with col6:
    avg_nps = df_loyalty["NPS_SCORE"].mean()
    st.metric("平均NPS", f"{avg_nps:.1f}")

with col7:
    total_pv = df_site_f["PAGE_VIEWS"].sum()
    st.metric("合計PV", f"{total_pv:,}")

with col8:
    total_hours = df_tv_f["VIEWING_SECONDS"].sum() / 3600
    st.metric("総視聴時間", f"{total_hours:,.0f}時間")


# =====================================================
# 2. テレビ視聴分析
# =====================================================
st.header("テレビ視聴分析")

col_tv1, col_tv2 = st.columns(2)

with col_tv1:
    st.subheader("チャンネル別視聴件数")
    ch_data = df_tv_f.groupby("CHANNEL").agg(
        VIEWS=("VIEWING_ID", "count"),
        CM_EXPOSED=("CM_EXPOSED", "sum")
    ).reset_index()
    ch_data["CM接触率(%)"] = (ch_data["CM_EXPOSED"] / ch_data["VIEWS"] * 100).round(1)

    chart_ch = (
        alt.Chart(ch_data)
        .mark_bar(color="#1f77b4")
        .encode(
            x=alt.X("VIEWS:Q", title="視聴件数"),
            y=alt.Y("CHANNEL:N", title="チャンネル", sort="-x"),
            tooltip=["CHANNEL", alt.Tooltip("VIEWS:Q", format=","), alt.Tooltip("CM接触率(%):Q", format=".1f")],
        )
    )
    st.altair_chart(chart_ch, use_container_width=True)

with col_tv2:
    st.subheader("時間帯別視聴トレンド")
    hour_data = df_tv_f.groupby("VIEWING_HOUR").agg(
        VIEWS=("VIEWING_ID", "count")
    ).reset_index()

    chart_hour = (
        alt.Chart(hour_data)
        .mark_area(color="#ff7f0e", opacity=0.7, line=True)
        .encode(
            x=alt.X("VIEWING_HOUR:O", title="時間帯"),
            y=alt.Y("VIEWS:Q", title="視聴件数"),
            tooltip=["VIEWING_HOUR", alt.Tooltip("VIEWS:Q", format=",")],
        )
    )
    st.altair_chart(chart_hour, use_container_width=True)

# TV vs CTV 比較
st.subheader("TV vs CTV デバイス別視聴比較")
col_dev1, col_dev2 = st.columns(2)

with col_dev1:
    device_data = df_tv_f.groupby("DEVICE_TYPE").agg(
        VIEWS=("VIEWING_ID", "count")
    ).reset_index()
    chart_device = (
        alt.Chart(device_data)
        .mark_arc(innerRadius=50)
        .encode(
            theta=alt.Theta("VIEWS:Q"),
            color=alt.Color("DEVICE_TYPE:N", title="デバイス"),
            tooltip=["DEVICE_TYPE", alt.Tooltip("VIEWS:Q", format=",")],
        )
    )
    st.altair_chart(chart_device, use_container_width=True)

with col_dev2:
    cm_by_device = df_tv_f.groupby("DEVICE_TYPE").agg(
        TOTAL=("VIEWING_ID", "count"),
        CM=("CM_EXPOSED", "sum")
    ).reset_index()
    cm_by_device["CM接触率(%)"] = (cm_by_device["CM"] / cm_by_device["TOTAL"] * 100).round(1)
    st.dataframe(cm_by_device.rename(columns={
        "DEVICE_TYPE": "デバイス", "TOTAL": "視聴件数", "CM": "CM接触件数"
    }), use_container_width=True)


# =====================================================
# CM効果分析（レスポンス分析）
# =====================================================
st.header("CM効果分析（レスポンス分析）")
st.caption("CM接触者のサイトコンバージョン（CV）を軸にした効果検証")

# --- 放送局別ドリル ---
col_cm1, col_cm2 = st.columns(2)

with col_cm1:
    st.subheader("放送局別ドリル（CV率比較）")
    ch_cm = df_cm_cv_f.groupby("CHANNEL").agg(
        CM接触数=("VIEWING_ID", "count"),
        CV数=("CONVERSION_FLAG", "sum"),
    ).reset_index()
    ch_cm["CV率(%)"] = (ch_cm["CV数"] / ch_cm["CM接触数"] * 100).round(2)

    chart_ch_cv = (
        alt.Chart(ch_cm)
        .mark_bar(color="#1f77b4")
        .encode(
            x=alt.X("CV率(%):Q", title="CV率（%）"),
            y=alt.Y("CHANNEL:N", title="放送局", sort="-x"),
            tooltip=["CHANNEL",
                      alt.Tooltip("CM接触数:Q", format=","),
                      alt.Tooltip("CV数:Q", format=","),
                      alt.Tooltip("CV率(%):Q", format=".2f")],
        )
    )
    st.altair_chart(chart_ch_cv, use_container_width=True)

# --- クリエイティブドリル ---
with col_cm2:
    st.subheader("クリエイティブドリル（素材別CV率）")
    cr_cm = df_cm_cv_f[df_cm_cv_f["CREATIVE_NAME"].notna()].groupby("CREATIVE_NAME").agg(
        CM接触数=("VIEWING_ID", "count"),
        CV数=("CONVERSION_FLAG", "sum"),
    ).reset_index()
    cr_cm["CV率(%)"] = (cr_cm["CV数"] / cr_cm["CM接触数"] * 100).round(2)

    chart_cr_cv = (
        alt.Chart(cr_cm)
        .mark_bar(color="#ff7f0e")
        .encode(
            x=alt.X("CV率(%):Q", title="CV率（%）"),
            y=alt.Y("CREATIVE_NAME:N", title="CM素材", sort="-x"),
            tooltip=["CREATIVE_NAME",
                      alt.Tooltip("CM接触数:Q", format=","),
                      alt.Tooltip("CV数:Q", format=","),
                      alt.Tooltip("CV率(%):Q", format=".2f")],
        )
    )
    st.altair_chart(chart_cr_cv, use_container_width=True)

# --- レスポンスヒートマップ ---
st.subheader("レスポンスヒートマップ（曜日×時間帯別CV数）")

hm_channel = st.selectbox("放送局で絞り込み", ["全局"] + sorted(df_cm_cv_f["CHANNEL"].unique().tolist()), key="hm_ch")
df_hm = df_cm_cv_f[df_cm_cv_f["CONVERSION_FLAG"] == True].copy()
if hm_channel != "全局":
    df_hm = df_hm[df_hm["CHANNEL"] == hm_channel]

dow_order = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
dow_label = {"Mon": "月", "Tue": "火", "Wed": "水", "Thu": "木", "Fri": "金", "Sat": "土", "Sun": "日"}
df_hm["曜日"] = df_hm["CM_DOW"].map(dow_label)

hm_data = df_hm.groupby(["曜日", "CM_HOUR"]).agg(
    CV数=("VIEWING_ID", "count")
).reset_index()
hm_data.rename(columns={"CM_HOUR": "時間帯"}, inplace=True)

dow_jp_order = ["月", "火", "水", "木", "金", "土", "日"]

chart_hm = (
    alt.Chart(hm_data)
    .mark_rect()
    .encode(
        x=alt.X("曜日:N", title="曜日", sort=dow_jp_order),
        y=alt.Y("時間帯:O", title="時間帯", sort=list(range(24))),
        color=alt.Color("CV数:Q", title="CV数",
                        scale=alt.Scale(scheme="blues")),
        tooltip=["曜日", alt.Tooltip("時間帯:O"), alt.Tooltip("CV数:Q", format=",")],
    )
    .properties(height=400)
)
st.altair_chart(chart_hm, use_container_width=True)

# --- レスポンス番組ランキング ---
st.subheader("レスポンス番組ランキング")

pgm_data = df_cm_cv_f.groupby("PROGRAM_NAME").agg(
    CM接触数=("VIEWING_ID", "count"),
    CV数=("CONVERSION_FLAG", "sum"),
).reset_index()
pgm_data["CV率(%)"] = (pgm_data["CV数"] / pgm_data["CM接触数"] * 100).round(2)
pgm_data = pgm_data.sort_values("CV率(%)", ascending=False).reset_index(drop=True)
pgm_data.index = pgm_data.index + 1
pgm_data.index.name = "順位"

col_pgm1, col_pgm2 = st.columns([1, 1])

with col_pgm1:
    chart_pgm = (
        alt.Chart(pgm_data.reset_index())
        .mark_bar(color="#2ca02c")
        .encode(
            x=alt.X("CV率(%):Q", title="CV率（%）"),
            y=alt.Y("PROGRAM_NAME:N", title="番組名", sort="-x"),
            tooltip=["PROGRAM_NAME",
                      alt.Tooltip("CM接触数:Q", format=","),
                      alt.Tooltip("CV数:Q", format=","),
                      alt.Tooltip("CV率(%):Q", format=".2f")],
        )
    )
    st.altair_chart(chart_pgm, use_container_width=True)

with col_pgm2:
    st.dataframe(
        pgm_data.rename(columns={
            "PROGRAM_NAME": "番組名", "CM接触数": "CM接触数", "CV数": "CV数"
        })[["番組名", "CM接触数", "CV数", "CV率(%)"]],
        use_container_width=True,
    )

# --- リーセンシー（CM接触→CV日数分布） ---
st.subheader("リーセンシー（CM接触からCVまでの日数分布）")

df_recency = df_cm_cv_f[
    (df_cm_cv_f["CONVERSION_FLAG"] == True) &
    (df_cm_cv_f["DAYS_TO_CV"].notna()) &
    (df_cm_cv_f["DAYS_TO_CV"] >= 0) &
    (df_cm_cv_f["DAYS_TO_CV"] <= 30)
].copy()

recency_data = df_recency.groupby("DAYS_TO_CV").agg(
    CV件数=("VIEWING_ID", "count")
).reset_index()
recency_data.rename(columns={"DAYS_TO_CV": "経過日数"}, inplace=True)

chart_recency = (
    alt.Chart(recency_data)
    .mark_area(color="#9467bd", opacity=0.7, line=True)
    .encode(
        x=alt.X("経過日数:Q", title="CM接触からCVまでの経過日数",
                scale=alt.Scale(domain=[0, 30])),
        y=alt.Y("CV件数:Q", title="CV件数"),
        tooltip=[alt.Tooltip("経過日数:Q"), alt.Tooltip("CV件数:Q", format=",")],
    )
    .properties(height=300)
)
st.altair_chart(chart_recency, use_container_width=True)


# =====================================================
# 3. 態度変容ファネル分析
# =====================================================
st.header("態度変容ファネル分析")

col_att1, col_att2 = st.columns(2)

with col_att1:
    st.subheader("CM接触者 vs 非接触者 リフト比較")
    exposed = df_attitude_f[df_attitude_f["CM_EXPOSED"] == True]
    unexposed = df_attitude_f[df_attitude_f["CM_EXPOSED"] == False]

    lift_data = pd.DataFrame({
        "ステージ": ["認知", "興味", "検討", "購入意向"],
        "CM接触者リフト": [
            (exposed["AWARENESS_AFTER"] - exposed["AWARENESS_BEFORE"]).mean() if len(exposed) > 0 else 0,
            (exposed["INTEREST_AFTER"] - exposed["INTEREST_BEFORE"]).mean() if len(exposed) > 0 else 0,
            (exposed["CONSIDER_AFTER"] - exposed["CONSIDER_BEFORE"]).mean() if len(exposed) > 0 else 0,
            (exposed["PURCHASE_AFTER"] - exposed["PURCHASE_BEFORE"]).mean() if len(exposed) > 0 else 0,
        ],
        "非接触者リフト": [
            (unexposed["AWARENESS_AFTER"] - unexposed["AWARENESS_BEFORE"]).mean() if len(unexposed) > 0 else 0,
            (unexposed["INTEREST_AFTER"] - unexposed["INTEREST_BEFORE"]).mean() if len(unexposed) > 0 else 0,
            (unexposed["CONSIDER_AFTER"] - unexposed["CONSIDER_BEFORE"]).mean() if len(unexposed) > 0 else 0,
            (unexposed["PURCHASE_AFTER"] - unexposed["PURCHASE_BEFORE"]).mean() if len(unexposed) > 0 else 0,
        ],
    })

    lift_long = lift_data.melt(id_vars=["ステージ"], var_name="グループ", value_name="リフト（pt）")
    stage_order = ["認知", "興味", "検討", "購入意向"]

    chart_lift = (
        alt.Chart(lift_long)
        .mark_bar()
        .encode(
            x=alt.X("グループ:N", title=""),
            y=alt.Y("リフト（pt）:Q", title="平均リフト（ポイント）"),
            color=alt.Color("グループ:N", scale=alt.Scale(
                domain=["CM接触者リフト", "非接触者リフト"],
                range=["#2ca02c", "#d62728"]
            )),
            column=alt.Column("ステージ:N", title="ステージ", sort=stage_order),
            tooltip=["ステージ", "グループ", alt.Tooltip("リフト（pt）:Q", format=".2f")],
        )
        .properties(width=100)
    )
    st.altair_chart(chart_lift, use_container_width=True)

with col_att2:
    st.subheader("キャンペーン別 認知リフト")
    att_by_camp = df_attitude_f.merge(
        df_campaigns[["CAMPAIGN_ID", "CAMPAIGN_NAME"]], on="CAMPAIGN_ID", how="left"
    )
    camp_lift = att_by_camp.groupby("CAMPAIGN_NAME").apply(
        lambda x: pd.Series({
            "認知リフト": (x["AWARENESS_AFTER"] - x["AWARENESS_BEFORE"]).mean(),
            "購入リフト": (x["PURCHASE_AFTER"] - x["PURCHASE_BEFORE"]).mean(),
            "回答数": len(x),
        })
    ).reset_index()

    chart_camp_lift = (
        alt.Chart(camp_lift)
        .mark_bar(color="#1f77b4")
        .encode(
            x=alt.X("認知リフト:Q", title="平均認知リフト（pt）"),
            y=alt.Y("CAMPAIGN_NAME:N", title="キャンペーン", sort="-x"),
            tooltip=["CAMPAIGN_NAME", alt.Tooltip("認知リフト:Q", format=".2f"),
                      alt.Tooltip("購入リフト:Q", format=".2f"), alt.Tooltip("回答数:Q", format=",")],
        )
    )
    st.altair_chart(chart_camp_lift, use_container_width=True)


# =====================================================
# 4. サイト来訪分析
# =====================================================
st.header("サイト来訪分析")

col_s1, col_s2 = st.columns(2)

with col_s1:
    st.subheader("流入経路別セッション数・CVR")
    ref_data = df_site_f.groupby("REFERRER_TYPE").agg(
        SESSIONS=("SESSION_ID", "count"),
        CVS=("CONVERSION_FLAG", "sum"),
        AVG_PV=("PAGE_VIEWS", "mean"),
    ).reset_index()
    ref_data["CVR(%)"] = (ref_data["CVS"] / ref_data["SESSIONS"] * 100).round(1)
    ref_data["AVG_PV"] = ref_data["AVG_PV"].round(1)

    chart_ref = (
        alt.Chart(ref_data)
        .mark_bar(color="#1f77b4")
        .encode(
            x=alt.X("SESSIONS:Q", title="セッション数"),
            y=alt.Y("REFERRER_TYPE:N", title="流入経路", sort="-x"),
            tooltip=["REFERRER_TYPE", alt.Tooltip("SESSIONS:Q", format=","),
                      alt.Tooltip("CVR(%):Q", format=".1f"), alt.Tooltip("AVG_PV:Q", format=".1f")],
        )
    )
    st.altair_chart(chart_ref, use_container_width=True)

with col_s2:
    st.subheader("月別セッション数・CV推移")
    df_site_f_copy = df_site_f.copy()
    df_site_f_copy["MONTH"] = pd.to_datetime(df_site_f_copy["VISIT_DATE"]).dt.to_period("M").astype(str)
    monthly_site = df_site_f_copy.groupby("MONTH").agg(
        SESSIONS=("SESSION_ID", "count"),
        CVS=("CONVERSION_FLAG", "sum"),
    ).reset_index()

    base = alt.Chart(monthly_site).encode(x=alt.X("MONTH:N", title="月"))
    bar = base.mark_bar(color="#1f77b4", opacity=0.6).encode(
        y=alt.Y("SESSIONS:Q", title="セッション数"),
        tooltip=["MONTH", alt.Tooltip("SESSIONS:Q", format=",")],
    )
    line = base.mark_line(color="#d62728", strokeWidth=2, point=True).encode(
        y=alt.Y("CVS:Q", title="CV数"),
        tooltip=["MONTH", alt.Tooltip("CVS:Q", format=",")],
    )
    chart_monthly = alt.layer(bar, line).resolve_scale(y="independent")
    st.altair_chart(chart_monthly, use_container_width=True)


# =====================================================
# 5. オフライン購買分析
# =====================================================
st.header("オフライン購買分析")

col_p1, col_p2 = st.columns(2)

with col_p1:
    st.subheader("商品カテゴリ別購買額")
    cat_data = df_purchase_f.groupby("PRODUCT_CATEGORY").agg(
        TOTAL_AMOUNT=("AMOUNT", "sum"),
        COUNT=("PURCHASE_ID", "count"),
    ).reset_index()
    cat_data["AVG_AMOUNT"] = (cat_data["TOTAL_AMOUNT"] / cat_data["COUNT"]).round(0)

    chart_cat = (
        alt.Chart(cat_data)
        .mark_bar(color="#2ca02c")
        .encode(
            x=alt.X("TOTAL_AMOUNT:Q", title="購買総額（円）"),
            y=alt.Y("PRODUCT_CATEGORY:N", title="カテゴリ", sort="-x"),
            tooltip=["PRODUCT_CATEGORY", alt.Tooltip("TOTAL_AMOUNT:Q", format=","),
                      alt.Tooltip("COUNT:Q", format=","), alt.Tooltip("AVG_AMOUNT:Q", format=",")],
        )
    )
    st.altair_chart(chart_cat, use_container_width=True)

with col_p2:
    st.subheader("CM接触者 vs 非接触者 購買比較")
    cm_purchase = df_purchase_f.groupby("CM_EXPOSED").agg(
        AVG_AMOUNT=("AMOUNT", "mean"),
        COUNT=("PURCHASE_ID", "count"),
        TOTAL=("AMOUNT", "sum"),
    ).reset_index()
    cm_purchase["グループ"] = cm_purchase["CM_EXPOSED"].map({True: "CM接触者", False: "非接触者"})
    cm_purchase["AVG_AMOUNT"] = cm_purchase["AVG_AMOUNT"].round(0)

    chart_cm_purchase = (
        alt.Chart(cm_purchase)
        .mark_bar()
        .encode(
            x=alt.X("グループ:N", title=""),
            y=alt.Y("AVG_AMOUNT:Q", title="平均購買金額（円）"),
            color=alt.Color("グループ:N", scale=alt.Scale(
                domain=["CM接触者", "非接触者"],
                range=["#2ca02c", "#d62728"]
            )),
            tooltip=["グループ", alt.Tooltip("AVG_AMOUNT:Q", format=","),
                      alt.Tooltip("COUNT:Q", format=","), alt.Tooltip("TOTAL:Q", format=",")],
        )
    )
    st.altair_chart(chart_cm_purchase, use_container_width=True)


# =====================================================
# 6. 来店・アプリ分析
# =====================================================
st.header("来店・アプリ分析")

col_a1, col_a2 = st.columns(2)

with col_a1:
    st.subheader("店舗別来店件数")
    store_data = df_store_f.groupby("STORE_NAME").agg(
        VISITS=("VISIT_ID", "count"),
        AVG_STAY=("STAY_MINUTES", "mean"),
    ).reset_index()
    store_data["AVG_STAY"] = store_data["AVG_STAY"].round(1)

    chart_store = (
        alt.Chart(store_data)
        .mark_bar(color="#9467bd")
        .encode(
            x=alt.X("VISITS:Q", title="来店件数"),
            y=alt.Y("STORE_NAME:N", title="店舗", sort="-x"),
            tooltip=["STORE_NAME", alt.Tooltip("VISITS:Q", format=","),
                      alt.Tooltip("AVG_STAY:Q", format=".1f")],
        )
    )
    st.altair_chart(chart_store, use_container_width=True)

with col_a2:
    st.subheader("アプリ別DL数・起動数")
    dl_by_app = df_app_dl_f.groupby("APP_NAME").agg(DL=("DOWNLOAD_ID", "count")).reset_index()
    launch_by_app = df_app_launch_f.groupby("APP_NAME").agg(LAUNCH=("LAUNCH_ID", "count")).reset_index()
    app_data = dl_by_app.merge(launch_by_app, on="APP_NAME", how="outer").fillna(0)
    app_data["LAUNCH"] = app_data["LAUNCH"].astype(int)
    app_data["DL"] = app_data["DL"].astype(int)

    app_long = app_data.melt(id_vars=["APP_NAME"], var_name="指標", value_name="件数")

    chart_app = (
        alt.Chart(app_long)
        .mark_bar()
        .encode(
            x=alt.X("指標:N", title=""),
            y=alt.Y("件数:Q", title="件数"),
            color=alt.Color("指標:N", scale=alt.Scale(
                domain=["DL", "LAUNCH"],
                range=["#1f77b4", "#ff7f0e"]
            )),
            column=alt.Column("APP_NAME:N", title="アプリ"),
            tooltip=["APP_NAME", "指標", alt.Tooltip("件数:Q", format=",")],
        )
        .properties(width=100)
    )
    st.altair_chart(chart_app, use_container_width=True)

# アプリDLチャネル分析
st.subheader("アプリDL 広告チャネル別")
col_ch1, col_ch2 = st.columns(2)

with col_ch1:
    channel_data = df_app_dl_f.groupby("AD_CHANNEL").agg(
        DL=("DOWNLOAD_ID", "count")
    ).reset_index()
    chart_channel = (
        alt.Chart(channel_data)
        .mark_arc(innerRadius=50)
        .encode(
            theta=alt.Theta("DL:Q"),
            color=alt.Color("AD_CHANNEL:N", title="チャネル"),
            tooltip=["AD_CHANNEL", alt.Tooltip("DL:Q", format=",")],
        )
    )
    st.altair_chart(chart_channel, use_container_width=True)

with col_ch2:
    os_data = df_app_dl_f.groupby("OS_TYPE").agg(DL=("DOWNLOAD_ID", "count")).reset_index()
    chart_os = (
        alt.Chart(os_data)
        .mark_arc(innerRadius=50)
        .encode(
            theta=alt.Theta("DL:Q"),
            color=alt.Color("OS_TYPE:N", title="OS",
                            scale=alt.Scale(domain=["iOS", "Android"], range=["#636363", "#2ca02c"])),
            tooltip=["OS_TYPE", alt.Tooltip("DL:Q", format=",")],
        )
    )
    st.altair_chart(chart_os, use_container_width=True)


# =====================================================
# 7. 顧客ロイヤリティ分析
# =====================================================
st.header("顧客ロイヤリティ分析")

col_l1, col_l2 = st.columns(2)

with col_l1:
    st.subheader("ロイヤリティセグメント分布")
    seg_data = df_loyalty.groupby("LOYALTY_SEGMENT").agg(
        COUNT=("CUSTOMER_ID", "count"),
        AVG_NPS=("NPS_SCORE", "mean"),
        AVG_LTV=("LTV_AMOUNT", "mean"),
    ).reset_index()
    seg_data["AVG_NPS"] = seg_data["AVG_NPS"].round(1)
    seg_data["AVG_LTV"] = seg_data["AVG_LTV"].round(0)

    chart_seg = (
        alt.Chart(seg_data)
        .mark_bar()
        .encode(
            x=alt.X("LOYALTY_SEGMENT:N", title="セグメント",
                     sort=["プロモーター", "パッシブ", "デトラクター"]),
            y=alt.Y("COUNT:Q", title="顧客数"),
            color=alt.Color("LOYALTY_SEGMENT:N", scale=alt.Scale(
                domain=["プロモーター", "パッシブ", "デトラクター"],
                range=["#2ca02c", "#ff7f0e", "#d62728"]
            ), legend=None),
            tooltip=["LOYALTY_SEGMENT", alt.Tooltip("COUNT:Q", format=","),
                      alt.Tooltip("AVG_NPS:Q", format=".1f"), alt.Tooltip("AVG_LTV:Q", format=",")],
        )
    )
    st.altair_chart(chart_seg, use_container_width=True)

with col_l2:
    st.subheader("年齢層 × 性別 分布")
    age_gender = df_loyalty.groupby(["AGE_GROUP", "GENDER"]).agg(
        COUNT=("CUSTOMER_ID", "count")
    ).reset_index()
    age_order = ["10代", "20代", "30代", "40代", "50代", "60代"]

    chart_age = (
        alt.Chart(age_gender)
        .mark_bar()
        .encode(
            x=alt.X("GENDER:N", title=""),
            y=alt.Y("COUNT:Q", title="顧客数"),
            color=alt.Color("GENDER:N", title="性別", scale=alt.Scale(
                domain=["男性", "女性"],
                range=["#1f77b4", "#e377c2"]
            )),
            column=alt.Column("AGE_GROUP:N", title="年齢層", sort=age_order),
            tooltip=["AGE_GROUP", "GENDER", alt.Tooltip("COUNT:Q", format=",")],
        )
        .properties(width=80)
    )
    st.altair_chart(chart_age, use_container_width=True)

# エリア別NPS
st.subheader("エリア別 平均NPS・平均LTV")
area_loyalty = df_loyalty[df_loyalty["AREA"].isin(selected_areas)].groupby("AREA").agg(
    AVG_NPS=("NPS_SCORE", "mean"),
    AVG_LTV=("LTV_AMOUNT", "mean"),
    COUNT=("CUSTOMER_ID", "count"),
).reset_index()
area_loyalty["AVG_NPS"] = area_loyalty["AVG_NPS"].round(1)
area_loyalty["AVG_LTV"] = area_loyalty["AVG_LTV"].round(0)

st.dataframe(
    area_loyalty.rename(columns={
        "AREA": "エリア", "AVG_NPS": "平均NPS", "AVG_LTV": "平均LTV（円）", "COUNT": "顧客数"
    }),
    use_container_width=True,
)
