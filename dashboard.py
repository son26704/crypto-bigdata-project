import streamlit as st
import pandas as pd
import numpy as np
import psycopg2
import plotly.graph_objects as go
from datetime import timedelta
from streamlit_autorefresh import st_autorefresh

# =========================
# PAGE CONFIG
# =========================
st.set_page_config(page_title="Crypto Dashboard", page_icon="💎", layout="wide")

DB_CONFIG = {
    "dbname": "cryptodb",
    "user": "cryptouser",
    "password": "cryptopass123",
    "host": "localhost",
    "port": "5433",
}

# =========================
# DB CONNECTION
# =========================
@st.cache_resource
def get_conn():
    return psycopg2.connect(**DB_CONFIG)

def _read_sql(query: str, params=None) -> pd.DataFrame:
    conn = get_conn()
    return pd.read_sql(query, conn, params=params)

# =========================
# DATA QUERIES
# =========================
@st.cache_data(ttl=600)
def get_coin_list():
    return _read_sql(
        """
        SELECT DISTINCT symbol, name, image
        FROM realtime_prices
        ORDER BY symbol
        """
    )

@st.cache_data(ttl=30)
def get_latest_timestamp(symbol: str):
    df = _read_sql(
        """
        SELECT MAX(timestamp) AS max_ts
        FROM realtime_prices
        WHERE symbol = %s
        """,
        params=(symbol,),
    )
    if df.empty or pd.isna(df.loc[0, "max_ts"]):
        return None
    return pd.to_datetime(df.loc[0, "max_ts"])

@st.cache_data(ttl=30)
def get_realtime_data(symbol: str, start_ts=None, end_ts=None, limit: int = 5000) -> pd.DataFrame:
    if symbol is None:
        return pd.DataFrame()

    where = ["symbol = %s"]
    params = [symbol]

    if start_ts is not None:
        where.append("timestamp >= %s")
        params.append(start_ts)
    if end_ts is not None:
        where.append("timestamp <= %s")
        params.append(end_ts)

    where_sql = " AND ".join(where)

    df = _read_sql(
        f"""
        SELECT
            symbol, name, image,
            price, market_cap, volume_24h,
            high_24h, low_24h, ath, atl,
            price_change_percentage_24h,
            ma_5min, ma_15min, ma_1hour,
            timestamp
        FROM realtime_prices
        WHERE {where_sql}
        ORDER BY timestamp ASC
        LIMIT %s
        """,
        params=tuple(params + [limit]),
    )

    if df.empty:
        return df

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    # đảm bảo số liệu vẽ plotly ổn định (NUMERIC/Decimal -> float)
    numeric_cols = [
        "price", "market_cap", "volume_24h",
        "high_24h", "low_24h", "ath", "atl",
        "price_change_percentage_24h",
        "ma_5min", "ma_15min", "ma_1hour",
    ]
    for c in numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    return df.sort_values("timestamp")

@st.cache_data(ttl=120)
def get_hourly_stats(symbol: str, limit: int = 168):
    df = _read_sql(
        """
        SELECT
            hour_timestamp, open_price, high_price, low_price, close_price,
            total_volume, price_volatility, record_count
        FROM hourly_stats
        WHERE symbol = %s
        ORDER BY hour_timestamp DESC
        LIMIT %s
        """,
        params=(symbol, limit),
    )
    if not df.empty:
        df["hour_timestamp"] = pd.to_datetime(df["hour_timestamp"])
        for c in ["open_price", "high_price", "low_price", "close_price", "total_volume", "price_volatility"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

@st.cache_data(ttl=30)
def get_alerts(symbol: str, limit: int = 80):
    df = _read_sql(
        """
        SELECT alert_type, message, price_after, change_percentage, timestamp
        FROM alerts
        WHERE symbol = %s
        ORDER BY timestamp DESC
        LIMIT %s
        """,
        params=(symbol, limit),
    )
    if not df.empty:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        for c in ["price_after", "change_percentage"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

# =========================
# GAP HANDLING (break lines)
# =========================
def insert_gap_breaks(df: pd.DataFrame, time_col: str = "timestamp", gap_minutes: int = 10) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    dfx = df.copy().sort_values(time_col).reset_index(drop=True)
    threshold = pd.Timedelta(minutes=gap_minutes)

    gap_rows = []
    for i in range(1, len(dfx)):
        dt = dfx.loc[i, time_col] - dfx.loc[i - 1, time_col]
        if dt > threshold:
            mid_time = dfx.loc[i - 1, time_col] + (dt / 2)
            row = {c: np.nan for c in dfx.columns}
            row[time_col] = mid_time
            gap_rows.append(row)

    if not gap_rows:
        return dfx

    out = pd.concat([dfx, pd.DataFrame(gap_rows)], ignore_index=True)
    return out.sort_values(time_col).reset_index(drop=True)

# =========================
# CHARTS
# =========================
def _common_xaxis():
    return dict(
        title="Thời gian",
        type="date",
    )

def create_price_chart(df1, sym1, df2=None, sym2=None, show_ma=True):
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df1["timestamp"], y=df1["price"], mode="lines",
                             name=f"Price ({sym1})", connectgaps=False))

    if df2 is not None and sym2:
        fig.add_trace(go.Scatter(x=df2["timestamp"], y=df2["price"], mode="lines",
                                 name=f"Price ({sym2})", connectgaps=False))

    if show_ma and "ma_5min" in df1.columns and df1["ma_5min"].notna().any():
        fig.add_trace(go.Scatter(x=df1["timestamp"], y=df1["ma_5min"], mode="lines",
                                 name=f"MA 5m ({sym1})", visible="legendonly",
                                 line=dict(dash="dot"), connectgaps=False))
    if show_ma and "ma_15min" in df1.columns and df1["ma_15min"].notna().any():
        fig.add_trace(go.Scatter(x=df1["timestamp"], y=df1["ma_15min"], mode="lines",
                                 name=f"MA 15m ({sym1})", visible="legendonly",
                                 line=dict(dash="dot"), connectgaps=False))
    if show_ma and "ma_1hour" in df1.columns and df1["ma_1hour"].notna().any():
        fig.add_trace(go.Scatter(x=df1["timestamp"], y=df1["ma_1hour"], mode="lines",
                                 name=f"MA 1h ({sym1})", visible="legendonly",
                                 line=dict(dash="dot"), connectgaps=False))

    fig.update_layout(
        template="plotly_dark",
        height=420,
        title="Realtime Price",
        hovermode="x unified",
        xaxis=_common_xaxis(),
        yaxis=dict(title="USD"),
        margin=dict(l=10, r=10, t=55, b=10),
        legend=dict(orientation="h", y=1.02, x=1, xanchor="right"),
    )
    return fig

def create_volume_chart(df1, sym1, df2=None, sym2=None):
    fig = go.Figure()
    fig.add_trace(go.Bar(x=df1["timestamp"], y=df1["volume_24h"], name=f"Volume ({sym1})", opacity=0.75))
    if df2 is not None and sym2:
        fig.add_trace(go.Bar(x=df2["timestamp"], y=df2["volume_24h"], name=f"Volume ({sym2})", opacity=0.55))

    fig.update_layout(
        barmode="overlay",
        template="plotly_dark",
        height=360,
        title="Volume (24h)",
        hovermode="x unified",
        xaxis=_common_xaxis(),
        yaxis=dict(title="Volume"),
        margin=dict(l=10, r=10, t=55, b=10),
        legend=dict(orientation="h", y=1.02, x=1, xanchor="right"),
    )
    return fig

def create_hourly_candlestick(ohlc: pd.DataFrame):
    fig = go.Figure()
    fig.add_trace(go.Candlestick(
        x=ohlc["hour_timestamp"],
        open=ohlc["open_price"],
        high=ohlc["high_price"],
        low=ohlc["low_price"],
        close=ohlc["close_price"],
        name="Hourly"
    ))
    fig.update_layout(
        template="plotly_dark",
        height=420,
        title="Hourly Candlestick",
        xaxis=_common_xaxis(),
        yaxis=dict(title="USD"),
        margin=dict(l=10, r=10, t=55, b=10),
    )
    return fig

# =========================
# SIDEBAR
# =========================
st.sidebar.title("🎛️ Điều khiển")

refresh_on = st.sidebar.toggle("Auto refresh", value=True)
if refresh_on:
    st_autorefresh(interval=30_000, key="datarefresh")

coins = get_coin_list()
if coins.empty:
    st.warning("Chưa có dữ liệu realtime. Hãy bật Producer/Streaming trước.")
    st.stop()

symbols = list(coins["symbol"].unique())

c1, c2 = st.sidebar.columns(2)
selected_symbol1 = c1.selectbox("Coin", symbols, key="coin1")
selected_symbol2 = c2.selectbox("So sánh", [None] + symbols, key="coin2", index=0)
if selected_symbol1 == selected_symbol2:
    selected_symbol2 = None

st.sidebar.divider()

latest_ts = get_latest_timestamp(selected_symbol1)
if latest_ts is None:
    st.warning(f"Chưa có dữ liệu cho {selected_symbol1}.")
    st.stop()

range_mode = st.sidebar.selectbox(
    "Khoảng thời gian",
    ["30 phút", "1 giờ", "6 giờ", "24 giờ", "7 ngày", "Tùy chọn", "Tất cả"],
    index=2
)

gap_minutes = st.sidebar.slider("Ngắt khi mất dữ liệu (phút)", 2, 60, 10, 1)
max_rows = st.sidebar.slider("Giới hạn số điểm", 200, 20000, 5000, 200)
show_ma = st.sidebar.toggle("MA (ẩn/hiện bằng legend)", value=True)

start_ts, end_ts = None, None

def _lookback(latest, delta):
    return latest - delta, latest

if range_mode == "30 phút":
    start_ts, end_ts = _lookback(latest_ts, timedelta(minutes=30))
elif range_mode == "1 giờ":
    start_ts, end_ts = _lookback(latest_ts, timedelta(hours=1))
elif range_mode == "6 giờ":
    start_ts, end_ts = _lookback(latest_ts, timedelta(hours=6))
elif range_mode == "24 giờ":
    start_ts, end_ts = _lookback(latest_ts, timedelta(days=1))
elif range_mode == "7 ngày":
    start_ts, end_ts = _lookback(latest_ts, timedelta(days=7))
elif range_mode == "Tùy chọn":
    slider_min = (latest_ts - timedelta(days=30)).to_pydatetime()
    slider_max = latest_ts.to_pydatetime()
    picked = st.sidebar.slider(
        "Chọn thời gian",
        min_value=slider_min,
        max_value=slider_max,
        value=((latest_ts - timedelta(hours=6)).to_pydatetime(), latest_ts.to_pydatetime()),
        step=timedelta(minutes=5),
    )
    start_ts, end_ts = pd.to_datetime(picked[0]), pd.to_datetime(picked[1])
elif range_mode == "Tất cả":
    start_ts, end_ts = None, None

# =========================
# FETCH DATA
# =========================
df1 = get_realtime_data(selected_symbol1, start_ts=start_ts, end_ts=end_ts, limit=max_rows)
df2 = get_realtime_data(selected_symbol2, start_ts=start_ts, end_ts=end_ts, limit=max_rows) if selected_symbol2 else None

if df1.empty:
    st.warning("Không có dữ liệu trong khoảng thời gian đã chọn.")
    st.stop()

df1p = insert_gap_breaks(df1, gap_minutes=gap_minutes)
df2p = insert_gap_breaks(df2, gap_minutes=gap_minutes) if df2 is not None and not df2.empty else None

# =========================
# HEADER
# =========================
coin_info1 = coins[coins["symbol"] == selected_symbol1].iloc[0]
h1, h2 = st.columns([1, 6], vertical_alignment="center")
with h1:
    if pd.notna(coin_info1.get("image", None)) and coin_info1["image"]:
        st.image(coin_info1["image"], width=70)
with h2:
    st.title(f"{coin_info1['name']} ({selected_symbol1})")

last_valid = df1.dropna(subset=["price"]).iloc[-1]
last_ts = pd.to_datetime(last_valid["timestamp"])
fresh_mins = (pd.Timestamp.now() - last_ts).total_seconds() / 60.0

m1, m2, m3, m4, m5 = st.columns([1.1, 1.1, 1.1, 1.1, 1.7])
m1.metric("Giá (USD)", f"${float(last_valid['price']):,.4f}", f"{float(last_valid['price_change_percentage_24h']):.2f}%")
m2.metric("Market Cap", f"${float(last_valid['market_cap'])/1e9:.2f}B" if pd.notna(last_valid["market_cap"]) else "—")
m3.metric("Volume 24H", f"${float(last_valid['volume_24h'])/1e6:.2f}M" if pd.notna(last_valid["volume_24h"]) else "—")
m4.metric("ATH", f"${float(last_valid['ath']):,.2f}" if pd.notna(last_valid["ath"]) else "—")
with m5:
    st.info(f"Cập nhật: {last_ts.strftime('%Y-%m-%d %H:%M:%S')}  •  ~{fresh_mins:.1f} phút trước")

st.divider()

# =========================
# MAIN NAV (no st.tabs -> avoid reset)
# =========================
if "main_view" not in st.session_state:
    st.session_state["main_view"] = "🔥 Realtime"

main_view = st.segmented_control(
    "",
    options=["🔥 Realtime", "🧊 Batch", "🚨 Alerts"],
    key="main_view",
)
# =========================
# VIEWS
# =========================
if main_view == "🔥 Realtime":
    st.subheader("Realtime")

    fig_price = create_price_chart(df1p, selected_symbol1, df2p, selected_symbol2, show_ma=show_ma)
    st.plotly_chart(fig_price, use_container_width=True)

    st.markdown("—")
    fig_vol = create_volume_chart(df1p, selected_symbol1, df2p, selected_symbol2)
    st.plotly_chart(fig_vol, use_container_width=True)

    with st.expander("Xem dữ liệu (mới nhất)", expanded=False):
        cols_show = [
            "timestamp", "price", "market_cap", "volume_24h",
            "high_24h", "low_24h", "price_change_percentage_24h",
            "ma_5min", "ma_15min", "ma_1hour"
        ]
        cols_show = [c for c in cols_show if c in df1.columns]
        st.dataframe(df1[cols_show].tail(300), use_container_width=True)

    st.download_button(
        "⬇️ Tải CSV (realtime)",
        data=df1.to_csv(index=False).encode("utf-8"),
        file_name=f"realtime_{selected_symbol1}.csv",
        mime="text/csv",
    )

elif main_view == "🧊 Batch":
    st.subheader("Batch")

    ohlc = get_hourly_stats(selected_symbol1, limit=168)
    if ohlc.empty:
        st.info("Chưa có dữ liệu batch. Hãy chạy batch job để tạo báo cáo.")
    else:
        ohlc_sorted = ohlc.sort_values("hour_timestamp")
        st.plotly_chart(create_hourly_candlestick(ohlc_sorted), use_container_width=True)

        table = ohlc.sort_values("hour_timestamp", ascending=False).copy()
        table.rename(columns={
            "hour_timestamp": "Giờ",
            "open_price": "Mở", "high_price": "Cao", "low_price": "Thấp", "close_price": "Đóng",
            "total_volume": "Tổng Volume",
            "price_volatility": "Volatility",
            "record_count": "Số bản ghi",
        }, inplace=True)

        table["Giờ"] = table["Giờ"].dt.strftime("%Y-%m-%d %H:%M")
        for c in ["Mở", "Đóng", "Cao", "Thấp"]:
            if c in table.columns:
                table[c] = table[c].map(lambda x: f"${float(x):,.4f}" if pd.notna(x) else "—")
        if "Tổng Volume" in table.columns:
            table["Tổng Volume"] = table["Tổng Volume"].map(lambda x: f"{float(x):,.0f}" if pd.notna(x) else "—")
        if "Volatility" in table.columns:
            table["Volatility"] = table["Volatility"].map(lambda x: f"{float(x):,.6f}" if pd.notna(x) else "—")

        st.dataframe(table, use_container_width=True)

else:
    st.subheader("Alerts")
    alerts = get_alerts(selected_symbol1, limit=80)
    if alerts.empty:
        st.info("Chưa có cảnh báo.")
    else:
        disp = alerts.copy()
        disp.rename(columns={
            "alert_type": "Loại",
            "message": "Nội dung",
            "price_after": "Giá",
            "change_percentage": "% thay đổi",
            "timestamp": "Thời gian",
        }, inplace=True)
        disp["Thời gian"] = disp["Thời gian"].dt.strftime("%Y-%m-%d %H:%M:%S")
        disp["Giá"] = disp["Giá"].map(lambda x: f"${float(x):,.4f}" if pd.notna(x) else "—")
        disp["% thay đổi"] = disp["% thay đổi"].map(lambda x: f"{float(x):.2f}%" if pd.notna(x) else "—")
        st.dataframe(disp, use_container_width=True)

st.caption("Tip: Nếu bạn tắt hệ thống vài ngày, hãy dùng khoảng 6h/24h để nhìn biến động rõ hơn.")