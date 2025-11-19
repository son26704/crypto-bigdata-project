import streamlit as st
import pandas as pd
import psycopg2
import plotly.graph_objects as go
from streamlit_autorefresh import st_autorefresh
import datetime

# --- 1. CẤU HÌNH TRANG & KẾT NỐI ---
st.set_page_config(
    page_title="Crypto Big Data Monitor",
    page_icon="📊",
    layout="wide", # Chế độ toàn màn hình
    initial_sidebar_state="expanded"
)

# Tự động refresh trang mỗi 30 giây (phù hợp với Spark Trigger)
st_autorefresh(interval=30000, key="datarefresh")

# Config Database (Kết nối qua Port-forward)
DB_CONFIG = {
    "dbname": "cryptodb",
    "user": "cryptouser",
    "password": "cryptopass123",
    "host": "localhost",
    "port": "5433" 
}

@st.cache_resource
def get_db_connection():
    """Tạo kết nối DB (Cache để không phải connect lại liên tục)"""
    try:
        return psycopg2.connect(**DB_CONFIG)
    except Exception as e:
        st.error(f"🔴 Lỗi kết nối DB: {e}")
        return None

# --- 2. HÀM LẤY DỮ LIỆU ---
def get_symbols():
    """Lấy danh sách các đồng coin đang có trong DB"""
    conn = get_db_connection()
    if not conn: return []
    query = "SELECT DISTINCT symbol FROM realtime_prices ORDER BY symbol;"
    df = pd.read_sql(query, conn)
    return df['symbol'].tolist()

def get_realtime_data(symbol, limit=200):
    """Lấy dữ liệu Realtime để vẽ Line Chart"""
    conn = get_db_connection()
    query = f"""
        SELECT timestamp, price, ma_5min, volume_24h 
        FROM realtime_prices 
        WHERE symbol = '{symbol}' 
        ORDER BY timestamp DESC 
        LIMIT {limit}
    """
    df = pd.read_sql(query, conn)
    return df.sort_values(by="timestamp")

def get_hourly_data(symbol):
    """Lấy dữ liệu Hourly Stats để vẽ Nến (Candlestick)"""
    conn = get_db_connection()
    # Lấy 72 giờ gần nhất
    query = f"""
        SELECT hour_timestamp, open_price, high_price, low_price, close_price, total_volume
        FROM hourly_stats
        WHERE symbol = '{symbol}'
        ORDER BY hour_timestamp DESC
        LIMIT 72
    """
    df = pd.read_sql(query, conn)
    return df.sort_values(by="hour_timestamp")

def get_latest_alerts(limit=10):
    """Lấy cảnh báo mới nhất"""
    conn = get_db_connection()
    query = f"""
        SELECT timestamp, symbol, alert_type, message, price_after, volume
        FROM alerts 
        ORDER BY timestamp DESC 
        LIMIT {limit}
    """
    return pd.read_sql(query, conn)

# --- 3. GIAO DIỆN CHÍNH (UI) ---

# --- Sidebar: Bộ lọc ---
st.sidebar.title("🎛️ Control Panel")
available_symbols = get_symbols()
if not available_symbols:
    st.warning("Chưa có dữ liệu trong Database. Hãy chạy Pipeline trước!")
    st.stop()

selected_symbol = st.sidebar.selectbox("Chọn Crypto:", available_symbols, index=0)
st.sidebar.markdown("---")
st.sidebar.info(f"Đang theo dõi: **{selected_symbol}**")

# --- Header: KPI Metrics (Giá hiện tại) ---
st.title(f"🚀 {selected_symbol} Market Overview")

# Lấy bản ghi mới nhất
realtime_df = get_realtime_data(selected_symbol, limit=2)
if not realtime_df.empty:
    latest = realtime_df.iloc[-1]
    prev = realtime_df.iloc[-2] if len(realtime_df) > 1 else latest
    
    delta_price = latest['price'] - prev['price']
    delta_percent = (delta_price / prev['price']) * 100 if prev['price'] != 0 else 0
    
    col1, col2, col3 = st.columns(3)
    col1.metric("Giá Hiện Tại", f"${latest['price']:,.2f}", f"{delta_percent:.2f}%")
    col2.metric("Volume 24H", f"${latest['volume_24h']:,.0f}", "Updated")
    col3.metric("Cập nhật lúc", latest['timestamp'].strftime('%H:%M:%S'), "Live")

# --- Tabs: Chia giao diện thành các tab chức năng ---
tab1, tab2, tab3 = st.tabs(["📈 Real-time Analysis", "🕯️ Historical (Batch)", "⚠️ System Alerts"])

# === TAB 1: REAL-TIME CHART ===
with tab1:
    st.subheader("Diễn biến giá thực (Streaming)")
    
    # Query dữ liệu nhiều hơn để vẽ biểu đồ
    chart_data = get_realtime_data(selected_symbol, limit=500)
    
    if not chart_data.empty:
        fig = go.Figure()
        
        # Đường giá (Price)
        fig.add_trace(go.Scatter(
            x=chart_data['timestamp'], 
            y=chart_data['price'],
            mode='lines+markers', # Hiện cả điểm để thấy rõ khi data bị gãy
            name='Price',
            line=dict(color='#00F0FF', width=2),
            marker=dict(size=4)
        ))
        
        # Đường MA (Moving Average)
        if chart_data['ma_5min'].notna().any():
            fig.add_trace(go.Scatter(
                x=chart_data['timestamp'], 
                y=chart_data['ma_5min'],
                mode='lines',
                name='MA 5min',
                line=dict(color='#FFA500', width=1, dash='dot')
            ))

        fig.update_layout(
            height=500,
            template="plotly_dark",
            xaxis_title="Thời gian",
            yaxis_title="Giá (USD)",
            hovermode="x unified"
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Đang chờ dữ liệu Streaming...")

# === TAB 2: HISTORICAL CHART (CANDLESTICK) ===
with tab2:
    st.subheader("Dữ liệu tổng hợp theo giờ (Batch Job)")
    hourly_df = get_hourly_data(selected_symbol)
    
    if not hourly_df.empty:
        fig_candle = go.Figure(data=[go.Candlestick(
            x=hourly_df['hour_timestamp'],
            open=hourly_df['open_price'],
            high=hourly_df['high_price'],
            low=hourly_df['low_price'],
            close=hourly_df['close_price'],
            name="OHLC"
        )])
        
        fig_candle.update_layout(
            height=500,
            template="plotly_dark",
            xaxis_rangeslider_visible=False,
            title=f"Biểu đồ nến {selected_symbol} (Hourly)"
        )
        st.plotly_chart(fig_candle, use_container_width=True)
        
        with st.expander("Xem dữ liệu thô (Hourly)"):
            st.dataframe(hourly_df)
    else:
        st.warning("Chưa có dữ liệu Batch. Hãy chạy 'spark-submit batch_processor.py'!")

# === TAB 3: ALERTS ===
with tab3:
    st.subheader("🔥 Cảnh báo phát hiện từ Spark Streaming")
    alerts_df = get_latest_alerts(20)
    
    if not alerts_df.empty:
        # Style cho bảng đẹp hơn
        def highlight_type(val):
            color = 'red' if 'VOLATILITY' in val else 'orange'
            return f'color: {color}; font-weight: bold'

        st.dataframe(
            alerts_df.style.applymap(highlight_type, subset=['alert_type']),
            use_container_width=True
        )
    else:
        st.success("Hệ thống ổn định. Chưa có cảnh báo nào.")