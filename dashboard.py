import streamlit as st
import pandas as pd
import psycopg2
import plotly.graph_objects as go
from streamlit_autorefresh import st_autorefresh

st.set_page_config(page_title="Crypto Big Data Dashboard", page_icon="💎", layout="wide")
st_autorefresh(interval=30000, key="datarefresh") # Refresh sau 30 giây

DB_CONFIG = {
    "dbname": "cryptodb",
    "user": "cryptouser",
    "password": "cryptopass123",
    "host": "localhost",
    "port": "5433" 
}

@st.cache_resource
def get_conn():
    return psycopg2.connect(**DB_CONFIG)

# --- QUERY DATA ---
def get_coin_list():
    conn = get_conn()
    return pd.read_sql("SELECT DISTINCT symbol, name, image FROM realtime_prices ORDER BY symbol", conn)

def get_realtime_data(symbol, limit=200):
    """Lấy dữ liệu Realtime cho 1 coin"""
    conn = get_conn()
    query = f"""
        SELECT * FROM realtime_prices 
        WHERE symbol = '{symbol}' ORDER BY timestamp DESC LIMIT {limit}
    """
    # Lấy 200 bản ghi, sau đó sắp xếp lại theo thời gian tăng dần
    return pd.read_sql(query, conn).sort_values("timestamp")

def get_hourly_stats(symbol):
    """Lấy dữ liệu Batch (hourly) cho Bảng Báo cáo"""
    conn = get_conn()
    # Lấy 7 ngày dữ liệu gần nhất để báo cáo
    query = f"""
        SELECT 
            hour_timestamp, open_price, high_price, low_price, close_price, 
            total_volume, price_volatility, record_count
        FROM hourly_stats 
        WHERE symbol = '{symbol}'
        ORDER BY hour_timestamp DESC LIMIT 168 
    """ # 168 records = 7 ngày x 24 giờ
    return pd.read_sql(query, conn)

# --- CHARTING FUNCTIONS ---

def create_line_chart(df1, symbol1, metric, title, df2=None, symbol2=None):
    """Tạo biểu đồ đường hỗ trợ so sánh 2 coin"""
    fig = go.Figure()

    # Coin 1
    fig.add_trace(go.Scatter(
        x=df1['timestamp'], 
        y=df1[metric], 
        mode='lines', 
        name=f"{metric} ({symbol1})",
        line=dict(color='#00F0FF', width=2),
        connectgaps=False # Xử lý ngắt quãng dữ liệu
    ))

    # Coin 2 (So sánh)
    if df2 is not None and symbol2:
        fig.add_trace(go.Scatter(
            x=df2['timestamp'], 
            y=df2[metric], 
            mode='lines', 
            name=f"{metric} ({symbol2})",
            line=dict(color='#FFD700', width=2),
            connectgaps=False
        ))
    
    # Thêm MA (Chỉ thêm cho Coin 1, nếu có)
    if metric == 'price' and df1['ma_5min'].notna().any():
        fig.add_trace(go.Scatter(
            x=df1['timestamp'], y=df1['ma_5min'], name=f"MA 5M ({symbol1})", 
            line=dict(dash='dot', color='orange', width=1), 
            visible='legendonly', # Ẩn đi, chỉ hiện khi click vào Legend
            connectgaps=False
        ))
        if 'ma_15min' in df1.columns and df1['ma_15min'].notna().any():
            fig.add_trace(go.Scatter(
                x=df1['timestamp'], y=df1['ma_15min'], name=f"MA 15M ({symbol1})", 
                line=dict(dash='dot', color='red', width=1), 
                visible='legendonly', 
                connectgaps=False
            ))
        if 'ma_1hour' in df1.columns and df1['ma_1hour'].notna().any():
             fig.add_trace(go.Scatter(
                x=df1['timestamp'], y=df1['ma_1hour'], name=f"MA 1H ({symbol1})", 
                line=dict(dash='dot', color='green', width=1), 
                visible='legendonly', 
                connectgaps=False
            ))

    fig.update_layout(
        title=title,
        template="plotly_dark",
        height=400,
        xaxis_title="Thời Gian",
        yaxis_title=metric.replace('_', ' ').title(),
        hovermode="x unified"
    )
    return fig

# --- UI SETUP ---
st.sidebar.title("🎛️ Control Panel")
coins = get_coin_list()

if coins.empty:
    st.warning("Đang chờ dữ liệu Realtime... Vui lòng bật Producer.")
    st.stop()

# --- COIN SELECTION ---
col_select1, col_select2 = st.sidebar.columns(2)
selected_symbol1 = col_select1.selectbox("Chọn Coin Chính (1):", coins['symbol'].unique(), key='coin1')
selected_symbol2 = col_select2.selectbox("Chọn Coin So sánh (2):", [None] + list(coins['symbol'].unique()), key='coin2', index=0)

# Loại bỏ trùng lặp nếu người dùng chọn coin1 = coin2
if selected_symbol1 == selected_symbol2:
    selected_symbol2 = None

# --- DATA FETCHING ---
df1 = get_realtime_data(selected_symbol1)
df2 = get_realtime_data(selected_symbol2) if selected_symbol2 else None

if df1.empty:
    st.warning(f"Chưa có dữ liệu realtime cho {selected_symbol1}.")
    st.stop()

# --- HEADER: METRICS ---
coin_info1 = coins[coins['symbol'] == selected_symbol1].iloc[0]
st.header(f"💰 {coin_info1['name']} ({selected_symbol1}) Dashboard")

with st.container():
    last1 = df1.iloc[-1]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Giá (USD)", f"${last1['price']:,.4f}", f"{last1['price_change_percentage_24h']:.2f}%")
    c2.metric("Vốn hóa (Market Cap)", f"${last1['market_cap']/1e9:.2f}B")
    c3.metric("Volume 24H", f"${last1['volume_24h']/1e6:.2f}M")
    c4.metric("Đỉnh Lịch Sử (ATH)", f"${last1['ath']:,.2f}")

# --- MAIN CHARTS ---
tab1, tab2 = st.tabs(["📊 Realtime Price & Volume", "📜 Batch Historical Summary"])

with tab1:
    st.subheader(f"Diễn biến Realtime (Last {len(df1)} records)")
    
    # 1. BIỂU ĐỒ PRICE
    price_title = f"Giá ({selected_symbol1}" + (f" vs {selected_symbol2})" if selected_symbol2 else ")")
    fig_price = create_line_chart(df1, selected_symbol1, 'price', price_title, df2, selected_symbol2)
    st.plotly_chart(fig_price, use_container_width=True)
    
    # 2. BIỂU ĐỒ VOLUME
    st.markdown("---")
    volume_title = f"Volume 24H ({selected_symbol1}" + (f" vs {selected_symbol2})" if selected_symbol2 else ")")
    fig_volume = create_line_chart(df1, selected_symbol1, 'volume_24h', volume_title, df2, selected_symbol2)
    st.plotly_chart(fig_volume, use_container_width=True)

with tab2:
    st.subheader(f"Báo cáo Tổng hợp Batch (Hourly) cho {selected_symbol1}")
    
    # 1. Bảng báo cáo
    ohlc_df = get_hourly_stats(selected_symbol1)
    
    if not ohlc_df.empty:
        # Làm sạch và format dữ liệu
        ohlc_df.rename(columns={
            'hour_timestamp': 'Giờ', 
            'open_price': 'Mở', 'close_price': 'Đóng', 
            'high_price': 'Cao', 'low_price': 'Thấp',
            'total_volume': 'Tổng Volume', 
            'price_volatility': 'Biến động (StdDev)', 
            'record_count': 'SL Bản ghi'
        }, inplace=True)
        
        # Format số liệu
        ohlc_df['Giờ'] = ohlc_df['Giờ'].dt.strftime('%Y-%m-%d %H:%M')
        for col in ['Mở', 'Đóng', 'Cao', 'Thấp']:
            ohlc_df[col] = ohlc_df[col].map('${:,.4f}'.format)
        ohlc_df['Tổng Volume'] = ohlc_df['Tổng Volume'].map('{:,.0f}'.format)
        ohlc_df['Biến động (StdDev)'] = ohlc_df['Biến động (StdDev)'].map('{:,.4f}'.format)
        
        st.dataframe(ohlc_df, use_container_width=True)
        
        # 2. Thông tin tóm tắt
        st.markdown("---")
        st.info("💡 Bảng này hiển thị dữ liệu đã được tính toán trong Batch Job, phục vụ phân tích lịch sử và báo cáo (Cold Path).")
        
    else:
        st.info("Chưa có dữ liệu Batch (hourly_stats). Vui lòng chạy Batch Job sau khi có dữ liệu Realtime.")