# 📊 Crypto Dashboard - Hướng Dẫn Sử Dụng Chi Tiết

## 🎯 Tổng Quan

Dashboard này cung cấp giao diện trực quan để theo dõi real-time dữ liệu cryptocurrency từ hệ thống Big Data pipeline của bạn. Được xây dựng với Flask, Bootstrap 5, và Chart.js.

## 🚀 Khởi Động Dashboard

### Yêu Cầu Tiên Quyết

1. **Hệ thống Big Data đang chạy:**
   - Minikube đã start
   - Kafka đang chạy và có port-forward: `kubectl port-forward kafka-0 9092:9092`
   - PostgreSQL đang chạy và có port-forward: `kubectl port-forward postgres-0 5433:5432`
   - Producer đang gửi dữ liệu
   - Spark Streaming đang xử lý

2. **Python virtual environment:**
   ```bash
   # Windows
   venv\Scripts\activate

   # Linux/Mac
   source venv/bin/activate
   ```

### Chạy Dashboard

```bash
python dashboard/app.py
```

Dashboard sẽ khả dụng tại: **http://localhost:5000**

---

## 📑 Các Trang Chính

### 1️⃣ **HOME (Trang Chủ)** - `/`

**Mục đích:** Theo dõi giá cryptocurrency real-time và thống kê tổng quan.

#### **Các Phần Chính:**

##### **A. System Statistics Cards (4 cards trên cùng)**
- **Total Records:** Tổng số bản ghi giá trong database
- **Total Alerts:** Tổng số cảnh báo đã phát hiện
- **Tracked Coins:** Số lượng đồng coin đang theo dõi
- **Last Update:** Thời gian cập nhật dữ liệu gần nhất

💡 **Cách đọc:**
- Số lớn = Hệ thống đang thu thập nhiều dữ liệu
- Last Update cập nhật liên tục = Hệ thống hoạt động tốt

##### **B. Real-time Prices Table**
Hiển thị giá real-time của tất cả cryptocurrency.

**Các Cột:**
- **Rank:** Thứ hạng theo market cap
- **Symbol:** Ký hiệu coin (BTC, ETH,...)
- **Name:** Tên đầy đủ
- **Price (USD):** Giá hiện tại (tự động format: $0.000001 hoặc $50,000)
- **24h Change:** % thay đổi trong 24h (🟢 tăng, 🔴 giảm)
- **Volume:** Khối lượng giao dịch 24h
- **Market Cap:** Vốn hóa thị trường

💡 **Tính Năng:**
- ✅ Auto-refresh mỗi 30 giây
- ✅ Highlight row khi hover
- ✅ Animation khi giá thay đổi
- ✅ Nút Refresh thủ công ở góc phải

##### **C. Top Movers Sidebar**

**Top Gainers 🚀:**
- Top 10 coins tăng giá mạnh nhất 24h
- Hiển thị: Symbol, Name, Price, % Change

**Top Losers 📉:**
- Top 10 coins giảm giá mạnh nhất 24h

**Market Summary:**
- **Avg Change:** % thay đổi trung bình của tất cả coins
- **Gainers:** Số lượng coins đang tăng
- **Losers:** Số lượng coins đang giảm

💡 **Cách sử dụng:** Nhanh chóng xác định xu hướng thị trường và cơ hội trading.

---

### 2️⃣ **CHARTS (Biểu Đồ)** - `/charts`

**Mục đích:** Phân tích chi tiết xu hướng giá với biểu đồ interactive.

#### **Các Phần Chính:**

##### **A. Price Charts & Analytics (Card chính)**

**Controls (Bảng điều khiển):**

1. **Cryptocurrencies:**
   - Multi-select dropdown (giữ Ctrl/Cmd để chọn nhiều)
   - Chọn 1-10 coins để so sánh trên cùng 1 biểu đồ
   - 💡 **Tip:** Chọn tối đa 3-4 coins để biểu đồ dễ đọc

2. **Time Range:**
   - 50 points (~25 phút): Quick view
   - 100 points (~50 phút): Default, cân bằng
   - 200 points (~100 phút): Mid-term analysis
   - 500 points (~4 giờ): Long-term trends

3. **Chart Type:**
   - **Line Chart:** Đường liền, rõ ràng
   - **Area Chart:** Tô màu dưới đường, đẹp mắt
   - **Bar Chart:** Dạng cột, so sánh từng điểm

**Quick Actions:**
- **Update All Charts:** Làm mới tất cả biểu đồ
- **Select Top 3 Coins:** Tự động chọn BTC, ETH, USDT

##### **B. Moving Averages Analysis**

**Mục đích:** Phân tích xu hướng giá qua các đường MA (Moving Average).

**Các đường hiển thị:**
- **Price (Solid line - Màu xanh dương):** Giá thực tế
- **MA 5min (Dashed line - Màu xanh lá):** Trung bình 5 phút
- **MA 15min (Dashed line - Màu vàng):** Trung bình 15 phút
- **MA 1hour (Dashed line - Màu đỏ):** Trung bình 1 giờ

💡 **Cách đọc MA:**
- Price > MA = Xu hướng tăng (Bullish)
- Price < MA = Xu hướng giảm (Bearish)
- MA ngắn hạn cắt MA dài hạn từ dưới lên = Signal mua
- MA ngắn hạn cắt MA dài hạn từ trên xuống = Signal bán

##### **C. Trading Volume Comparison**

Biểu đồ cột so sánh khối lượng giao dịch 24h của top 10 coins.

💡 **Insight:** Volume cao = Thanh khoản tốt, dễ mua/bán.

---

### 3️⃣ **ALERTS (Cảnh Báo)** - `/alerts`

**Mục đích:** Theo dõi các sự kiện biến động giá bất thường.

#### **Các Phần Chính:**

##### **A. Filter Alerts**

**Bộ lọc:**
- **Symbol:** Lọc theo coin cụ thể (BTC, ETH,...)
- **Alert Type:** Lọc theo loại cảnh báo
  - `VOLATILITY_WARNING`: Biến động > 5%
  - `PRICE_SPIKE`: Tăng đột biến
  - `PRICE_DROP`: Giảm đột biến
- **Limit:** Số lượng alerts hiển thị (25/50/100/200)

**Buttons:**
- **Apply Filters:** Áp dụng bộ lọc
- **Clear:** Xóa tất cả filter
- **Refresh:** Làm mới dữ liệu

##### **B. Alerts Summary Cards**

- **Total Alerts:** Tổng số cảnh báo hiện có
- **High Volatility:** Số cảnh báo biến động cao
- **Last Alert:** Thời gian cảnh báo gần nhất

##### **C. Alerts Table**

**Các cột:**
- **Time:** Thời gian xảy ra alert
- **Symbol:** Coin bị ảnh hưởng
- **Type:** Loại cảnh báo (badge màu)
  - 🟡 Warning
  - 🔴 Price Spike
  - 🔵 Price Drop
- **Message:** Mô tả chi tiết
- **Price:** Giá tại thời điểm alert
- **Change %:** % thay đổi gây ra alert

💡 **Use Case:** Nhận biết các biến động bất thường để ra quyết định trading kịp thời.

---

### 4️⃣ **STATISTICS (Thống Kê)** - `/stats`

**Mục đích:** Phân tích dữ liệu aggregate theo giờ/ngày.

#### **3 Chế Độ Xem:**

##### **A. Hourly Stats (Thống kê theo giờ)**

**Hourly Price Trends Chart:**
- Line chart hiển thị giá trung bình mỗi giờ
- Tối đa 5 coins cùng lúc
- Time unit: Hour

**Hourly Data Table:**
- Top 20 records gần nhất
- Columns: Hour, Symbol, Avg Price

💡 **Note:** Dữ liệu hourly chỉ xuất hiện sau khi Spark Batch Processing chạy (cần thêm batch job).

##### **B. Daily Stats (Thống kê theo ngày)**

Tương tự Hourly nhưng aggregate theo ngày.

💡 **Use Case:** Phân tích xu hướng dài hạn (weekly/monthly trends).

##### **C. Comparison (So sánh)**

**3 Biểu đồ:**

1. **Market Cap Distribution (Bar Chart):**
   - So sánh vốn hóa của top 10 coins
   - Y-axis: Market Cap (tỷ đô)

2. **Volume Distribution (Doughnut Chart):**
   - Phân bố % volume của top 10 coins
   - Legend ở bên phải

3. **Price Change Comparison (Horizontal Bar):**
   - So sánh % thay đổi 24h
   - 🟢 Tăng, 🔴 Giảm
   - Sắp xếp từ cao đến thấp

💡 **Insight:** Nhanh chóng so sánh hiệu suất các coins trong portfolio.

---

## 🎨 Giao Diện & UX

### **Theme:**
- **Background:** Gradient tím đến hồng (Purple to Violet)
- **Cards:** Trắng với shadow, hover effect (nâng lên)
- **Primary Color:** Indigo (#4f46e5)
- **Success/Danger:** Green/Red cho tăng/giảm

### **Responsive Design:**
- ✅ Desktop (>1200px): Full layout
- ✅ Tablet (768px-1200px): 2-column layout
- ✅ Mobile (<768px): Single column, stacked cards

### **Animations:**
- Smooth transitions (0.3s)
- Hover effects: Scale, shadow, color change
- Price flash animation khi giá thay đổi
- Slide-in animation cho refresh indicator

---

## ⚙️ Configuration

### **File: `dashboard/config.py`**

```python
POSTGRES_CONFIG = {
    'host': 'localhost',
    'port': 5433,  # Sau khi port-forward
    'database': 'cryptodb',
    'user': 'cryptouser',
    'password': 'cryptopass123'
}

REFRESH_INTERVAL = 30  # Auto-refresh mỗi 30 giây
TOP_MOVERS_LIMIT = 10  # Top 10 gainers/losers
CHART_DATA_LIMIT = 100  # Default số điểm trên chart
```

**Tùy chỉnh:**
- Tăng `REFRESH_INTERVAL` nếu muốn giảm load server
- Tăng `TOP_MOVERS_LIMIT` để xem nhiều coins hơn
- Tăng `CHART_DATA_LIMIT` cho historical data dài hơn

---

## 🔧 Troubleshooting

### **Vấn đề 1: "No data available"**

**Nguyên nhân:**
- Producer chưa chạy
- Spark Streaming chưa ghi vào database
- Port-forward chưa được thiết lập

**Giải pháp:**
```bash
# Kiểm tra producer
python producer/main.py

# Kiểm tra Spark Streaming
kubectl logs -f spark-master-xxx

# Kiểm tra port-forward
kubectl port-forward postgres-0 5433:5432
```

### **Vấn đề 2: "Failed to load data"**

**Nguyên nhân:** Mất kết nối database

**Giải pháp:**
```bash
# Test kết nối database
psql -h localhost -p 5433 -U cryptouser -d cryptodb

# Kiểm tra lại config.py
```

### **Vấn đề 3: Charts không hiển thị**

**Nguyên nhân:**
- Chưa đủ dữ liệu trong database
- JavaScript error (check console)

**Giải pháp:**
- Đợi ít nhất 5-10 phút để có đủ data points
- Mở Browser DevTools → Console để xem errors
- Hard refresh (Ctrl+F5)

### **Vấn đề 4: Hourly/Daily Stats trống**

**Nguyên nhân:** Chưa có Spark Batch Job để tính aggregates

**Giải pháp:**
Cần implement batch processing job riêng:
```python
# spark-apps/batch/batch_processor.py
# Aggregate realtime_prices → hourly_aggregates, daily_aggregates
```

---

## 📊 API Endpoints Reference

### **GET /api/realtime-prices**
Lấy giá mới nhất của tất cả coins.

**Response:**
```json
[
  {
    "symbol": "BTC",
    "name": "Bitcoin",
    "price": 50000.50,
    "volume_24h": 30000000000,
    "market_cap": 1000000000000,
    "price_change_24h": 2.5,
    "ma_5min": 49980.25,
    "ma_15min": 49950.75,
    "ma_1hour": 49800.10,
    "timestamp": "2025-11-17T10:30:00"
  }
]
```

### **GET /api/price-history/{symbol}?limit=100**
Lấy lịch sử giá của 1 coin.

**Parameters:**
- `symbol` (path): BTC, ETH, etc.
- `limit` (query, optional): Số điểm dữ liệu (default 100, max 1000)

### **GET /api/top-movers**
Lấy top gainers và losers.

**Response:**
```json
{
  "gainers": [...],  // Top 10 tăng giá
  "losers": [...]    // Top 10 giảm giá
}
```

### **GET /api/alerts?symbol=BTC&type=VOLATILITY_WARNING&limit=50**
Lấy danh sách alerts với filters.

**Parameters:**
- `symbol` (optional): Filter by coin
- `type` (optional): Filter by alert type
- `limit` (optional): Số lượng alerts (default 50)

### **GET /api/system-stats**
Lấy thống kê hệ thống.

**Response:**
```json
{
  "total_records": 15000,
  "total_alerts": 120,
  "unique_symbols": 20,
  "latest_update": "2025-11-17T10:30:00"
}
```

---

## 🎯 Best Practices

### **Cho Người Dùng:**

1. **Monitoring Real-time:**
   - Để Home page mở trong tab riêng
   - Auto-refresh sẽ cập nhật liên tục

2. **Phân tích xu hướng:**
   - Dùng Charts page với MA analysis
   - Chọn time range phù hợp (50-500 points)

3. **Quản lý risk:**
   - Set up Alerts page trên monitor thứ 2
   - Filter theo coins bạn đang hold

4. **So sánh performance:**
   - Dùng Statistics → Comparison
   - Xem market cap và volume distribution

### **Cho Developers:**

1. **Thêm coins mới:**
   - Update `charts.html` → `#symbolSelect` options
   - Hoặc dynamic load từ `/api/realtime-prices`

2. **Customize theme:**
   - Edit CSS variables trong `base.html` → `:root`
   - Change gradient colors trong cards

3. **Tối ưu performance:**
   - Tăng `REFRESH_INTERVAL` trong production
   - Thêm Redis cache cho API responses
   - Database indexing đã được setup

---

## 📱 Mobile Usage

Dashboard responsive 100% trên mobile:
- Navbar collapse thành hamburger menu
- Cards stack vertically
- Tables scroll horizontally
- Charts auto-resize

**Recommended:** Sử dụng landscape mode cho charts để xem rõ hơn.

---

## 🆘 Support

Nếu gặp vấn đề:
1. Check logs trong terminal đang chạy `app.py`
2. Xem Browser Console (F12) để debug JavaScript
3. Kiểm tra database connection
4. Restart toàn bộ hệ thống (Minikube + Dashboard)

---

## 📝 Changelog

### Version 1.0 (Current)
- ✅ Real-time price monitoring
- ✅ Multi-coin comparison charts
- ✅ Moving averages analysis
- ✅ Alert system with filters
- ✅ Responsive design
- ✅ Auto-refresh (30s)
- ✅ Modern UI with gradients and animations

### Planned Features
- 🔜 Dark/Light theme toggle
- 🔜 User preferences (favorite coins)
- 🔜 Export data to CSV
- 🔜 Email/SMS notifications for alerts
- 🔜 Portfolio tracking
- 🔜 Technical indicators (RSI, MACD, Bollinger Bands)

---

**Happy Trading! 🚀📈💰**
