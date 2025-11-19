from flask import Flask, render_template, jsonify, request
import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta
import json
from config import POSTGRES_CONFIG, REFRESH_INTERVAL, TOP_MOVERS_LIMIT, CHART_DATA_LIMIT

app = Flask(__name__)


def get_db_connection():
    """Create database connection"""
    try:
        return psycopg2.connect(**POSTGRES_CONFIG)
    except Exception as e:
        print(f"Database connection error: {e}")
        return None


def query_db(query, params=None):
    """Execute query and return results"""
    conn = get_db_connection()
    if not conn:
        return []

    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    try:
        cur.execute(query, params or ())
        results = cur.fetchall()
        return results
    except Exception as e:
        print(f"Database query error: {e}")
        print(f"Query: {query}")
        print(f"Params: {params}")
        return []
    finally:
        cur.close()
        conn.close()


# ========== ROUTES ==========

@app.route('/')
def index():
    """Home page - Real-time prices"""
    return render_template('index.html', refresh_interval=REFRESH_INTERVAL)


@app.route('/charts')
def charts():
    """Charts page"""
    return render_template('charts.html')


@app.route('/alerts')
def alerts():
    """Alerts page"""
    return render_template('alerts.html')


@app.route('/stats')
def stats():
    """Statistics page"""
    return render_template('stats.html')


# ========== API ENDPOINTS ==========

@app.route('/api/realtime-prices')
def api_realtime_prices():
    """Get latest realtime prices"""
    query = """
        SELECT DISTINCT ON (symbol)
            symbol, name, price, volume_24h, market_cap,
            price_change_24h, price_change_percentage_24h,
            ma_5min, ma_15min, ma_1hour,
            timestamp, created_at
        FROM realtime_prices
        ORDER BY symbol, timestamp DESC
        LIMIT 50
    """

    data = query_db(query)

    # Convert to JSON-serializable format
    result = []
    for row in data:
        result.append({
            'symbol': row['symbol'],
            'name': row['name'],
            'price': float(row['price']) if row['price'] else 0,
            'volume_24h': float(row['volume_24h']) if row['volume_24h'] else 0,
            'market_cap': float(row['market_cap']) if row['market_cap'] else 0,
            'price_change_24h': float(row['price_change_percentage_24h']) if row['price_change_percentage_24h'] else 0,
            'ma_5min': float(row['ma_5min']) if row['ma_5min'] else None,
            'ma_15min': float(row['ma_15min']) if row['ma_15min'] else None,
            'ma_1hour': float(row['ma_1hour']) if row['ma_1hour'] else None,
            'timestamp': row['timestamp'].isoformat() if row['timestamp'] else None
        })

    return jsonify(result)


@app.route('/api/price-history/<symbol>')
def api_price_history(symbol):
    """Get price history for chart"""
    # Get limit from query parameter, default to CHART_DATA_LIMIT
    limit = request.args.get('limit', CHART_DATA_LIMIT, type=int)

    # Cap maximum limit to prevent abuse
    limit = min(limit, 1000)

    query = """
        SELECT symbol, price, timestamp
        FROM realtime_prices
        WHERE symbol = %s
        ORDER BY timestamp DESC
        LIMIT %s
    """

    data = query_db(query, (symbol, limit))

    result = {
        'symbol': symbol,
        'data': [
            {
                'timestamp': row['timestamp'].isoformat(),
                'price': float(row['price'])
            }
            for row in reversed(data)  # Reverse to show oldest first
        ]
    }

    return jsonify(result)


@app.route('/api/top-movers')
def api_top_movers():
    """Get top gainers and losers"""
    query = """
        SELECT DISTINCT ON (symbol)
            symbol, name, price, price_change_percentage_24h
        FROM realtime_prices
        ORDER BY symbol, timestamp DESC
    """

    data = query_db(query)

    # Filter out None values and sort by price change
    filtered_data = [row for row in data if row['price_change_percentage_24h'] is not None]
    sorted_data = sorted(filtered_data, key=lambda x: x['price_change_percentage_24h'], reverse=True)

    result = {
        'gainers': [
            {
                'symbol': row['symbol'],
                'name': row['name'],
                'price': float(row['price']) if row['price'] else 0,
                'change': float(row['price_change_percentage_24h']) if row['price_change_percentage_24h'] else 0
            }
            for row in sorted_data[:TOP_MOVERS_LIMIT]
        ],
        'losers': [
            {
                'symbol': row['symbol'],
                'name': row['name'],
                'price': float(row['price']) if row['price'] else 0,
                'change': float(row['price_change_percentage_24h']) if row['price_change_percentage_24h'] else 0
            }
            for row in sorted_data[-TOP_MOVERS_LIMIT:]
        ]
    }

    return jsonify(result)


@app.route('/api/alerts')
def api_alerts():
    """Get recent alerts"""
    # Get optional filters
    symbol = request.args.get('symbol', None)
    alert_type = request.args.get('type', None)
    limit = request.args.get('limit', 50, type=int)

    query_parts = ["SELECT symbol, alert_type, message, price_after, change_percentage, timestamp FROM alerts"]
    conditions = []
    params = []

    if symbol:
        conditions.append("symbol = %s")
        params.append(symbol)

    if alert_type:
        conditions.append("alert_type = %s")
        params.append(alert_type)

    if conditions:
        query_parts.append("WHERE " + " AND ".join(conditions))

    query_parts.append("ORDER BY timestamp DESC LIMIT %s")
    params.append(limit)

    query = " ".join(query_parts)
    data = query_db(query, tuple(params))

    result = [
        {
            'symbol': row['symbol'],
            'type': row['alert_type'],
            'message': row['message'],
            'price': float(row['price_after']) if row['price_after'] else 0,
            'change': float(row['change_percentage']) if row['change_percentage'] else 0,
            'timestamp': row['timestamp'].isoformat() if row['timestamp'] else None
        }
        for row in data
    ]

    return jsonify(result)


@app.route('/api/hourly-stats')
def api_hourly_stats():
    """Get hourly statistics"""
    query = """
        SELECT symbol, hour_timestamp, avg_price,
               high_price, low_price, total_volume
        FROM hourly_aggregates
        ORDER BY hour_timestamp DESC
        LIMIT 100
    """

    data = query_db(query)

    result = [
        {
            'symbol': row['symbol'],
            'hour': row['hour_timestamp'].isoformat() if row['hour_timestamp'] else None,
            'avg_price': float(row['avg_price']) if row['avg_price'] else 0,
            'high': float(row['high_price']) if row['high_price'] else 0,
            'low': float(row['low_price']) if row['low_price'] else 0,
            'volume': float(row['total_volume']) if row['total_volume'] else 0
        }
        for row in data
    ]

    return jsonify(result)


@app.route('/api/daily-stats')
def api_daily_stats():
    """Get daily statistics"""
    query = """
        SELECT symbol, date, avg_price,
               high_price, low_price, total_volume,
               price_volatility
        FROM daily_aggregates
        ORDER BY date DESC
        LIMIT 50
    """

    data = query_db(query)

    result = [
        {
            'symbol': row['symbol'],
            'day': row['date'].isoformat() if row['date'] else None,
            'avg_price': float(row['avg_price']) if row['avg_price'] else 0,
            'high': float(row['high_price']) if row['high_price'] else 0,
            'low': float(row['low_price']) if row['low_price'] else 0,
            'volume': float(row['total_volume']) if row['total_volume'] else 0,
            'change': float(row['price_volatility']) if row['price_volatility'] else 0
        }
        for row in data
    ]

    return jsonify(result)


@app.route('/api/system-stats')
def api_system_stats():
    """Get system statistics"""
    queries = {
        'total_records': "SELECT COUNT(*) as count FROM realtime_prices",
        'total_alerts': "SELECT COUNT(*) as count FROM alerts",
        'unique_symbols': "SELECT COUNT(DISTINCT symbol) as count FROM realtime_prices",
        'latest_update': "SELECT MAX(timestamp) as time FROM realtime_prices"
    }

    result = {}
    for key, query in queries.items():
        data = query_db(query)
        if data:
            if key == 'latest_update':
                result[key] = data[0]['time'].isoformat() if data[0]['time'] else None
            else:
                result[key] = data[0]['count']

    return jsonify(result)


@app.route('/api/alert-types')
def api_alert_types():
    """Get distinct alert types for filtering"""
    query = "SELECT DISTINCT alert_type FROM alerts ORDER BY alert_type"
    data = query_db(query)
    result = [row['alert_type'] for row in data]
    return jsonify(result)


# Error handlers
@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Not found'}), 404


@app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Internal server error'}), 500


if __name__ == '__main__':
    print("=" * 60)
    print("🚀 Starting Crypto Dashboard")
    print("=" * 60)
    print(f"📊 Database: {POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}")
    print(f"🔄 Refresh interval: {REFRESH_INTERVAL}s")
    print(f"📈 Chart data limit: {CHART_DATA_LIMIT} points")
    print("=" * 60)
    print("\n✅ Dashboard available at: http://localhost:5000")
    print("📄 Pages:")
    print("   - Home (Real-time Prices): http://localhost:5000/")
    print("   - Charts: http://localhost:5000/charts")
    print("   - Alerts: http://localhost:5000/alerts")
    print("   - Statistics: http://localhost:5000/stats")
    print("\n🛑 Press Ctrl+C to stop")
    print("=" * 60)

    app.run(debug=True, host='0.0.0.0', port=5000)
