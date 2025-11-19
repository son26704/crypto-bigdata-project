POSTGRES_CONFIG = {
    'host': 'postgres.crypto-bigdata.svc.cluster.local',
    'port': 5432,
    'database': 'cryptodb',
    'user': 'cryptouser',
    'password': 'cryptopass123'
}
POSTGRES_JDBC_URL = f"jdbc:postgresql://{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}"
POSTGRES_PROPERTIES = {
    "user": POSTGRES_CONFIG['user'],
    "password": POSTGRES_CONFIG['password'],
    "driver": "org.postgresql.Driver"
}