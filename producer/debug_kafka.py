from kafka import KafkaConsumer

print(">>> DANG KET NOI...")
try:
    # Kết nối
    consumer = KafkaConsumer(
        bootstrap_servers=['localhost:9092'],
        request_timeout_ms=5000
    )
    
    # Gọi lệnh này để ép consumer lấy metadata về
    consumer.topics()
    
    print(">>> DANH SACH BROKER TRONG CLUSTER:")
    # Lấy danh sách các node trong mạng Kafka
    cluster = consumer._client.cluster
    for node in cluster.brokers():
        print(f"------------------------------------------------")
        print(f"ID: {node.nodeId}")
        print(f"HOST (QUAN TRONG): {node.host}")
        print(f"PORT: {node.port}")
        print(f"------------------------------------------------")
        
except Exception as e:
    print(f"LOI: {e}")