# Initialize Minikube with Sufficient Resources
minikube start --memory=6144 --cpus=3 --disk-size=30g

# Stop Minikube 
minikube stop

# Start Minikube and Set Namespace
minikube start
kubectl config set-context --current --namespace=crypto-bigdata

# Verify Cluster Resources and Status
kubectl get pods
kubectl top pods
kubectl top nodes
kubectl get svc
kubectl get pvc

# Access Spark Master Pod and Launch PySpark Shell
kubectl exec -it spark-master-5f778b99f7-rpd6z -- bash
export PATH=$PATH:/opt/spark/bin
export SPARK_HOME=/opt/spark
pyspark \
  --master spark://spark-master:7077 \
  --conf spark.driver.host=$(hostname -i) \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.executor.memory=512m \
  --conf spark.executor.memoryOverhead=128m \
  --conf spark.cores.max=1

# Port Forwarding
kubectl port-forward kafka-0 9092:9092
kubectl port-forward deployment/spark-master 8080:8080
kubectl port-forward namenode-0 9870:9870
kubectl port-forward pod/postgres-0 5433:5432
minikube dashboard

python producer/main.py

# Create Kafka Topic
kubectl exec -it kafka-0 -- kafka-topics --delete --topic crypto-prices --bootstrap-server localhost:9092
kubectl exec -it kafka-0 -- kafka-topics \
  --create \
  --topic crypto-prices \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1

# Deploy Spark Streaming Application
# Must run in powershell or command prompt(NOT in Git Bash)
kubectl exec -n crypto-bigdata spark-master-5f778b99f7-rpd6z -- mkdir -p /tmp/spark-apps/common
kubectl exec -n crypto-bigdata spark-master-5f778b99f7-rpd6z -- mkdir -p /tmp/spark-apps/batch
kubectl exec -n crypto-bigdata spark-master-5f778b99f7-rpd6z -- mkdir -p /tmp/spark-apps/streaming

kubectl cp spark-apps/common/postgres_config.py spark-master-5f778b99f7-rpd6z:/tmp/spark-apps/common/
kubectl cp spark-apps/streaming/config.py spark-master-5f778b99f7-rpd6z:/tmp/spark-apps/streaming/
kubectl cp spark-apps/streaming/realtime_processor.py spark-master-5f778b99f7-rpd6z:/tmp/spark-apps/streaming/
kubectl cp spark-apps/batch/batch_processor.py crypto-bigdata/spark-master-5f778b99f7-rpd6z:/tmp/spark-apps/batch/
kubectl cp spark-apps/batch/hdfs_reader.py crypto-bigdata/spark-master-5f778b99f7-rpd6z:/tmp/spark-apps/batch/

# Alternative method
kubectl cp spark-apps/common/postgres_config.py spark-master-5f778b99f7-rpd6z:/tmp/spark-apps/common/postgres_config.py
kubectl cp spark-apps/batch/batch_processor.py crypto-bigdata/spark-master-5f778b99f7-rpd6z:/tmp/spark-apps/batch/batch_processor.py
kubectl cp spark-apps/batch/hdfs_reader.py crypto-bigdata/spark-master-5f778b99f7-rpd6z:/tmp/spark-apps/batch/hdfs_reader.py
kubectl cp spark-apps/streaming/realtime_processor.py spark-master-5f778b99f7-rpd6z:/tmp/spark-apps/streaming/realtime_processor.py

kubectl exec -it -n crypto-bigdata spark-master-5f778b99f7-rpd6z -- bash

/opt/spark/bin/spark-submit \
   --master local[2] \
   --name "CryptoProcessor" \
   --driver-memory 512M \
   --conf spark.driver.maxResultSize=200M \
   --conf spark.jars.ivy=/tmp/.ivy2 \
   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1 \
   /tmp/spark-apps/streaming/realtime_processor.py

# Run Streamlit Dashboard
streamlit run dashboard.py

# Submit Batch Processing Job
/opt/spark/bin/spark-submit \
  --master local[2] \
  --name "BatchProcessor" \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.postgresql:postgresql:42.7.1 \
  /tmp/spark-apps/batch/batch_processor.py

# Test HDFS Reader Script
/opt/spark/bin/spark-submit --master local[2] /tmp/spark-apps/batch/hdfs_reader.py