FROM python:3.13-slim

WORKDIR /app

# Basic setup
RUN apt-get update && \
    apt-get install -y curl netcat-traditional dnsutils iputils-ping && \
    rm -rf /var/lib/apt/lists/*

# Copy source code
COPY coinbase_kafka_producer/producer.py .
COPY coinbase_kafka_producer/requirements.txt .

# Install python dependencies
RUN pip install -r requirements.txt

# Start script + logs
RUN echo '#!/bin/bash\n\
echo "=== Kiểm tra kết nối Kafka ==="\n\
echo "Thử kết nối đến: $BOOTSTRAP_SERVERS"\n\
KAFKA_HOST=$(echo $BOOTSTRAP_SERVERS | cut -d: -f1)\n\
KAFKA_PORT=$(echo $BOOTSTRAP_SERVERS | cut -d: -f2)\n\
echo "Kiểm tra kết nối đến $KAFKA_HOST:$KAFKA_PORT..."\n\
timeout 60 bash -c "until nc -z $KAFKA_HOST $KAFKA_PORT 2>/dev/null; do echo \"Đang chờ Kafka khởi động...\"; sleep 5; done"\n\
if [ $? -eq 0 ]; then\n\
  echo "Kết nối thành công đến Kafka."\n\
else\n\
  echo "Warning: Không thể kết nối đến Kafka sau 60 giây!"\n\
  echo "Sẽ tiếp tục khởi động Producer nhưng có thể sẽ không kết nối được."\n\
fi\n\
\n\
echo "=== Khởi động Producer ==="\n\
python producer.py\n' > /app/start.sh && chmod +x /app/start.sh


ENTRYPOINT ["/app/start.sh"]