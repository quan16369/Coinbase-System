FROM golang:latest AS builder

WORKDIR /app

# Cài đặt librdkafka-dev để hỗ trợ confluent-kafka-go
RUN apt-get update && \
    apt-get install -y librdkafka-dev pkg-config && \
    rm -rf /var/lib/apt/lists/*

# Copy go.mod and go.sum first for better caching
COPY go_kafka_consumer/go.mod go_kafka_consumer/go.sum ./
RUN go mod download

# Copy source code
COPY go_kafka_consumer/consumer.go .

# Build with CGO enabled (confluent-kafka-go)
RUN CGO_ENABLED=1 GOOS=linux go build -o consumer ./consumer.go

FROM debian:bookworm-slim

# Dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ca-certificates \
    netcat-traditional \
    librdkafka1 \
    curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/consumer .

# Script khởi động
RUN echo '#!/bin/bash \n\
# Kiểm tra biến môi trường \n\
echo "=== Kiểm tra cấu hình ===" \n\
echo "Kafka Bootstrap Servers: ${BOOTSTRAP_SERVERS:-kafka:9092}" \n\
echo "S3 Endpoint: ${AWS_ENDPOINT:-AWS S3}" \n\
echo "S3 Bucket: ${S3_BUCKET:-ie212-coinbase-data}" \n\
echo "AWS Region: ${AWS_REGION:-ap-southeast-1}" \n\
\n\
# Kiểm tra kết nối Kafka \n\
echo "=== Kiểm tra kết nối Kafka ===" \n\
KAFKA_HOST=$(echo ${BOOTSTRAP_SERVERS:-kafka:9092} | cut -d: -f1) \n\
KAFKA_PORT=$(echo ${BOOTSTRAP_SERVERS:-kafka:9092} | cut -d: -f2) \n\
echo "Đang đợi Kafka tại $KAFKA_HOST:$KAFKA_PORT..." \n\
timeout 60 bash -c "until nc -z $KAFKA_HOST $KAFKA_PORT 2>/dev/null; do echo \"Đang đợi Kafka khởi động...\"; sleep 5; done" \n\
if [ $? -ne 0 ]; then \n\
  echo "CẢNH BÁO: Không thể kết nối đến Kafka sau 60 giây!" \n\
  echo "Consumer có thể sẽ không hoạt động cho đến khi Kafka sẵn sàng." \n\
fi \n\
\n\
# Kiểm tra kết nối MinIO/S3 \n\
if [ ! -z "$AWS_ENDPOINT" ] && [[ "$AWS_ENDPOINT" != "AWS S3" ]]; then \n\
  echo "=== Kiểm tra kết nối MinIO/S3 ===" \n\
  # Phân tích URL để lấy host và port \n\
  if [[ "$AWS_ENDPOINT" == http://* ]] || [[ "$AWS_ENDPOINT" == https://* ]]; then \n\
    # Loại bỏ phần giao thức (http:// hoặc https://) \n\
    S3_URL=$(echo "$AWS_ENDPOINT" | sed -e "s|^[^/]*//||") \n\
    # Phân tách host và port \n\
    S3_HOST=$(echo "$S3_URL" | cut -d: -f1) \n\
    S3_PORT=$(echo "$S3_URL" | cut -d: -f2) \n\
    \n\
    # Set default port nếu không có port được chỉ định \n\
    if [ "$S3_HOST" = "$S3_PORT" ]; then \n\
      if [[ "$AWS_ENDPOINT" == https://* ]]; then \n\
        S3_PORT=443 \n\
      else \n\
        S3_PORT=80 \n\
      fi \n\
    fi \n\
    \n\
    # Loại bỏ đường dẫn sau host:port nếu có \n\
    S3_PORT=$(echo "$S3_PORT" | cut -d/ -f1) \n\
    \n\
    echo "Đang đợi MinIO/S3 tại $S3_HOST:$S3_PORT..." \n\
    timeout 60 bash -c "until nc -z $S3_HOST $S3_PORT 2>/dev/null; do echo \"Đang đợi MinIO/S3 khởi động...\"; sleep 5; done" \n\
    if [ $? -ne 0 ]; then \n\
      echo "CẢNH BÁO: Không thể kết nối đến MinIO/S3 sau 60 giây!" \n\
      echo "Consumer có thể sẽ không hoạt động đúng nếu không có kết nối S3." \n\
    else \n\
      echo "Kết nối thành công đến MinIO/S3 tại $S3_HOST:$S3_PORT" \n\
    fi \n\
  else \n\
    echo "AWS_ENDPOINT không có định dạng http(s)://host:port, bỏ qua kiểm tra kết nối" \n\
  fi \n\
else \n\
  echo "=== Bỏ qua kiểm tra kết nối MinIO/S3 ===" \n\
  echo "Không tìm thấy biến AWS_ENDPOINT hoặc sử dụng AWS S3 mặc định" \n\
fi \n\
\n\
echo "=== Khởi động Consumer ===" \n\
./consumer' > /app/start.sh && chmod +x /app/start.sh

CMD ["/app/start.sh"]