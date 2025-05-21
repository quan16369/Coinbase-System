FROM python:3.10-slim

WORKDIR /app

# Copy model/Crypto-TS-Model-master directory
COPY model/Crypto-TS-Model-master /app/model/Crypto-TS-Model-master

# Copy prediction service
COPY prediction_real_time/prediction_service.py /app/prediction_real_time/prediction_service.py
COPY prediction_real_time/requirements.txt .
# Copy checkpoint and config
# Paths should match MODEL_CONFIG_PATH and MODEL_CHECKPOINT_PATH
COPY model/Crypto-TS-Model-master/configs /app/model/configs
COPY model/Crypto-TS-Model-master/checkpoints /app/model/checkpoints

# Install dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        python3-dev \
        libssl-dev \
        build-essential \
        netcat-traditional \
        procps \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages
RUN pip install -r requirements.txt

# Create startup script with correct checkpoint path
RUN echo '#!/bin/bash \n\
\n\
# Check environment variables \n\
echo "=== Configuration Check ===" \n\
echo "Cassandra Hosts: ${CASSANDRA_HOSTS:-cassandra}" \n\
echo "Cassandra Port: ${CASSANDRA_PORT:-9042}" \n\
echo "Cassandra Keyspace: ${CASSANDRA_KEYSPACE:-coinbase}" \n\
echo "Product IDs: ${PRODUCT_IDS:-BTC-USD,ETH-USD,XRP-USD}" \n\
echo "Prediction Interval: ${PREDICTION_INTERVAL:-5} minutes" \n\
echo "Model Config Path: ${MODEL_CONFIG_PATH:-/app/model/configs/train_config.yaml}" \n\
echo "Model Checkpoint Path: ${MODEL_CHECKPOINT_PATH:-/app/model/checkpoints/best_epoch_16.pt}" \n\
echo "Enable Health Check: ${ENABLE_HEALTH_CHECK:-false}" \n\
\n\
# Check Cassandra connection \n\
echo "=== Checking Cassandra Connection ===" \n\
CASSANDRA_HOST=$(echo ${CASSANDRA_HOSTS:-cassandra} | cut -d, -f1) \n\
CASSANDRA_PORT=${CASSANDRA_PORT:-9042} \n\
echo "Waiting for Cassandra at $CASSANDRA_HOST:$CASSANDRA_PORT..." \n\
while ! nc -z $CASSANDRA_HOST $CASSANDRA_PORT >/dev/null 2>&1; do \n\
  echo "Waiting for Cassandra to be ready..." \n\
  sleep 5 \n\
done \n\
echo "Cassandra is ready!" \n\
\n\
# Check if model files exist \n\
if [ ! -f "${MODEL_CONFIG_PATH:-/app/model/configs/train_config.yaml}" ]; then \n\
  echo "ERROR: Model config file not found at ${MODEL_CONFIG_PATH:-/app/model/configs/train_config.yaml}" \n\
  exit 1 \n\
fi \n\
\n\
if [ ! -f "${MODEL_CHECKPOINT_PATH:-/app/model/checkpoints/best_epoch_16.pt}" ]; then \n\
  echo "ERROR: Model checkpoint file not found at ${MODEL_CHECKPOINT_PATH:-/app/model/checkpoints/best_epoch_16.pt}" \n\
  exit 1 \n\
fi \n\
\n\
# Start prediction service \n\
echo "=== Starting Prediction Service ===" \n\
cd /app \n\
python -m prediction.service \n\
' > /app/start.sh && chmod +x /app/start.sh

# Expose port for health check if enabled
EXPOSE 8000

# Set health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

CMD ["/app/start.sh"]