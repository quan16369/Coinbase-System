# "IE212 Final: Real-Time Coinbase Data Pipeline for Advanced Cryptocurrency Price Forecasting"

## Project Overview

## Screenshot

![plot](https://i.imgur.com/arNNfss.png)
## Architecture

![architecture]()

The real-time data pipeline project facilitates the collection, processing, storage, and visualization of cryptocurrency market data from Coinbase. It comprises key components:

- **Coinbase WebSocket API**: This serves as the initial data source, providing real-time cryptocurrency market data streams, including trades and price changes.

- **Kafka Producer**: To efficiently manage data, a Python-based microservice functions as a Kafka producer. It collects data from the Coinbase WebSocket API and sends it to the Kafka broker.

- **Kafka Broker**: Kafka, an open-source distributed event streaming platform, forms the core of the data pipeline. It efficiently handles high-throughput, fault-tolerant, real-time data streams, receiving data from the producer and making it available for further processing.

- **Go Kafka Consumer**: Implemented in Go, the Kafka consumer pulls raw data from Kafka topics and stores them directly into HDFS (Hadoop Distributed File System). This step ensures robust and scalable storage of raw data.
- **AWS S3 (MinIO) for Raw Data Storage**: 

- **PySpark Structured Streaming for Data Processing**: Apache Spark, a powerful in-memory data processing framework, is chosen for real-time data processing. With Spark Structured Streaming, real-time transformations and computations are applied to incoming data streams, ensuring data is ready for storage.

- **Cassandra Database**:  
For long-term data storage, Apache Cassandra, a highly scalable NoSQL database known for its exceptional write and read performance, is employed. Cassandra serves as the solution for storing historical cryptocurrency market data. 
The data also flows into cryptocurrency predicting model from the Database, expecting to increase the Acuuracy for the model thanks to simultaneously new data added.

- **Grafana for Data Visualization**: To make data easily understandable, Grafana, an open-source platform for monitoring and observability, is utilized. Grafana queries data from Cassandra to create compelling real-time visualizations, providing insights into cryptocurrency market trends.

## Deployment
<!--
![kubernetes-pods](https://i.imgur.com/LacnL5c.png)
-->

## How to use
<p align="center">
  <img src="https://i.imgur.com/LU2iYUF.png" style="width: 600px"/>
</p>

Install Docker Desktop. After that, run: docker-compose up -d
## Future Work
* Perform code cleanup and integration testing
* Deploy to EKS
* Add monitoring and logging tools
* Perform more comprehensive analysis (like forecasting or sliding window avg)
