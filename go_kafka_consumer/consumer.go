package main

import (
    "bytes"
    "fmt"
    "log"
    "os"
    "time"

    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/credentials"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/s3"
    "github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
    // Kafka configuration from environment variables
    bootstrapServers := getEnv("BOOTSTRAP_SERVERS", "localhost:9092")
    
    c, err := kafka.NewConsumer(&kafka.ConfigMap{
        "bootstrap.servers": bootstrapServers,
        "group.id":          "s3-consumer-group",
        "auto.offset.reset": "earliest",
    })
    if err != nil {
        log.Fatalf("Failed to create consumer: %s", err)
    }
    defer c.Close()

    fmt.Printf("created Kafka consumer with bootstrap servers: %s\n", bootstrapServers)
    c.SubscribeTopics([]string{"coin-data"}, nil)
    
    // S3/MinIO configuration from environment variables
    region := getEnv("AWS_REGION", "ap-southeast-1")
    endpoint := getEnv("AWS_ENDPOINT", "")
    accessKey := getEnv("AWS_ACCESS_KEY_ID", "minioadmin")
    secretKey := getEnv("AWS_SECRET_ACCESS_KEY", "minioadmin")
    forcePathStyle := getEnv("AWS_S3_FORCE_PATH_STYLE", "true") == "true"
    bucketName := getEnv("S3_BUCKET", "ie212-coinbase-data")
    
    // Create AWS session for S3/MinIO
    awsConfig := &aws.Config{
        Region:           aws.String(region),
        Credentials:      credentials.NewStaticCredentials(accessKey, secretKey, ""),
        S3ForcePathStyle: aws.Bool(forcePathStyle),
    }
    
    // If endpoint is set, use it (for MinIO)
    if endpoint != "" {
        awsConfig.Endpoint = aws.String(endpoint)
    }
    
    sess, err := session.NewSession(awsConfig)
    if err != nil {
        log.Fatalf("Failed to create AWS session: %s", err)
    }
    
    s3Client := s3.New(sess)
    fmt.Printf("Connected sucessfully to S3/MinIO, bucket: %s\n", bucketName)
    
    timeout := 30 * time.Second
    
    // Poll messages from Kafka and write to S3/MinIO
    fmt.Println("Getting data from Kafka and write to S3/MinIO...")
    for {
        msg, err := c.ReadMessage(timeout)
        if err != nil {
            // Check timeout
            if err.(kafka.Error).Code() == kafka.ErrTimedOut {
                fmt.Println("Timed out, continuing...")
                continue
            }
            log.Printf("Consumer error: %v (%v)\n", err, msg)
            continue
        }
        
        // Create a unique S3 object key based on timestamp and product_id
        objectKey := fmt.Sprintf("coin-data/%d.json", time.Now().UnixNano())
        
        // Write message to S3/MinIO
        _, err = s3Client.PutObject(&s3.PutObjectInput{
            Bucket:      aws.String(bucketName),
            Key:         aws.String(objectKey),
            Body:        bytes.NewReader(msg.Value),
            ContentType: aws.String("application/json"),
        })
        
        if err != nil {
            log.Printf("Failed to write to S3/MinIO: %s", err)
            continue
        }
        
        fmt.Printf("Saved %s to S3/MinIO: %s\n", string(msg.Key), objectKey)
    }
}

// Helper function to get environment variables with defaults
func getEnv(key, defaultValue string) string {
    value := os.Getenv(key)
    if value == "" {
        return defaultValue
    }
    return value
}