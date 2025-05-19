import os
import json
import websocket
import threading
from kafka import KafkaProducer
from kafka.errors import KafkaError

TOPIC = "coin-data"
BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS", "localhost:9092")
print(f"Attempting to connect to Kafka at: {BOOTSTRAP_SERVERS}")
PRODUCT_IDS = ["ETH-USD", "BTC-USD", "XRP-USD"]
COINBASE_WS_URL = "wss://ws-feed.exchange.coinbase.com"

def create_producer():
    try:
        # Removed key_serializer to avoid None key encoding issues
        return KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=str.encode
        )
    except KafkaError as e:
        print(f"Failed to create Kafka producer: {e}")
        return None

def on_open(ws):
    print("WebSocket connection opened")
    subscribe_message = {
        "type": "subscribe",
        "product_ids": PRODUCT_IDS,
        "channels": ["ticker"]
    }
    try:
        ws.send(json.dumps(subscribe_message))
        print("Sent subscription request")
    except Exception as e:
        print(f"Error sending subscription: {e}")

def on_message(ws, message, producer, subscribed):
    # Check if message is valid
    if message is None or not isinstance(message, str) or not message.strip():
        print(f"Invalid message received: {repr(message)}")
        return

    try:
        data = json.loads(message)
        message_type = data.get("type")
        
        # Handle subscription confirmation
        if message_type == "subscriptions":
            print("Subscription successful")
            subscribed[0] = True
            return
            
        # Handle subscription errors
        if message_type == "error":
            print(f"Subscription failed: {message}")
            if producer:
                producer.close()
            ws.close()
            return
            
        # Process ticker messages
        if message_type == "ticker" and producer is not None:
            if message:
                product_id = data.get("product_id", "unknown")
                print(f"Sending {product_id} message to Kafka: {message[:100]}...")
                # Use product_id as key when sending to Kafka
                producer.send(TOPIC, key=product_id.encode(), value=message)
            else:
                print("Received empty message, not sending to Kafka")
        else:
            print(f"Ignored non-ticker message: {message[:100]}...")
    except json.JSONDecodeError:
        print(f"Failed to parse message as JSON: {message[:100]}...")
    except Exception as e:
        print(f"Error processing message: {e}")

def on_error(ws, error):
    print(f"WebSocket error: {error}")

def on_close(ws, close_status_code, close_msg):
    print(f"WebSocket connection closed: {close_status_code}, {close_msg}")

def main():
    producer = create_producer()
    if producer is None:
        print("Cannot proceed without Kafka producer")
        return
        
    subscribed = [False]
    ws = websocket.WebSocketApp(
        COINBASE_WS_URL,
        on_open=on_open,
        on_message=lambda ws, msg: on_message(ws, msg, producer, subscribed),
        on_error=on_error,
        on_close=on_close
    )
    
    ws_thread = threading.Thread(target=ws.run_forever)
    ws_thread.daemon = True
    ws_thread.start()
    
    try:
        print("Producer running. Press Ctrl+C to stop...")
        ws_thread.join()
    except KeyboardInterrupt:
        print("Shutting down...")
        if producer:
            producer.close()
        ws.close()

if __name__ == "__main__":
    main()