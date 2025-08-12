from kafka import KafkaConsumer, KafkaProducer
from elasticsearch import Elasticsearch
import json
import threading
import urllib3
from datetime import datetime

# Stop event for threads
stop_event = threading.Event()

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Elasticsearch configuration
es = Elasticsearch(
    ['https://localhost:9200'],
    basic_auth=('elastic', 'dVyE3EXuinLABpYjbg8j'),
    verify_certs=False
)

# Elasticsearch index name
INDEX_NAME = "service-logs"

# Elasticsearch index mapping
index_body = {
    "mappings": {
        "properties": {
            "log_id": {"type": "keyword"},
            "node_id": {"type": "keyword"},
            "log_level": {"type": "keyword"},
            "message_type": {"type": "keyword"},
            "message": {"type": "text"},
            "service_name": {"type": "keyword"},
            "timestamp": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
            "status": {"type": "keyword"},
            "response_time_ms": {"type": "integer"},
            "threshold_limit_ms": {"type": "integer"},
            "error_details": {
                "type": "nested",
                "properties": {
                    "error_code": {"type": "keyword"},
                    "error_message": {"type": "text"}
                }
            }
        }
    }
}

# Create index if it does not exist
if not es.indices.exists(index=INDEX_NAME):
    es.indices.create(index=INDEX_NAME, body=index_body)
    print(f"Index '{INDEX_NAME}' created.")

# Kafka producer for sending alerts
alert_producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def send_to_alerts(alert_message):
    """Send a message to the 'service-alerts' topic."""
    try:
        alert_producer.send('service-alerts', value=alert_message)
        # print(f"Alert sent to 'service-alerts': {alert_message}")
    except Exception as e:
        print(f"Error sending alert: {e}")

def preprocess_log(log):
    """Preprocess log to reformat the timestamp."""
    if "timestamp" in log:
        try:
            dt = datetime.strptime(log["timestamp"], "%Y-%m-%dT%H:%M:%S.%f")
            log["timestamp"] = dt.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError as e:
            print(f"Error parsing timestamp: {e}")
    return log

def send_to_elasticsearch(log):
    """Index a log message into Elasticsearch."""
    try:
        log = preprocess_log(log)
        response = es.index(index=INDEX_NAME, body=log)
        print(f"Log indexed to Elasticsearch: {response['result']}")
    except Exception as e:
        print(f"Error indexing log: {e}")

def handle_registration(log_message):
    """
    Handle Microservice Registration Message.
    - Index the message into Elasticsearch.
    - Send a Microservice Registry alert to the `service-alerts` topic.
    """
    # Create Microservice Registry message
    registry_message = {
        "message_type": "REGISTRATION",
        "node_id": log_message.get("node_id", "Unknown"),
        "service_name": log_message.get("service_name", "Unknown"),
        "status": "UP",  # Default status for registration
        "timestamp": log_message.get("timestamp", datetime.now().isoformat())
    }
    # Send the registry message to the alerts topic
    send_to_alerts(registry_message)
    # Index the registration log to Elasticsearch
    send_to_elasticsearch(log_message)

def consume_logs():
    """Consume log messages from the 'service-logs' topic."""
    log_consumer = KafkaConsumer(
        'service-logs',
        bootstrap_servers=['localhost:9092'],
        group_id='log-consumers',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=1000
    )
    print("Consuming logs:")
    try:
        while not stop_event.is_set():
            for message in log_consumer:
                log_message = message.value
                if stop_event.is_set():
                    break
                # Handle different log types
                if log_message.get("message_type") == "REGISTRATION":
                    handle_registration(log_message)
                elif log_message.get("log_level") in ["WARN", "ERROR"]:
                    send_to_alerts(log_message)
                send_to_elasticsearch(log_message)
    finally:
        log_consumer.close()
        print("Log consumer stopped.")

def consume_heartbeats():
    """Consume heartbeat messages from the 'service-heartbeats' topic."""
    heartbeat_consumer = KafkaConsumer(
        'service-heartbeats',
        bootstrap_servers=['localhost:9092'],
        group_id='heartbeat-consumers',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=1000
    )
    print("Consuming heartbeats:")
    down_count = 0
    try:
        while not stop_event.is_set():
            for message in heartbeat_consumer:
                heartbeat_message = message.value
                if stop_event.is_set():
                    break
                send_to_elasticsearch(heartbeat_message)
                if heartbeat_message.get("status") == "DOWN":
                    down_count += 1
                    if down_count >= 3:
                        stop_event.set()
                        break
    finally:
        heartbeat_consumer.close()
        print("Heartbeat consumer stopped.")

def send_shutdown_message():
    """Send a shutdown message to the 'service-alerts' topic."""
    try:
        shutdown_message = {"message_type": "Shut"}
        send_to_alerts(shutdown_message)
    except Exception as e:
        print(f"Error sending shutdown message: {e}")

if __name__ == "__main__":
    try:
        log_thread = threading.Thread(target=consume_logs, daemon=True)
        heartbeat_thread = threading.Thread(target=consume_heartbeats, daemon=True)

        log_thread.start()
        heartbeat_thread.start()

        # Monitor threads
        while log_thread.is_alive() or heartbeat_thread.is_alive():
            log_thread.join(timeout=1)
            heartbeat_thread.join(timeout=1)

    except KeyboardInterrupt:
        print("\nInterrupted. Shutting down.")
        stop_event.set()
    finally:
        # Clean up and send shutdown message
        send_shutdown_message()
        log_thread.join()
        heartbeat_thread.join()
        print("Exiting consumer.")
