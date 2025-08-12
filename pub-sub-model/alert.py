from kafka import KafkaConsumer
import json

def consume_alerts():
    alert_consumer = KafkaConsumer(
        'service-alerts',
        bootstrap_servers=['localhost:9092'],
        group_id='alert-consumers',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    print("Consuming alerts:")
    try:
        for message in alert_consumer:
            alert_message = message.value
            # print(f"ALERT: {alert_message}")
            # Check for shutdown message
            if alert_message.get("message_type") == "Shut":
                print("Shutdown message received. Exiting alert consumer.")
                break
            else:
                print(f"ALERT: {alert_message}")
    except Exception as e:
        print(f"Error consuming alerts: {e}")
    finally:
        alert_consumer.close()
        print("Alert consumer stopped.")

if __name__ == "__main__":
    try:
        consume_alerts()
    except KeyboardInterrupt:
        print("\nInterrupted. Exiting.")
