import time
import random
import json
from datetime import datetime
import sys

SERVICE_NAME = "SearchService"
NODE_ID = "search-node-01"
HEARTBEAT_INTERVAL = 5  # sec
node_status = "UP"
last_heartbeat_sent = datetime.now()

def send_message(message):
    print(json.dumps(message), flush=True)
    sys.stdout.flush()  


def generate_registration_message():
    registration_message = {
        "node_id": NODE_ID,
        "message_type": "REGISTRATION",
        "service_name": SERVICE_NAME,
        "timestamp": datetime.now().isoformat()
    }
    send_message(registration_message)

def generate_log_message():
    log_type = random.choice(["INFO", "WARN", "ERROR"])
    log_id = f"log-{datetime.now().strftime('%Y%m%d%H%M%S%f')}"
    timestamp = datetime.now().isoformat()
    if log_type == "INFO":
        message = {
            "log_id": log_id,
            "node_id": NODE_ID,
            "log_level": "INFO",
            "message_type": "LOG",
            "message": "Search query executed successfully",
            "service_name": SERVICE_NAME,
            "timestamp": timestamp
        }
    elif log_type == "WARN":
        message = {
            "log_id": log_id,
            "node_id": NODE_ID,
            "log_level": "WARN",
            "message_type": "LOG",
            "message": "Search query took longer than expected",
            "service_name": SERVICE_NAME,
            "response_time_ms": "2500",  
            "threshold_limit_ms": "2000",  
            "timestamp": timestamp
        }
    elif log_type == "ERROR":
        message = {
            "log_id": log_id,
            "node_id": NODE_ID,
            "log_level": "ERROR",
            "message_type": "LOG",
            "message": "Search query failed due to timeout",
            "service_name": SERVICE_NAME,
            "error_details": {
                "error_code": "QUERY_TIMEOUT",
                "error_message": "The search query exceeded the timeout limit"
            },
            "timestamp": timestamp
        }

    send_message(message)

def generate_heartbeat_message():
    global last_heartbeat_sent
    if random.choice([True, False]):  
        heartbeat_message = {
            "node_id": NODE_ID,
            "message_type": "HEARTBEAT",
            "status": node_status,
            "timestamp": datetime.now().isoformat()
        }
        send_message(heartbeat_message)
        last_heartbeat_sent = datetime.now()

def check_missing_heartbeat():
    time_since_last_heartbeat = (datetime.now() - last_heartbeat_sent).total_seconds()
    if time_since_last_heartbeat > HEARTBEAT_INTERVAL * 2: 
        missing_heartbeat_message = {
            "log_id": f"warn-{datetime.now().strftime('%Y%m%d%H%M%S%f')}",
            "node_id": NODE_ID,
            "log_level": "WARN",
            "message_type": "LOG",
            "message": f"Missing heartbeat for {time_since_last_heartbeat:.1f} seconds",
            "service_name": SERVICE_NAME,
            "timestamp": datetime.now().isoformat()
        }
        send_message(missing_heartbeat_message)

def shutdown_gracefully():
    global node_status
    node_status = "DOWN"

    shutdown_message = {
        "log_id": f"shutdown-{datetime.now().strftime('%Y%m%d%H%M%S%f')}",
        "node_id": NODE_ID,
        "log_level": "INFO",
        "message_type": "LOG",
        "message": "Service shutting down gracefully",
        "service_name": SERVICE_NAME,
        "timestamp": datetime.now().isoformat()
    }
    send_message(shutdown_message)
    
    final_heartbeat_message = {
        "node_id": NODE_ID,
        "message_type": "HEARTBEAT",
        "status": "DOWN",
        "timestamp": datetime.now().isoformat()
    }
    send_message(final_heartbeat_message)
def main():
    generate_registration_message()

    try:
        while True:
            generate_log_message()
            time.sleep(1)

            generate_heartbeat_message()
            time.sleep(HEARTBEAT_INTERVAL)

            check_missing_heartbeat()
    except KeyboardInterrupt:
        try:
            shutdown_gracefully()
        finally:
            sys.exit(0)

if __name__ == "__main__":
    main()