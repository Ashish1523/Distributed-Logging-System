import requests
import json
import sys
import select

FLUENTD_LOG_URL = "http://localhost:9882/user.logs"
FLUENTD_HEARTBEAT_URL = "http://localhost:9882/user.heartbeats"

def accumulate_log(data):
    try:
        headers = {'Content-Type': 'application/json'}
        if data.get("message_type") == "HEARTBEAT":
            url = FLUENTD_HEARTBEAT_URL
        elif data.get("message_type") == "LOG":
            url = FLUENTD_LOG_URL
        elif data.get("message_type") == "REGISTRATION":
            url = FLUENTD_HEARTBEAT_URL
        else:
            print("[WARN] Unknown message type. Skipping...")
            return
        response = requests.post(url, data=json.dumps(data), headers=headers)
        if response.status_code == 200:
            print("[INFO] Log sent to Fluentd")
        else:
            print(f"[ERROR] Failed to send log: {response.status_code}")
    except Exception as e:
        print(f"[ERROR] {str(e)}")

def flush_remaining_logs():
    while True:
        if not select.select([sys.stdin], [], [], 0.1)[0]:
            break
        try:
            line = sys.stdin.readline()
            if line:
                data = json.loads(line.strip())
                accumulate_log(data)
        except Exception as e:
            print(f"[ERROR] {str(e)}")
            break

def main():
    try:
        while True:
            if select.select([sys.stdin], [], [], 0.1)[0]:
                line = sys.stdin.readline()
                if not line:
                    break  # EOI
                data = json.loads(line.strip())
                accumulate_log(data)
    except KeyboardInterrupt:
        print("[INFO] KeyboardInterrupt received. Flushing remaining logs...")
        flush_remaining_logs()
        print("[INFO] Log accumulator shutting down...")
    except Exception as e:
        print(f"[ERROR] {str(e)}")
    finally:
        sys.exit(0)

if __name__ == "__main__":
    main()
