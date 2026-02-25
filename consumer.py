import socket
import struct
import time

def fetch_message(sock, topic, offset):
    topic_bytes = topic.encode('utf-8')
    topic_len = len(topic_bytes)
    offset_bytes = struct.pack("!Q", offset)
    payload = struct.pack("!B", topic_len) + topic_bytes + offset_bytes
    
    header = struct.pack("!IB", len(payload), 2)
    sock.sendall(header + payload)
    
    resp_len_data = sock.recv(4)
    if not resp_len_data:
        return None, None
    resp_len = struct.unpack("!I", resp_len_data)[0]
    resp_body = sock.recv(resp_len)
    status = resp_body[:2]
    
    if status == b"OK":
        next_offset = struct.unpack("!Q", resp_body[2:10])[0]
        msg_data = resp_body[10:]
        return msg_data.decode('utf-8'), next_offset
    else:
        return None, offset

def main():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(('127.0.0.1', 8888))
    
    current_offset = 0
    print("CONSUMER STARTED. LISTENING.....")
    
    try:
        while True:
            msg, next_offset = fetch_message(sock, current_offset)
            
            if msg:
                print(f"RECEIVED FROM OFFSET {current_offset}: {msg}")
                current_offset = next_offset
            else:
                time.sleep(1)
                
    except KeyboardInterrupt:
        print("CLOSING CONSUMER.")
    finally:
        sock.close()

if __name__ == "__main__":
    main()