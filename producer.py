
import socket
import struct

def send_message(sock, command, topic, text):
    msg_bytes = text.encode('utf-8')
    topic_bytes = topic.encode('utf-8')
    topic_len = len(topic_bytes)
    payload = struct.pack("!B", topic_len) + topic_bytes + msg_bytes
    header = struct.pack("!IB", len(payload), command)
    sock.sendall(header + payload)
    resp_len_data = sock.recv(4)
    resp_len = struct.unpack("!I", resp_len_data)[0]
    resp_body = sock.recv(resp_len)
    return resp_body

def main():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(('127.0.0.1', 8888))
    
    for i in range(3):
        msg = f"Log message number {i}"
        response = send_message(sock, 1, msg)
        status = response[:2]
        offset = struct.unpack("!Q", response[2:])[0]
        print(f"sent: '{msg}' -> Status: {status}, Saved offset: {offset}")
    sock.close()
if __name__ == "__main__":
    main()