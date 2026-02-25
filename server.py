import asyncio
import struct
import os

class LogManager:
    def __init__(self, data_dir="data"):
        self.data_dir = data_dir
        os.makedirs(self.data_dir, exist_ok=True) 
        self.open_files = {} 

    def _get_file_path(self, topic):
        return os.path.join(self.data_dir, f"{topic}.log")

    def write_message(self, topic, payload):
        path = self._get_file_path(topic)
        if topic not in self.open_files:
            self.open_files[topic] = open(path, "ab", buffering=0)
        
        f = self.open_files[topic]
        current_offset = f.tell()
        
        msg_len = len(payload)
        entry = struct.pack("!I", msg_len) + payload
        f.write(entry)
        
        return current_offset

    def read_message(self, topic, offset):
        path = self._get_file_path(topic)
        if not os.path.exists(path):
            return None
        try:
            with open(path, "rb") as f:
                f.seek(offset)
                length_data = f.read(4)
                if not length_data or len(length_data) < 4:
                    return None
                msg_len = struct.unpack("!I", length_data)[0]
                payload = f.read(msg_len)
                next_offset = offset + 4 + msg_len
                
                return payload, next_offset
        except FileNotFoundError:
            return None

header_format = "!IB"
header_size = struct.calcsize(header_format)
log_manager = LogManager()


async def handle_client(reader , writer):
    address = writer.get_extra_info('peername')
    print(f"ESTABLISHED CONNECTION WITH {address}")
    loop = asyncio.get_running_loop()
    try:
        while True:
            header = await reader.readexactly(header_size)
            mes_length ,command= struct.unpack(header_format ,header)

            payload = await reader.readexactly(mes_length)
            res = b""
            topic_length = payload[0] 
            topic_name = payload[1:1+topic_length].decode('utf-8')
            actual_data = payload[1+topic_length:]
            if command == 1:
                print(f"[{address}] PUBLISH: {len(actual_data)} BYTES")
                offset = await loop.run_in_executor(None, log_manager.write_message, topic_name, actual_data)
                res = b"OK" + struct.pack("!Q", offset)
            elif command ==2:
                if len(actual_data)!=8 :
                    res = b"ER_BAD_PAYLOAD"
                else:
                    requested_offset = struct.unpack("!Q", actual_data)[0]
                    print(f"[{address}] FETCH FROM OFFSET: {requested_offset}")
                    result = await loop.run_in_executor(None, log_manager.read_message, topic_name, requested_offset)
                    
                    if result:
                        msg_data, next_offset = result
                        res = b"OK" + struct.pack("!Q", next_offset) + msg_data
                    else:
                        res= b"NF"
            else:
                print(f"UNKNOWN COMMAND: {command}")
                res = b"ER"


            print(f"RECEIVED COMMAND FROM {address}:  {command} , PAYLOAD: {payload[:20]}...")
            writer.write(struct.pack("!I" ,len(res)) + res)
            await writer.drain()


    except asyncio.IncompleteReadError:
        print(f"CLIENT {address} DISCONNECTED")
    except Exception as e:
        print(f"ERROR: {e}")
    finally:
        writer.close()
        await writer.wait_closed()
        print(f"CONNECTION WITH {address} CLOSED")

async def main():
    server = await asyncio.start_server(handle_client , "127.0.0.1" , 8888)
    addrs = ', '.join(str(sock.getsockname()) for sock in server.sockets)
    print(f"SERVER IS RUNNING ON {addrs}")
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())