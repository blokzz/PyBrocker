import asyncio
import struct

header_format = "!IB"
header_size = struct.calcsize(header_format)

async def handle_client(writer , reader):
    address = writer.get_extra_info('peername')
    print(f"ESTABLISHED CONNECTION WITH {address}")
    try:
        while True:
            header = await reader.readexactly(header_size)
            mes_length ,command= struct.unpack(header_format ,header)

            payload = await reader.readexactly(mes_length)
            print(f"RECEIVED COMMAND FROM {address}:  {command} , PAYLOAD: {payload[:20]}...")
            res = b"ok"
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