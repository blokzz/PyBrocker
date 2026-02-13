import asyncio
import struct

header_format = "!IB"
header_size = struct.calcsize(header_format)

def handle_client(writer , reader):
    