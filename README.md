#  PyBroker: Lightweight TCP Message Broker

PyBroker is a minimalist, high-performance message broker written entirely from scratch in Python, utilizing only the standard library. 

This project bypasses the overhead of HTTP and JSON, operating directly on raw TCP sockets with a custom-designed binary protocol. Data is persisted to disk using an **Append-Only Log** data structure, guaranteeing extremely fast sequential writes.



##  Key Features

* **Asynchronous Networking Engine:** Capable of handling thousands of concurrent connections using `asyncio` without blocking the main event loop.
* **Custom Binary Protocol (Wire Protocol):** Communication is based on precise byte packing (`struct`), significantly minimizing network bandwidth consumption.
* **Topic-based Routing:** Automatic partitioning of messages into distinct topics, generating separate `.log` files for each stream.
* **Disk-backed Persistence:** Durable disk storage utilizing a `ThreadPoolExecutor` to offload blocking I/O operations from the async event loop.
* **Offset Tracking:** A pointer-based reading system (offsets) allowing consumers to resume reading from a specific point in the byte stream.

##  Wire Protocol Architecture

Applications communicate with the broker by sending strictly formatted data frames over TCP.

**Header (5 Bytes):**
| Size | C Type | Description |
| :--- | :--- | :--- |
| 4 Bytes | `uint32` | Total length of the payload (Payload Length) |
| 1 Byte | `uint8` | Command type (1 = PUBLISH, 2 = FETCH) |

**Payload Structure (PUBLISH / FETCH):**
| Size | C Type | Description |
| :--- | :--- | :--- |
| 1 Byte | `uint8` | Length of the topic name (N) |
| N Bytes | `char[]` | Topic Name (e.g., "logs") |
| Remainder | `bytes` / `uint64` | The actual message payload (for PUBLISH) OR an 8-byte Offset (for FETCH) |

##  Getting Started

###  Running Locally (Requires Python 3.8+)

1. Start the broker server:
   ```bash
   python server.py

2. In a new terminal, start the consumer to begin listening:
   ```bash
   python consumer.py

3. In another terminal, run the producer to send data:
   ```bash
   python producer.py
