

"""Threaded TCP server on :9090. Main thread accepts; a bounded pool does the work.

Bounded pool (not thread-per-request) so a burst of clients can't spawn unlimited
threads and take the box down. Extra connections just queue.
"""

import socket
from concurrent.futures import ThreadPoolExecutor

from protocol import ProtocolError, recv_message, send_message
from storage import KeyValueStore, TaskQueue

HOST = "0.0.0.0"
PORT = 9090
MAX_WORKERS = 10
BACKLOG = 128
DEQUEUE_TIMEOUT = 5.0   # how long a DEQUEUE blocks on an empty queue


def process_request(req, kv, queue):
    action = req.get("action")
    if action == "PUT":
        kv.put(req["key"], req["value"])
        return {"status": "OK"}
    if action == "GET":
        val = kv.get(req["key"])
        return {"status": "OK", "value": val} if val is not None else {"status": "NOT_FOUND"}
    if action == "ENQUEUE":
        queue.enqueue(req["value"])
        return {"status": "OK"}
    if action == "DEQUEUE":
        item = queue.dequeue(timeout=DEQUEUE_TIMEOUT)
        return {"status": "OK", "value": item} if item is not None else {"status": "EMPTY"}
    return {"status": "ERROR", "message": f"unknown action: {action!r}"}


def handle_client(conn, addr, kv, queue):
    # Persistent connection: keep serving frames until the client goes away.
    with conn:
        try:
            while True:
                req = recv_message(conn)
                if req is None:
                    break                    # clean disconnect
                try:
                    resp = process_request(req, kv, queue)
                except (KeyError, TypeError) as e:
                    resp = {"status": "ERROR", "message": f"bad request: {e}"}
                send_message(conn, resp)
        except (ProtocolError, ConnectionError, OSError) as e:
            # one client's mess stays one client's problem
            print(f"[!] {addr} dropped: {e}")


def main():
    kv = KeyValueStore()
    queue = TaskQueue()
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv:
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR,
                       1)  # rebind fast after restart
        srv.bind((HOST, PORT))
        srv.listen(BACKLOG)
        print(f"[*] Server initialized on {HOST}:{PORT}")
        print(f"[*] Thread Pool initialized with max_workers={MAX_WORKERS}...")
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            try:
                while True:
                    conn, addr = srv.accept()
                    pool.submit(handle_client, conn, addr, kv, queue)
            except KeyboardInterrupt:
                print("\n[*] Shutting down, draining workers...")


if __name__ == "__main__":
    main()
