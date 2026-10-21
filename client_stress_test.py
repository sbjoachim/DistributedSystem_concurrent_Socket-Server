"""Fire 20 clients at the server at once and check nothing gets corrupted.

A Barrier holds every thread until all 20 have connected, then lets them go
together, so the server takes the hit under real contention. If framing or
locking were broken, a GET would read back the wrong value and that worker fails.
"""

import socket
import threading
import time

from protocol import recv_message, send_message

HOST = "127.0.0.1"
PORT = 9090
WORKERS = 20
OPS = 25            # put+get round trips per worker before it enqueues

gate = threading.Barrier(WORKERS)
results = [None] * WORKERS   # one slot per worker, each written by a single thread


def transaction(sock, wid):
    for i in range(OPS):
        key = f"worker-{wid:02d}-key-{i}"
        val = f"val-{wid:02d}-{i}"
        send_message(sock, {"action": "PUT", "key": key, "value": val})
        if recv_message(sock).get("status") != "OK":
            raise RuntimeError(f"PUT rejected for {key}")
        send_message(sock, {"action": "GET", "key": key})
        got = recv_message(sock).get("value")
        if got != val:
            raise RuntimeError(f"GET mismatch on {key}: got {got!r}, wanted {val!r}")
    send_message(sock, {"action": "ENQUEUE", "value": f"task-{wid:02d}"})
    if recv_message(sock).get("status") != "OK":
        raise RuntimeError("ENQUEUE rejected")


def worker(wid):
    try:
        with socket.create_connection((HOST, PORT)) as sock:
            gate.wait()             # everyone starts at the same instant
            transaction(sock, wid)
        results[wid] = True
        print(f"[Worker Thread {wid:02d}] Transaction verified successfully.")
    except Exception as e:
        results[wid] = False
        print(f"[Worker Thread {wid:02d}] FAILED: {e!r}")


def main():
    print(f"[*] Launching {WORKERS} concurrent client worker threads...")
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(WORKERS)]
    t0 = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.perf_counter() - t0

    passed = sum(1 for r in results if r)
    if passed == WORKERS:
        print(f"[+] All {WORKERS} workers completed execution in {elapsed:.3f} seconds.")
    else:
        print(f"[-] {WORKERS - passed} worker(s) FAILED; {passed}/{WORKERS} passed "
              f"in {elapsed:.3f} seconds.")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
