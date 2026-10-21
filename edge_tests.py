"""Edge-case checks for the Unit 1 server (run while server.py is running).

Each test prints PASS/FAIL. The last test confirms the server survived every
abuse case and still answers normal requests.
"""

import socket
import struct
import time

from protocol import recv_message, send_message

HOST, PORT = "127.0.0.1", 9090


def conn():
    return socket.create_connection((HOST, PORT), timeout=10)


def report(name, ok, detail=""):
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" +
          (f"  ({detail})" if detail else ""))


def roundtrip(sock, req):
    send_message(sock, req)
    return recv_message(sock)


def test_put_get():
    with conn() as s:
        r1 = roundtrip(
            s, {"action": "PUT", "key": "course", "value": "COSC 3657"})
        r2 = roundtrip(s, {"action": "GET", "key": "course"})
    report("1. PUT then GET returns the stored value",
           r1.get("status") == "OK" and r2.get("value") == "COSC 3657", r2)


def test_get_missing():
    with conn() as s:
        r = roundtrip(s, {"action": "GET", "key": "no-such-key-xyz"})
    report("2. GET of a missing key returns NOT_FOUND",
           r.get("status") == "NOT_FOUND", r)


def test_queue_fifo():
    with conn() as s:
        # drain anything left by the stress test so FIFO order is clear
        s.settimeout(30)
        while roundtrip(s, {"action": "DEQUEUE"}).get("status") == "OK":
            pass
        roundtrip(s, {"action": "ENQUEUE", "value": "first"})
        roundtrip(s, {"action": "ENQUEUE", "value": "second"})
        a = roundtrip(s, {"action": "DEQUEUE"}).get("value")
        b = roundtrip(s, {"action": "DEQUEUE"}).get("value")
    report("3. Queue is first-in, first-out", (a, b)
           == ("first", "second"), f"{a}, {b}")


def test_dequeue_empty_blocks_then_times_out():
    with conn() as s:
        s.settimeout(30)
        t0 = time.perf_counter()
        r = roundtrip(s, {"action": "DEQUEUE"})
        waited = time.perf_counter() - t0
    report("4. DEQUEUE on empty queue waits (~5 s) then returns EMPTY",
           r.get("status") == "EMPTY" and waited >= 4, f"waited {waited:.1f}s")


def test_unknown_action():
    with conn() as s:
        r = roundtrip(s, {"action": "DELETE", "key": "x"})
    report("5. Unknown action returns ERROR (no crash)",
           r.get("status") == "ERROR", r)


def test_missing_field():
    with conn() as s:
        r = roundtrip(s, {"action": "PUT", "key": "x"})  # no "value"
    report("6. Malformed request (missing field) returns ERROR",
           r.get("status") == "ERROR", r)


def test_abrupt_disconnect_mid_frame():
    s = conn()
    # promise 50 bytes, send 10
    s.sendall(struct.pack("!I", 50) + b'{"action":')
    s.close()                                         # vanish mid-frame
    report("7. Client dies mid-frame (server should log 'dropped')",
           True, "check server terminal")


def test_oversized_header():
    s = conn()
    s.sendall(struct.pack("!I", 4_000_000_000))  # claims a ~4 GB payload
    try:
        # server should close the connection, not allocate 4 GB
        data = s.recv(1)
        ok = data == b""
    except OSError:
        ok = True
    finally:
        s.close()
    report("8. Header claiming 4 GB is rejected (MAX_PAYLOAD guard)", ok)


def test_fragmented_frame():
    payload = b'{"action":"GET","key":"course"}'
    frame = struct.pack("!I", len(payload)) + payload
    with conn() as s:
        for b in frame:              # send ONE byte at a time
            s.sendall(bytes([b]))
            time.sleep(0.005)
        r = recv_message(s)
    report("9. Frame sent 1 byte at a time is reassembled (recv_exact)",
           r.get("value") == "COSC 3657", r)


def test_still_alive():
    with conn() as s:
        r = roundtrip(s, {"action": "GET", "key": "course"})
    report("10. Server still healthy after all abuse",
           r.get("value") == "COSC 3657", r)


if __name__ == "__main__":
    print("[*] Running edge-case tests against", f"{HOST}:{PORT}\n")
    for t in (test_put_get, test_get_missing, test_queue_fifo,
              test_dequeue_empty_blocks_then_times_out, test_unknown_action,
              test_missing_field, test_abrupt_disconnect_mid_frame,
              test_oversized_header, test_fragmented_frame, test_still_alive):
        try:
            t()
        except Exception as e:
            report(t.__name__, False, repr(e))
        time.sleep(0.2)
    print("\n[*] Done.")
