"""Length-prefixed framing over a TCP byte stream.

Wire format: 4-byte big-endian unsigned length, then that many bytes of UTF-8 JSON.
TCP won't preserve message boundaries for us, so we carry the length ourselves.
"""

import json
import socket
import struct

HEADER = "!I"                       # big-endian uint32, the on-wire length field
HEADER_LEN = struct.calcsize(HEADER)  # 4
MAX_PAYLOAD = 8 * 1024 * 1024       # reject absurd lengths from a bad/hostile header


class ProtocolError(Exception):
    pass


def recv_exact(sock, n):
    """Read exactly n bytes. recv() can return short, so loop until we're full.

    Returns None on a clean disconnect right on a boundary; raises if the peer
    dies mid-frame. recv_into fills a preallocated buffer so we skip the
    per-chunk copy.
    """
    buf = bytearray(n)
    view = memoryview(buf)
    got = 0
    while got < n:
        r = sock.recv_into(view[got:])
        if r == 0:                  # peer closed
            if got == 0:
                return None         # nothing partial pending, this is a clean EOF
            raise ProtocolError(f"short frame: {got}/{n} bytes before EOF")
        got += r
    return bytes(buf)


def send_message(sock, obj):
    payload = json.dumps(obj, separators=(",", ":")).encode()
    if len(payload) > MAX_PAYLOAD:
        raise ProtocolError(f"payload too big: {len(payload)} bytes")
    # sendall handles partial writes for us; one call keeps the frame contiguous.
    sock.sendall(struct.pack(HEADER, len(payload)) + payload)


def recv_message(sock):
    header = recv_exact(sock, HEADER_LEN)
    if header is None:
        return None                 # client hung up between messages, fine
    (n,) = struct.unpack(HEADER, header)
    if not 0 < n <= MAX_PAYLOAD:
        raise ProtocolError(f"bad frame length: {n}")
    payload = recv_exact(sock, n)
    if payload is None:
        raise ProtocolError("EOF after header, before payload")
    return json.loads(payload)
