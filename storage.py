"""Shared server state: a KV store and a task queue, both concurrency-safe.

The KV store uses a reader-writer lock so concurrent GETs don't block each other
and only PUTs are exclusive. The queue uses a Condition so DEQUEUE can sleep
until an item shows up instead of spinning.
"""

import collections
import threading


class ReadWriteLock:
    """Writer-preferring RW lock. Built on one Condition (a Lock + wait/notify).

    Writer preference means a waiting writer blocks new readers, so a steady
    stream of readers can't starve it.
    """

    def __init__(self):
        self._cond = threading.Condition(threading.Lock())
        self._readers = 0
        self._writer = False
        self._writers_waiting = 0

    def acquire_read(self):
        with self._cond:
            while self._writer or self._writers_waiting:
                self._cond.wait()
            self._readers += 1

    def release_read(self):
        with self._cond:
            self._readers -= 1
            if self._readers == 0:
                self._cond.notify_all()

    def acquire_write(self):
        with self._cond:
            self._writers_waiting += 1
            while self._writer or self._readers:
                self._cond.wait()
            self._writers_waiting -= 1
            self._writer = True

    def release_write(self):
        with self._cond:
            self._writer = False
            self._cond.notify_all()


class KeyValueStore:
    def __init__(self):
        self._data = {}
        self._lock = ReadWriteLock()

    def put(self, key, value):
        self._lock.acquire_write()
        try:
            self._data[key] = value
        finally:
            self._lock.release_write()   # finally, or an exception deadlocks the store

    def get(self, key):
        self._lock.acquire_read()
        try:
            return self._data.get(key)   # None if missing
        finally:
            self._lock.release_read()

    def size(self):
        self._lock.acquire_read()
        try:
            return len(self._data)
        finally:
            self._lock.release_read()


class TaskQueue:
    def __init__(self):
        self._items = collections.deque()   # popleft/append are O(1)
        self._cond = threading.Condition()

    def enqueue(self, item):
        with self._cond:
            self._items.append(item)
            self._cond.notify()             # wake one waiting consumer

    def dequeue(self, timeout=None):
        with self._cond:
            # wait_for re-checks after every wakeup, so spurious wakeups and the
            # "two consumers, one item" race sort themselves out.
            if not self._cond.wait_for(lambda: self._items, timeout=timeout):
                return None                 # timed out on an empty queue
            return self._items.popleft()

    def size(self):
        with self._cond:
            return len(self._items)
