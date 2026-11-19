"""熔断器与舱壁隔离。

熔断器:滑动窗口内统计错误率和慢调用比例,任一超阈值即断开;
断开 open_ms 后进入半开,放行 half_open 个探测请求,全部成功才闭合。
"""
import collections
import threading
import time


class CircuitOpenError(Exception):
    pass


class BulkheadFullError(Exception):
    pass


class CircuitBreaker:
    def __init__(self, name, failure_rate=0.5, slow_rate=0.5, slow_ms=1000,
                 window=20, min_calls=10, open_ms=8000, half_open=3,
                 on_event=None):
        self.name = name
        self.failure_rate = failure_rate
        self.slow_rate = slow_rate
        self.slow_ms = slow_ms
        self.window_size = window
        self.min_calls = min_calls
        self.open_ms = open_ms
        self.half_open = half_open
        self.on_event = on_event
        self.state = "CLOSED"
        self._window = collections.deque(maxlen=window)
        self._opened_at = 0.0
        self._half_inflight = 0
        self._half_ok = 0
        self._lock = threading.Lock()

    def reset(self):
        with self._lock:
            self.state = "CLOSED"
            self._window.clear()
            self._half_inflight = 0
            self._half_ok = 0

    def _transition(self, to):
        if self.state == to:
            return
        frm, self.state = self.state, to
        if self.on_event:
            try:
                self.on_event(self.name, frm, to)
            except Exception:
                pass

    def acquire(self):
        with self._lock:
            if self.state == "OPEN":
                if (time.time() - self._opened_at) * 1000 >= self.open_ms:
                    self._half_inflight = 0
                    self._half_ok = 0
                    self._transition("HALF_OPEN")
                else:
                    raise CircuitOpenError(self.name)
            if self.state == "HALF_OPEN":
                if self._half_inflight >= self.half_open:
                    raise CircuitOpenError(self.name)
                self._half_inflight += 1

    def release(self, ok, latency_ms):
        with self._lock:
            if self.state == "HALF_OPEN":
                self._half_inflight -= 1
                if not ok:
                    self._opened_at = time.time()
                    self._transition("OPEN")
                else:
                    self._half_ok += 1
                    if self._half_ok >= self.half_open:
                        self._window.clear()
                        self._transition("CLOSED")
                return
            if self.state != "CLOSED":
                return
            self._window.append((ok, latency_ms))
            n = len(self._window)
            if n >= self.min_calls:
                fails = sum(1 for ok_, _ in self._window if not ok_)
                slows = sum(1 for _, lat in self._window if lat >= self.slow_ms)
                if fails / n >= self.failure_rate or slows / n >= self.slow_rate:
                    self._opened_at = time.time()
                    self._transition("OPEN")


class Bulkhead:
    """舱壁:限制同一下游依赖最多占用的并发数,避免拖垮本服务线程池。"""

    def __init__(self, name, size):
        self.name = name
        self.size = size
        self._sem = threading.BoundedSemaphore(size)
        self.in_use = 0
        self.rejected = 0
        self._lock = threading.Lock()

    def acquire(self):
        if self._sem.acquire(blocking=False):
            with self._lock:
                self.in_use += 1
        else:
            with self._lock:
                self.rejected += 1
            raise BulkheadFullError(self.name)

    def release(self):
        with self._lock:
            self.in_use -= 1
        self._sem.release()
