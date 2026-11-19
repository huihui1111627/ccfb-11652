"""基础设施:有界线程池HTTP服务器、带连接池的HTTP客户端、延迟统计窗口。

只用标准库。PooledHTTPServer 模拟 Tomcat 类容器:固定工作线程 + 有界等待队列,
队列满直接 503,用于复现"下游慢了 -> 线程全卡住 -> 队列满 -> 服务不可用"的事故链路。
"""
import collections
import http.client
import json
import queue
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer


class JsonHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "chaos"

    def log_message(self, fmt, *args):
        pass

    def send_json(self, status, obj):
        body = json.dumps(obj, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass

    def read_body(self):
        n = int(self.headers.get("Content-Length") or 0)
        return self.rfile.read(n) if n > 0 else b""

    def read_json(self):
        raw = self.read_body()
        if not raw:
            return {}
        try:
            return json.loads(raw.decode())
        except Exception:
            return {}


class PooledHTTPServer(HTTPServer):
    """固定大小工作线程池 + 有界任务队列的 HTTP 服务器。"""

    request_queue_size = 256
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, addr, handler_cls, workers=20, queue_size=100, name=""):
        self.pool_name = name
        self.pool_size = workers
        self.pool_queue_size = queue_size
        self._q = queue.Queue(queue_size)
        self._active = 0
        self._rejected = 0
        self._lock = threading.Lock()
        super().__init__(addr, handler_cls)
        for _ in range(workers):
            threading.Thread(target=self._worker, daemon=True).start()

    def process_request(self, request, client_address):
        try:
            self._q.put_nowait((request, client_address))
        except queue.Full:
            with self._lock:
                self._rejected += 1
            body = b"thread pool exhausted"
            try:
                request.sendall(
                    b"HTTP/1.1 503 Service Unavailable\r\nContent-Type: text/plain\r\n"
                    b"Content-Length: " + str(len(body)).encode()
                    + b"\r\nConnection: close\r\n\r\n" + body)
            except OSError:
                pass
            self.shutdown_request(request)

    def _worker(self):
        while True:
            request, client_address = self._q.get()
            with self._lock:
                self._active += 1
            try:
                self.finish_request(request, client_address)
            except Exception:
                try:
                    self.handle_error(request, client_address)
                except Exception:
                    pass
            finally:
                self.shutdown_request(request)
                with self._lock:
                    self._active -= 1

    def handle_error(self, request, client_address):
        pass  # 客户端超时断开属正常现象,不刷堆栈

    def pool_stats(self):
        with self._lock:
            return {"size": self.pool_size, "active": self._active,
                    "queued": self._q.qsize(), "queue_size": self.pool_queue_size,
                    "rejected": self._rejected}


class LatencyWindow:
    """带时间戳的延迟样本窗口,用于分位数统计。"""

    def __init__(self, cap=4096):
        self._d = collections.deque(maxlen=cap)
        self._lock = threading.Lock()

    def add(self, ms):
        with self._lock:
            self._d.append((time.time(), ms))

    def snapshot(self, since=None):
        with self._lock:
            vals = sorted(ms for ts, ms in self._d if since is None or ts >= since)
        if not vals:
            return {"count": 0, "p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0}

        def pct(p):
            return round(vals[min(len(vals) - 1, int(p * len(vals)))], 1)

        return {"count": len(vals), "p50": pct(0.5), "p95": pct(0.95),
                "p99": pct(0.99), "max": round(vals[-1], 1)}


class DownstreamMetrics:
    def __init__(self):
        self.calls = 0
        self.errors = 0
        self.fallbacks = 0
        self.breaker_rejected = 0
        self.bulkhead_rejected = 0
        self.lat = LatencyWindow()
        self.lock = threading.Lock()

    def snapshot(self):
        with self.lock:
            return {"calls": self.calls, "errors": self.errors,
                    "fallbacks": self.fallbacks,
                    "breaker_rejected": self.breaker_rejected,
                    "bulkhead_rejected": self.bulkhead_rejected,
                    "lat": self.lat.snapshot()}


class ServiceMetrics:
    def __init__(self, name):
        self.name = name
        self.requests = 0
        self.errors = 0
        self.lat = LatencyWindow()
        self.downstreams = {}
        self._lock = threading.Lock()

    def ds(self, name):
        with self._lock:
            if name not in self.downstreams:
                self.downstreams[name] = DownstreamMetrics()
            return self.downstreams[name]

    def record(self, lat_ms, status):
        with self._lock:
            self.requests += 1
            if status >= 500:
                self.errors += 1
        self.lat.add(lat_ms)

    def snapshot(self):
        with self._lock:
            reqs, errs = self.requests, self.errors
            ds = {k: v.snapshot() for k, v in self.downstreams.items()}
        return {"name": self.name, "requests": reqs, "errors": errs,
                "lat": self.lat.snapshot(), "downstreams": ds}


class DownstreamError(Exception):
    def __init__(self, msg, status=None):
        super().__init__(msg)
        self.status = status


class HttpClient:
    """带连接占用信号量的 HTTP 客户端,模拟有界连接池。"""

    def __init__(self, endpoints, pool_size=10):
        self.endpoints = list(endpoints)  # [(host, port), ...]
        self.pool_size = pool_size
        self._sem = threading.BoundedSemaphore(pool_size)
        self._in_use = 0
        self._rr = 0
        self._lock = threading.Lock()

    def stats(self):
        with self._lock:
            return {"in_use": self._in_use, "size": self.pool_size}

    def post_json(self, path, payload, headers=None, timeout_s=30.0):
        if not self._sem.acquire(timeout=timeout_s):
            raise DownstreamError("connection pool exhausted")
        with self._lock:
            self._in_use += 1
            host, port = self.endpoints[self._rr % len(self.endpoints)]
            self._rr += 1
        try:
            body = json.dumps(payload or {}).encode()
            h = {"Content-Type": "application/json"}
            h.update(headers or {})
            conn = http.client.HTTPConnection(host, port, timeout=timeout_s)
            try:
                conn.request("POST", path, body=body, headers=h)
                resp = conn.getresponse()
                data = resp.read()
                status = resp.status
            finally:
                conn.close()
            if status >= 500:
                raise DownstreamError("downstream returned %d" % status, status=status)
            try:
                return status, json.loads(data.decode() or "{}")
            except Exception:
                return status, {}
        except (socket.timeout, TimeoutError):
            raise DownstreamError("downstream timeout")
        except DownstreamError:
            raise
        except (ConnectionError, http.client.HTTPException, OSError) as e:
            raise DownstreamError("connection error: %s" % e)
        finally:
            with self._lock:
                self._in_use -= 1
            self._sem.release()
