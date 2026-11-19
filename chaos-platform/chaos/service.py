"""服务框架:服务实例、下游调用封装(熔断/隔离/降级/超时)、全局开关、事件总线。"""
import threading
import time
from collections import deque
from urllib.parse import urlparse

from .core import (DownstreamError, HttpClient, JsonHandler, PooledHTTPServer,
                   ServiceMetrics)
from .resilience import (Bulkhead, BulkheadFullError, CircuitBreaker,
                         CircuitOpenError)

# 演练标记头,带此头的请求才会被"按标记"的注入规则命中,用于限制爆炸半径
CHAOS_TAG_HEADER = "X-Chaos-Tag"
REQUEST_ID_HEADER = "X-Request-Id"


class Settings:
    """全局开关:韧性模式 + 每条调用边的超时配置(ms)。"""

    def __init__(self):
        self.resilience = False
        self.timeouts = {}  # "caller->downstream" -> ms
        self._lock = threading.Lock()

    def set_resilience(self, on):
        with self._lock:
            self.resilience = bool(on)

    def is_resilience(self):
        with self._lock:
            return self.resilience

    def set_timeout(self, edge, ms):
        with self._lock:
            self.timeouts[edge] = int(ms)

    def timeout_for(self, caller, ds):
        with self._lock:
            return self.timeouts.get("%s->%s" % (caller, ds))


class EventBus:
    def __init__(self):
        self._subs = []
        self._lock = threading.Lock()
        self.history = deque(maxlen=300)

    def subscribe(self, fn):
        with self._lock:
            self._subs.append(fn)

    def emit(self, event):
        event.setdefault("ts", time.time())
        with self._lock:
            subs = list(self._subs)
        self.history.append(event)
        for fn in subs:
            try:
                fn(event)
            except Exception:
                pass


class Downstream:
    """对某个下游依赖的调用通道:连接池 + 舱壁 + 熔断器 + 降级。"""

    def __init__(self, caller, name, cfg, metrics, settings, bus):
        self.caller = caller
        self.name = name
        self.cfg = cfg
        self.metrics = metrics
        self.settings = settings
        self.client = HttpClient(cfg["endpoints"], cfg.get("pool_size", 10))
        breaker_cfg = dict(cfg.get("breaker") or {})
        self.breaker = CircuitBreaker(
            "%s->%s" % (caller, name),
            on_event=lambda _n, frm, to: bus.emit(
                {"type": "breaker", "edge": _n, "from": frm, "to": to}),
            **breaker_cfg)
        self.bulkhead = Bulkhead("%s->%s" % (caller, name),
                                 cfg.get("bulkhead_size", 5))
        self.fallback_fn = cfg.get("fallback")
        # 关闭韧性时的"拍脑袋"超时:30s,正是事故时的配置
        self.unprotected_timeout_s = cfg.get("unprotected_timeout_ms", 30000) / 1000.0

    def _m(self):
        return self.metrics.ds(self.name)

    def _fallback(self, m, payload, reason):
        if self.fallback_fn is not None:
            with m.lock:
                m.fallbacks += 1
            return 200, self.fallback_fn(payload)
        raise DownstreamError("call %s failed: %s" % (self.name, reason))

    def call(self, path, payload, headers=None):
        m = self._m()
        with m.lock:
            m.calls += 1
        t0 = time.time()
        if self.settings.is_resilience():
            timeout_ms = (self.settings.timeout_for(self.caller, self.name)
                          or self.cfg.get("timeout_ms", 2000))
            timeout_s = timeout_ms / 1000.0
            try:
                self.bulkhead.acquire()
            except BulkheadFullError:
                with m.lock:
                    m.bulkhead_rejected += 1
                return self._fallback(m, payload, "bulkhead full")
            try:
                self.breaker.acquire()
            except CircuitOpenError:
                self.bulkhead.release()
                with m.lock:
                    m.breaker_rejected += 1
                return self._fallback(m, payload, "circuit open")
            err = None
            try:
                status, obj = self.client.post_json(path, payload, headers, timeout_s)
            except DownstreamError as e:
                err = e
            dt = (time.time() - t0) * 1000
            m.lat.add(dt)
            self.breaker.release(err is None, dt)
            self.bulkhead.release()
            if err is None:
                return status, obj
            with m.lock:
                m.errors += 1
            return self._fallback(m, payload, str(err))
        else:
            try:
                status, obj = self.client.post_json(
                    path, payload, headers, self.unprotected_timeout_s)
                m.lat.add((time.time() - t0) * 1000)
                return status, obj
            except DownstreamError:
                m.lat.add((time.time() - t0) * 1000)
                with m.lock:
                    m.errors += 1
                raise

    def snapshot(self):
        m = self._m().snapshot()
        m.update({
            "breaker": self.breaker.state,
            "bulkhead_in_use": self.bulkhead.in_use,
            "bulkhead_size": self.bulkhead.size,
            "conn": self.client.stats(),
            "timeout_ms": (self.settings.timeout_for(self.caller, self.name)
                           or self.cfg.get("timeout_ms", 2000)),
        })
        return m


class CallContext:
    """单次请求的调用上下文,负责透传演练标记。"""

    def __init__(self, service, headers):
        self.service = service
        self.headers = headers

    def call(self, downstream, path, payload=None):
        ds = self.service.downstreams[downstream]
        fwd = {}
        for k in (CHAOS_TAG_HEADER, REQUEST_ID_HEADER):
            if k in self.headers:
                fwd[k] = self.headers[k]
        return ds.call(path, payload or {}, fwd)


class Service:
    def __init__(self, name, port, workers, queue_size, routes,
                 downstream_cfgs, settings, bus):
        self.name = name
        self.port = port
        self.settings = settings
        self.metrics = ServiceMetrics(name)
        self.downstreams = {
            ds_name: Downstream(name, ds_name, cfg, self.metrics, settings, bus)
            for ds_name, cfg in (downstream_cfgs or {}).items()
        }
        self.routes = routes  # {("POST", "/api/x"): fn(ctx, payload) -> (status, obj)}
        outer = self

        class H(JsonHandler):
            def do_POST(self):
                outer._handle(self)

            def do_GET(self):
                outer._handle(self)

        self.httpd = PooledHTTPServer(("127.0.0.1", port), H,
                                      workers=workers, queue_size=queue_size,
                                      name=name)

    def start(self):
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    def snapshot(self):
        snap = self.metrics.snapshot()
        snap["pool"] = self.httpd.pool_stats()
        snap["downstreams"] = {n: ds.snapshot() for n, ds in self.downstreams.items()}
        return snap

    def _handle(self, h):
        path = urlparse(h.path).path
        if path == "/__metrics":
            return h.send_json(200, self.snapshot())
        if path == "/health":
            return h.send_json(200, {"ok": True, "service": self.name})
        fn = self.routes.get((h.command, path))
        if fn is None:
            return h.send_json(404, {"error": "not found", "service": self.name})
        t0 = time.time()
        headers = {k: v for k, v in h.headers.items()}
        ctx = CallContext(self, headers)
        payload = h.read_json() if h.command == "POST" else {}
        try:
            status, obj = fn(ctx, payload)
        except DownstreamError as e:
            status, obj = 502, {"error": str(e), "service": self.name}
        except Exception as e:  # noqa: BLE001
            status, obj = 500, {"error": "%s: %s" % (type(e).__name__, e),
                                "service": self.name}
        self.metrics.record((time.time() - t0) * 1000, status)
        h.send_json(status, obj)
