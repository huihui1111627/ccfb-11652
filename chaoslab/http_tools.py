"""服务框架与下游调用器（靶场服务的公共底座）。

- PooledHTTPServer：有界工作线程池的 HTTP 服务，池满直接 503，
  /metrics 暴露线程池占用——用于观察「线程如何被慢下游一层层占光」；
- HttpCaller：服务间调用统一出口，自带每下游连接池（信号量舱壁）、
  熔断器、超时、降级、延迟直方图，韧性开关可在线切换；
- Metrics：轻量计数器/仪表，进程内聚合，供采集器抓取。
"""
from __future__ import annotations

import json
import threading
import time
import uuid
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable
from urllib import request as urlrequest
from urllib.error import HTTPError, URLError

from chaoslab.resilience import (
    CircuitBreaker, BreakerConfig, Histogram, ResiliencePolicy,
    SemaphoreBulkhead, advise_timeouts, DaemonThreadPoolExecutor,
)

# 全局超时执行池（resilience 里也有一份，这里独立引用便于测试隔离）


class Metrics:
    """线程安全的指标容器：计数器 + 仪表 + 近期事件。"""

    def __init__(self) -> None:
        self._counters: dict[str, float] = {}
        self._gauges: dict[str, float] = {}
        self._lock = threading.Lock()
        self.events: deque[dict[str, Any]] = deque(maxlen=200)

    def incr(self, name: str, value: float = 1.0) -> None:
        with self._lock:
            self._counters[name] = self._counters.get(name, 0.0) + value

    def gauge(self, name: str, value: float) -> None:
        with self._lock:
            self._gauges[name] = value

    def add_event(self, event: dict[str, Any]) -> None:
        event.setdefault("at", time.time())
        self.events.append(event)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "counters": dict(self._counters),
                "gauges": dict(self._gauges),
                "events": list(self.events),
            }


# ---------------------------------------------------------------- 下游调用器
class HttpCaller:
    """服务间同步 HTTP 调用的统一出口，聚合一个服务的全部韧性组件。

    每个下游依赖独占：
    - 一个连接池（SemaphoreBulkhead，池满快速失败 → 连接池隔离）
    - 一个熔断器（错误率 + 慢调用比例双触发，half-open 探测恢复）
    - 一条超时（默认值可被分位数建议覆盖）
    - 一个可选降级函数
    mode='off' 时以上全部失效，退化为裸调（对照实验）。
    """

    def __init__(self, service_name: str, proxy_base: str,
                 event_sink: Callable[[str, dict], None] | None = None):
        self.service_name = service_name
        self.proxy_base = proxy_base.rstrip("/")
        self.policies: dict[str, ResiliencePolicy] = {}
        self.metrics = Metrics()
        self._event_sink = event_sink or (lambda evt, payload: None)
        # 调用结果明细（最近 1000 条），供报告做错误分类与传播分析
        self.call_log: deque[dict[str, Any]] = deque(maxlen=1000)
        self._log_lock = threading.Lock()

    def register_dependency(
        self, dep: str, *, timeout_s: float = 2.0, pool_size: int = 8,
        error_rate_threshold: float = 0.5, slow_rate_threshold: float = 0.5,
        slow_call_ms: float = 800.0, min_calls: int = 8,
        open_hold_s: float = 5.0, fallback: Callable[[Exception], Any] | None = None,
    ) -> None:
        def sink(evt: str, payload: dict) -> None:
            payload = {**payload, "service": self.service_name, "dependency": dep}
            self.metrics.add_event({"event": evt, **payload})
            self._event_sink(evt, payload)

        breaker = CircuitBreaker(BreakerConfig(
            name=f"{self.service_name}->{dep}",
            error_rate_threshold=error_rate_threshold,
            slow_rate_threshold=slow_rate_threshold,
            slow_call_ms=slow_call_ms, min_calls=min_calls,
            open_hold_s=open_hold_s), on_event=sink)
        bulkhead = SemaphoreBulkhead(pool_size, name=f"{self.service_name}->{dep}",
                                     on_event=sink)
        self.policies[dep] = ResiliencePolicy(
            name=dep, mode="on", timeout_s=timeout_s, breaker=breaker,
            bulkhead=bulkhead, fallback=fallback, histogram=Histogram())

    def set_mode(self, mode: str) -> None:
        """on/off 在线切换，控制台/编排器通过控制端点调用。"""
        for p in self.policies.values():
            p.mode = mode
            if mode == "on" and p.breaker is not None:
                p.breaker.reset()
        self.metrics.gauge("resilience_mode", 1.0 if mode == "on" else 0.0)
        self.metrics.add_event({"event": "mode_changed", "mode": mode})

    def suggest_timeouts(self) -> dict[str, float]:
        """需求 5：基于本服务观测到的各下游延迟分位数给出超时建议。"""
        return advise_timeouts({d: p.histogram for d, p in self.policies.items()})

    def apply_suggested_timeouts(self, headroom: float = 1.5) -> dict[str, float]:
        suggested: dict[str, float] = {}
        for dep, p in self.policies.items():
            t = p.histogram.suggest_timeout(0.95, headroom) / 1000.0
            if t > 0:
                p.timeout_s = round(t, 3)
                suggested[dep] = p.timeout_s
        return suggested

    def call(self, dep: str, path: str, *, method: str = "GET",
             body: bytes | None = None, headers: dict[str, str] | None = None,
             timeout_floor_s: float = 4.0) -> tuple[int, bytes]:
        """通过故障注入代理调用下游。返回 (status, body)，失败由策略决定降级还是抛出。

        timeout_floor_s：底层 socket 超时。即使韧性 off（无熔断/隔离/降级），
        真实系统也有一个偏宽的 socket 超时兜底，不会无限期挂死——
        设 4s 既保留"慢调用占线程"的故障传播效果，又保证故障撤销后线程能排空自愈。
        """
        policy = self.policies.get(dep)
        url = f"{self.proxy_base}/svc/{dep}/{path.lstrip('/')}"
        req_headers = {"X-Request-Id": headers.get("X-Request-Id", "")
                       if headers else ""}
        if headers:
            req_headers.update(headers)

        def do_call() -> tuple[int, bytes]:
            req = urlrequest.Request(url, data=body, method=method, headers=req_headers)
            # 舱壁的 acquire/release 已由 ResiliencePolicy.execute 统一处理；
            # off 模式下不启用隔离，直接裸调
            resp = urlrequest.urlopen(req, timeout=timeout_floor_s)
            return resp.status, resp.read()

        start = time.monotonic()
        status = 0
        ok = False
        err_kind = ""
        fallback_used = False
        try:
            if policy is None:
                result = do_call()
            else:
                result = policy.execute(do_call)
            # 降级函数返回 dict（兜底业务对象），正常调用返回 (status, body)；
            # 这里统一归一化为 (status, bytes)，降级按 200 成功处理
            if isinstance(result, dict):
                fallback_used = True
                result = (200, json.dumps(result, ensure_ascii=False).encode())
            status, _ = result
            ok = 200 <= status < 500
            if not ok:
                err_kind = f"http_{status}"
            return result
        except HTTPError as exc:
            status = exc.code
            err_kind = f"http_{exc.code}"
            raise
        except (URLError, OSError) as exc:
            err_kind = "connection_error"
            self._log(dep, path, start, 0, False, err_kind)
            self.metrics.incr(f"calls.{dep}.error.{err_kind}")
            raise
        except Exception as exc:
            err_kind = type(exc).__name__
            # 配了降级时 ResiliencePolicy 已吞掉异常返回兜底值，不会走到这里；
            # 走到这里说明没有降级或降级本身失败，错误继续向上传播
            if policy is not None and policy.fallback is not None:
                fallback_used = True
                ok = True  # 降级成功对调用方是成功
            raise
        finally:
            self._log(dep, path, start, status, ok, err_kind)
            if ok:
                self.metrics.incr(f"calls.{dep}.success")
            elif err_kind:
                self.metrics.incr(f"calls.{dep}.error.{err_kind}")
            if fallback_used:
                self.metrics.incr(f"calls.{dep}.fallback")

    def _log(self, dep: str, path: str, start_mono: float,
             status: int, ok: bool, err_kind: str) -> None:
        with self._log_lock:
            self.call_log.append({
                "ts": time.time(), "dep": dep, "path": path, "status": status,
                "latency_ms": round((time.monotonic() - start_mono) * 1000.0, 1),
                "ok": ok, "error": err_kind})

    def metrics_snapshot(self) -> dict[str, Any]:
        snap = self.metrics.snapshot()
        deps: dict[str, Any] = {}
        for dep, p in self.policies.items():
            assert p.breaker and p.bulkhead
            bs = p.breaker.snapshot()
            deps[dep] = {
                "mode": p.mode,
                "timeout_s": p.timeout_s,
                "breaker_state": bs.state,
                "breaker_error_rate": bs.error_rate,
                "breaker_slow_rate": bs.slow_rate,
                "breaker_rejected": bs.total_rejected,
                "pool_active": p.bulkhead.active,
                "pool_max": p.bulkhead.max_concurrent,
                "pool_saturation": round(p.bulkhead.saturation(), 3),
                "pool_rejected": p.bulkhead.rejected_total,
                "samples": p.histogram.count(),
                "p95_ms": p.histogram.percentile(0.95),
                "suggested_timeout_s": p.histogram.suggest_timeout() / 1000.0,
            }
        snap["dependencies"] = deps
        with self._log_lock:
            snap["recent_calls"] = list(self.call_log)[-20:]
        return snap


# ---------------------------------------------------------------- 服务框架
class _PooledServer(ThreadingHTTPServer):
    """带独立有界线程池的 HTTP Server：模拟 Web 容器工作线程池。"""

    def __init__(self, addr, handler, pool_size: int):
        super().__init__(addr, handler)
        self.pool = DaemonThreadPoolExecutor(max_workers=pool_size,
                                             thread_name_prefix="worker")
        self.pool_size = pool_size
        self.active = 0
        self._active_lock = threading.Lock()
        self.rejected = 0
        self.daemon_threads = True

    def process_request(self, request, client_address):  # type: ignore[override]
        def job():
            with self._active_lock:
                self.active += 1
            try:
                self.finish_request(request, client_address)
            except Exception:
                self.handle_error(request, client_address)
            finally:
                with self._active_lock:
                    self.active -= 1
                self.shutdown_request(request)

        if self.pool._work_queue.qsize() + self.active >= self.pool_size:
            # 线程池占满：快速失败而不是排队，把拒绝数暴露给指标
            self.rejected += 1
            self.shutdown_request(request)
            return
        self.pool.submit(job)

    def pool_saturation(self) -> float:
        with self._active_lock:
            return self.active / self.pool_size if self.pool_size else 0.0


class ServiceApp:
    """靶场服务基类：路由 + 池化服务器 + /metrics + /control 端点。"""

    def __init__(self, name: str, host: str, port: int, instance_id: str,
                 pool_size: int = 16, proxy_base: str = "http://127.0.0.1:8200"):
        self.name = name
        self.host = host
        self.port = port
        self.instance_id = instance_id
        self.caller = HttpCaller(f"{name}({instance_id})", proxy_base,
                                 event_sink=self._on_resilience_event)
        self.routes: dict[str, Callable[[dict], tuple[int, Any]]] = {}
        self.latency_budget_ms = 0.0   # 业务自身处理耗时（模拟）
        self._server: _PooledServer | None = None
        self.started_at = time.time()

    def route(self, path: str):
        def deco(fn):
            self.routes[path] = fn
            return fn
        return deco

    def _on_resilience_event(self, evt: str, payload: dict) -> None:
        # 事件同时进 caller 的 Metrics（sink 内部已做），这里留扩展点
        pass

    # ------------------------------------------------ 内部 HTTP 处理
    def make_handler(self):
        app = self

        class Handler(BaseHTTPRequestHandler):
            server_version = f"ChaosLab-{app.name}/1.0"

            def log_message(self, fmt, *args):
                pass

            def _reply(self, code: int, payload: Any) -> None:
                if isinstance(payload, (dict, list)):
                    body = json.dumps(payload, ensure_ascii=False).encode()
                    ctype = "application/json; charset=utf-8"
                else:
                    body = str(payload).encode()
                    ctype = "text/plain; charset=utf-8"
                self.send_response(code)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):
                self._dispatch("GET")

            def do_POST(self):
                self._dispatch("POST")

            def _dispatch(self, method: str) -> None:
                from urllib.parse import urlsplit
                parsed = urlsplit(self.path)
                path = parsed.path
                req_id = self.headers.get("X-Request-Id") or uuid.uuid4().hex[:12]
                ctx = {
                    "method": method, "path": path, "query": parsed.query,
                    "request_id": req_id,
                    "headers": {k: v for k, v in self.headers.items()},
                    "body": b"", "app": app,
                }
                length = int(self.headers.get("Content-Length", 0))
                if length:
                    ctx["body"] = self.rfile.read(length)

                # 框架内置端点
                if path == "/health":
                    self._reply(200, {"ok": True, "service": app.name,
                                      "instance": app.instance_id})
                    return
                if path == "/metrics":
                    self._reply(200, app.metrics_snapshot())
                    return
                if path == "/control":
                    self._handle_control(ctx)
                    return

                handler = app.routes.get(path)
                if handler is None:
                    self._reply(404, {"error": f"no route {path}"})
                    return
                app.caller.metrics.incr("requests.total")
                start = time.monotonic()
                try:
                    if app.latency_budget_ms:
                        time.sleep(app.latency_budget_ms / 1000.0)
                    code, payload = handler(ctx)
                    app.caller.metrics.incr(
                        "requests.success" if code < 500 else "requests.error")
                    self._reply(code, payload)
                except Exception as exc:  # 业务异常 → 500，故障向上冒泡的体现
                    app.caller.metrics.incr("requests.error")
                    app.caller.metrics.add_event({
                        "event": "request_failed", "path": path,
                        "error": type(exc).__name__, "message": str(exc)[:200]})
                    self._reply(500, {"error": type(exc).__name__,
                                      "message": str(exc)[:200],
                                      "service": app.name, "instance": app.instance_id,
                                      "request_id": req_id})
                finally:
                    app.caller.metrics.gauge(
                        "latency_last_ms", (time.monotonic() - start) * 1000.0)

            def _handle_control(self, ctx: dict) -> None:
                try:
                    data = json.loads(ctx["body"] or b"{}")
                except json.JSONDecodeError:
                    self._reply(400, {"error": "bad json"})
                    return
                action = data.get("action")
                if action == "set_mode":
                    app.caller.set_mode(data.get("mode", "on"))
                    self._reply(200, {"ok": True, "mode": data.get("mode")})
                elif action == "reset_breakers":
                    for p in app.caller.policies.values():
                        if p.breaker:
                            p.breaker.reset()
                    self._reply(200, {"ok": True})
                elif action == "suggest_timeouts":
                    self._reply(200, {"suggested": app.caller.suggest_timeouts()})
                elif action == "apply_timeouts":
                    self._reply(200,
                                {"applied": app.caller.apply_suggested_timeouts()})
                else:
                    self._reply(400, {"error": f"unknown action {action}"})

        return Handler

    def metrics_snapshot(self) -> dict[str, Any]:
        snap = self.caller.metrics_snapshot()
        assert self._server is not None
        snap["service"] = self.name
        snap["instance_id"] = self.instance_id
        snap["thread_pool"] = {
            "active": self._server.active,
            "size": self._server.pool_size,
            "saturation": round(self._server.pool_saturation(), 3),
            "rejected": self._server.rejected,
        }
        snap["uptime_s"] = round(time.time() - self.started_at, 1)
        return snap

    def start(self) -> None:
        self._server = _PooledServer((self.host, self.port),
                                     self.make_handler(), pool_size=16)
        self.caller.metrics.gauge("resilience_mode", 1.0)
        threading.Thread(target=self._server.serve_forever,
                         name=f"svc-{self.name}", daemon=True).start()

    def stop(self) -> None:
        if self._server:
            self._server.shutdown()
            self._server.pool.shutdown(wait=False)
            self._server.server_close()
