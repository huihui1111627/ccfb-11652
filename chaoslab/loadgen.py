"""负载生成器：按目标 RPS 和并发度持续打挂号入口流量。

线程池驱动，每 1/rps 秒投放一个请求；记录状态码、延迟、错误类型，
带 X-Chaos-Tag 标记的流量可用于爆炸半径验证。
"""
from __future__ import annotations

import json
import threading
import time
import urllib.request
from dataclasses import dataclass, field
from urllib.error import HTTPError, URLError

from chaoslab.resilience import DaemonThreadPoolExecutor


@dataclass
class LoadStats:
    total: int = 0
    success: int = 0
    errors: int = 0
    fallback_hits: int = 0          # 响应体里带 degraded 标记的数量
    latency_ms: list[float] = field(default_factory=list)
    status_codes: dict[int, int] = field(default_factory=dict)
    error_kinds: dict[str, int] = field(default_factory=dict)
    started_at: float = 0.0
    finished_at: float = 0.0

    def error_rate(self) -> float:
        return self.errors / self.total if self.total else 0.0

    def percentile(self, q: float) -> float:
        if not self.latency_ms:
            return 0.0
        data = sorted(self.latency_ms)
        idx = min(len(data) - 1, int(round(q * (len(data) - 1))))
        return data[idx]

    def summary(self) -> dict:
        return {
            "total": self.total, "success": self.success, "errors": self.errors,
            "error_rate": round(self.error_rate(), 4),
            "fallback_hits": self.fallback_hits,
            "latency_p50_ms": round(self.percentile(0.5), 1),
            "latency_p95_ms": round(self.percentile(0.95), 1),
            "latency_p99_ms": round(self.percentile(0.99), 1),
            "status_codes": {str(k): v for k, v in sorted(self.status_codes.items())},
            "error_kinds": dict(sorted(self.error_kinds.items())),
            "duration_s": round(max(0, self.finished_at - self.started_at), 1),
        }


class LoadGenerator:
    def __init__(self, url: str, rps: int = 40, concurrency: int = 24,
                 tag: str = "", timeout_s: float = 8.0):
        self.url = url
        self.rps = rps
        self.concurrency = concurrency
        self.tag = tag
        self.timeout_s = timeout_s
        self._stop = threading.Event()
        self._pool = DaemonThreadPoolExecutor(max_workers=concurrency,
                                              thread_name_prefix="load")
        self.stats = LoadStats()
        self._lock = threading.Lock()
        self._inflight = 0
        self._inflight_lock = threading.Lock()

    @property
    def inflight(self) -> int:
        return self._inflight

    def _one_request(self) -> None:
        headers = {"Content-Type": "application/json"}
        if self.tag:
            headers["X-Chaos-Tag"] = self.tag
        req = urllib.request.Request(
            self.url, data=json.dumps({"patient_id": "P-1001"}).encode(),
            headers=headers, method="POST")
        start = time.monotonic()
        status = 0
        kind = ""
        body = b""
        try:
            with self._inflight_guard():
                resp = urllib.request.urlopen(req, timeout=self.timeout_s)
                status = resp.status
                body = resp.read()
        except HTTPError as exc:
            status = exc.code
            kind = f"http_{exc.code}"
            try:
                body = exc.read()
            except Exception:
                pass
        except (URLError, OSError, TimeoutError) as exc:
            kind = type(exc).__name__
        elapsed = (time.monotonic() - start) * 1000.0
        degraded = False
        if body:
            try:
                payload = json.loads(body)
                degraded = "degraded" in json.dumps(payload, ensure_ascii=False)
            except json.JSONDecodeError:
                pass
        with self._lock:
            self.stats.total += 1
            self.stats.latency_ms.append(elapsed)
            self.stats.status_codes[status] = self.stats.status_codes.get(status, 0) + 1
            if 200 <= status < 400:
                self.stats.success += 1
            else:
                self.stats.errors += 1
                self.stats.error_kinds[kind or f"http_{status}"] = \
                    self.stats.error_kinds.get(kind or f"http_{status}", 0) + 1
            if degraded:
                self.stats.fallback_hits += 1

    def _inflight_guard(self):
        class _G:
            def __enter__(self_inner):
                with self._inflight_lock:
                    self._inflight += 1

            def __exit__(self_inner, *a):
                with self._inflight_lock:
                    self._inflight -= 1
        return _G()

    def run(self, duration_s: float) -> LoadStats:
        self.stats = LoadStats(started_at=time.time())
        interval = 1.0 / self.rps if self.rps > 0 else 0.025
        deadline = time.time() + duration_s
        futures = []
        try:
            while time.time() < deadline and not self._stop.is_set():
                futures.append(self._pool.submit(self._one_request))
                self._stop.wait(interval)
            # 收尾：给在途请求一点时间
            end_wait = time.time() + self.timeout_s
            for f in futures:
                remaining = max(0.1, end_wait - time.time())
                try:
                    f.result(timeout=remaining)
                except Exception:
                    pass
        finally:
            self.stats.finished_at = time.time()
        return self.stats

    def stop(self) -> None:
        self._stop.set()

    def shutdown(self) -> None:
        self._stop.set()
        self._pool.shutdown(wait=False)
