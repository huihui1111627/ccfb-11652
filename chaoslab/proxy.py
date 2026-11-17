"""故障注入代理：所有服务间 HTTP 调用都经过它，按规则注入故障。

能力：
- 故障类型 delay / error / abort，可多规则叠加；
- 爆炸半径：请求标记（Header）、指定实例、概率抽样三重过滤（需求 3）；
- 上游实例多目标轮询负载均衡；
- 管理 API 供编排器动态下发/撤销规则，撤销即恢复，无需重启任何服务；
- /proxy/metrics 暴露自身命中统计与转发延迟。
"""
from __future__ import annotations

import hashlib
import http.client
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlsplit

from chaoslab.model import FaultRule, rule_from_dict

# 用于指定实例的内部 Header（从 path 的 /svc/<name>/ 剥离后传给目标实例）
INSTANCE_HEADER = "X-Chaos-Instance"
TAG_HEADER_DEFAULT = "X-Chaos-Tag"


class RuleEngine:
    """线程安全的规则表 + 爆炸半径匹配。"""

    def __init__(self) -> None:
        self._rules: dict[str, FaultRule] = {}
        self._lock = threading.RLock()
        self.hit_counts: dict[str, int] = {}

    def upsert(self, rule: FaultRule) -> None:
        with self._lock:
            self._rules[rule.rule_id()] = rule
            self.hit_counts.setdefault(rule.rule_id(), 0)

    def remove(self, target: str, name: str) -> bool:
        with self._lock:
            return self._rules.pop(f"{target}:{name}", None) is not None

    def clear(self) -> None:
        with self._lock:
            self._rules.clear()
            self.hit_counts.clear()

    def list_rules(self) -> list[FaultRule]:
        with self._lock:
            return list(self._rules.values())

    @staticmethod
    def _sampled(rule: FaultRule, request_id: str) -> bool:
        """按概率稳定抽样：同一请求在重试下结果一致。"""
        if rule.percent >= 100:
            return True
        if rule.percent <= 0:
            return False
        digest = hashlib.md5(f"{rule.rule_id()}:{request_id}".encode()).hexdigest()
        return int(digest[:8], 16) % 100 < rule.percent

    def match(self, target: str, headers: dict[str, str],
              instance_id: str, request_id: str) -> list[FaultRule]:
        """返回命中的规则（可能多条，delay 会叠加）。"""
        hits: list[FaultRule] = []
        with self._lock:
            rules = list(self._rules.values())
        for rule in rules:
            if not rule.enabled or rule.target != target:
                continue
            # 爆炸半径 1：请求标记
            if rule.tag_header:
                if headers.get(rule.tag_header.lower()) != rule.tag_value:
                    continue
            # 爆炸半径 2：指定实例
            if rule.instances and instance_id not in rule.instances:
                continue
            # 爆炸半径 3：概率
            if not self._sampled(rule, request_id):
                continue
            hits.append(rule)
            self.hit_counts[rule.rule_id()] = self.hit_counts.get(rule.rule_id(), 0) + 1
        return hits


class ServiceRegistry:
    """服务名 → 实例列表（host, port, instance_id），支持轮询。"""

    def __init__(self, topology: dict[str, list[dict[str, str]]]):
        self._topology = topology
        self._rr: dict[str, int] = {}
        self._lock = threading.Lock()

    def pick(self, service: str, pinned_instance: str = "") -> dict[str, str] | None:
        instances = self._topology.get(service)
        if not instances:
            return None
        if pinned_instance:
            for inst in instances:
                if inst["instance_id"] == pinned_instance:
                    return inst
            return None
        with self._lock:
            idx = self._rr.get(service, 0) % len(instances)
            self._rr[service] = idx + 1
        return instances[idx]


class ProxyHandler(BaseHTTPRequestHandler):
    server_version = "ChaosLabProxy/1.0"

    # 安静一点的日志（默认会打到 stderr，演练时太吵）
    def log_message(self, fmt: str, *args: Any) -> None:
        if getattr(self.server, "verbose", False):
            super().log_message(fmt, *args)

    # ------------------------------------------------ 管理 API
    def _send_json(self, code: int, payload: Any) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _handle_admin(self, method: str) -> bool:
        path = urlsplit(self.path).path
        if not path.startswith("/proxy/"):
            return False
        engine: RuleEngine = self.server.rule_engine  # type: ignore[attr-defined]
        if path == "/proxy/rules" and method == "GET":
            self._send_json(200, {"rules": [r.__dict__ for r in engine.list_rules()]})
        elif path == "/proxy/rules" and method == "POST":
            length = int(self.headers.get("Content-Length", 0))
            data = json.loads(self.rfile.read(length) or b"{}")
            rule = rule_from_dict(data)
            engine.upsert(rule)
            self._send_json(200, {"ok": True, "rule_id": rule.rule_id()})
        elif path == "/proxy/rules" and method == "DELETE":
            engine.clear()
            self._send_json(200, {"ok": True})
        elif path.startswith("/proxy/rules/") and method == "DELETE":
            rest = path[len("/proxy/rules/"):]
            target, _, name = rest.partition("/")
            ok = engine.remove(target, name)
            self._send_json(200 if ok else 404, {"ok": ok})
        elif path == "/proxy/metrics" and method == "GET":
            self._send_json(200, {
                "hit_counts": dict(engine.hit_counts),
                "forwarded": self.server.fwd_stats,  # type: ignore[attr-defined]
            })
        elif path == "/proxy/health" and method == "GET":
            self._send_json(200, {"ok": True})
        else:
            self._send_json(404, {"ok": False, "error": "unknown admin path"})
        return True

    # ------------------------------------------------ 代理转发
    def _proxy(self, method: str) -> None:
        engine: RuleEngine = self.server.rule_engine  # type: ignore[attr-defined]
        registry: ServiceRegistry = self.server.registry  # type: ignore[attr-defined]
        parts = [p for p in urlsplit(self.path).path.split("/") if p]
        # 约定 path: /svc/<service>/<业务路径...>
        if len(parts) < 3 or parts[0] != "svc":
            self._send_json(400, {"error": "path 必须是 /svc/<service>/<...>"})
            return
        service = parts[1]
        upstream_path = "/" + "/".join(parts[2:])
        query = urlsplit(self.path).query
        if query:
            upstream_path += "?" + query

        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length) if length else b""
        req_headers = {k.lower(): v for k, v in self.headers.items()
                       if k.lower() not in ("host", "content-length",
                                            INSTANCE_HEADER.lower())}
        request_id = req_headers.get("x-request-id", f"r-{time.monotonic_ns()}")
        pinned = req_headers.get(INSTANCE_HEADER.lower(), "")
        instance = registry.pick(service, pinned)
        if instance is None:
            self._send_json(502, {"error": f"服务 {service} 无可用实例"})
            return
        instance_id = instance["instance_id"]

        rules = engine.match(service, req_headers, instance_id, request_id)
        # ---- abort：直接断连，模拟连接被重置
        if any(r.fault_type == "abort" for r in rules):
            self.close_connection = True
            try:
                self.wfile.close()
            except OSError:
                pass
            return
        # ---- error：不转发，直接回错误
        err_rules = [r for r in rules if r.fault_type == "error"]
        if err_rules:
            r0 = err_rules[0]
            self._send_json(r0.error_code, {
                "error": "injected", "rule": r0.name, "target": service,
                "instance": instance_id})
            return
        # ---- delay：所有 delay 规则叠加，再加抖动
        delay_ms = sum(r.delay_ms for r in rules if r.fault_type == "delay")
        jitter_ms = max((r.jitter_ms for r in rules if r.fault_type == "delay"), default=0)
        if delay_ms or jitter_ms:
            # 抖动用请求 id 派生，保证可复现
            j = 0
            if jitter_ms:
                digest = hashlib.md5(request_id.encode()).hexdigest()
                j = int(digest[:6], 16) % (jitter_ms + 1)
            time.sleep((delay_ms + j) / 1000.0)

        # ---- 正常转发
        start = time.monotonic()
        try:
            conn = http.client.HTTPConnection(instance["host"], int(instance["port"]),
                                              timeout=10)
            conn.request(method, upstream_path, body=body, headers=req_headers)
            resp = conn.getresponse()
            data = resp.read()
        except (OSError, http.client.HTTPException) as exc:
            self.server.fwd_stats["errors"] += 1  # type: ignore[attr-defined]
            self._send_json(502, {"error": f"上游连接失败: {exc}", "service": service})
            return
        elapsed_ms = (time.monotonic() - start) * 1000.0
        st = self.server.fwd_stats  # type: ignore[attr-defined]
        st["count"] += 1
        st["total_ms"] += elapsed_ms
        self.send_response(resp.status)
        for k, v in resp.getheaders():
            if k.lower() not in ("transfer-encoding", "connection", "content-length"):
                self.send_header(k, v)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("X-Chaos-Upstream", instance_id)
        self.end_headers()
        self.wfile.write(data)
        conn.close()

    def do_GET(self) -> None:
        if self._handle_admin("GET"):
            return
        self._proxy("GET")

    def do_POST(self) -> None:
        if self._handle_admin("POST"):
            return
        self._proxy("POST")

    def do_DELETE(self) -> None:
        if self._handle_admin("DELETE"):
            return
        self._send_json(405, {"error": "method not allowed"})


class ChaosProxy:
    """故障注入代理服务封装。"""

    def __init__(self, port: int, topology: dict[str, list[dict[str, str]]],
                 host: str = "127.0.0.1"):
        self.port = port
        self.host = host
        self.rule_engine = RuleEngine()
        self.registry = ServiceRegistry(topology)
        self._httpd: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        httpd = ThreadingHTTPServer((self.host, self.port), ProxyHandler)
        httpd.daemon_threads = True
        httpd.rule_engine = self.rule_engine  # type: ignore[attr-defined]
        httpd.registry = self.registry  # type: ignore[attr-defined]
        httpd.fwd_stats = {"count": 0, "errors": 0, "total_ms": 0.0}  # type: ignore[attr-defined]
        httpd.verbose = False  # type: ignore[attr-defined]
        self._httpd = httpd
        self._thread = threading.Thread(target=httpd.serve_forever,
                                        name="chaos-proxy", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if self._httpd:
            self._httpd.shutdown()
            self._httpd.server_close()

    # 供同进程编排器直接调用的便捷方法
    def put_rule(self, rule: FaultRule) -> None:
        self.rule_engine.upsert(rule)

    def retract_rule(self, target: str, name: str) -> bool:
        return self.rule_engine.remove(target, name)

    def clear_rules(self) -> None:
        self.rule_engine.clear()
