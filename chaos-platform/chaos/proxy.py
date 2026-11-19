"""故障注入代理:挂在每个服务实例前面,按规则注入延迟/错误/断连。

规则匹配支持:目标实例、请求标记(X-Chaos-Tag)、路径前缀、百分比,
用于把爆炸半径限制在"带特定标记的请求"或"指定实例"内。
"""
import http.client
import random
import threading
import time
from http.server import ThreadingHTTPServer

from .core import JsonHandler
from .service import CHAOS_TAG_HEADER


class RuleManager:
    def __init__(self):
        self._rules = {}
        self._lock = threading.Lock()

    def upsert(self, rule):
        if not rule.get("id") or not rule.get("target"):
            raise ValueError("rule requires id and target")
        rule.setdefault("enabled", False)
        rule.setdefault("match", {})
        with self._lock:
            self._rules[rule["id"]] = dict(rule)

    def delete(self, rule_id):
        with self._lock:
            self._rules.pop(rule_id, None)

    def set_enabled(self, rule_id, flag):
        with self._lock:
            if rule_id in self._rules:
                self._rules[rule_id]["enabled"] = bool(flag)

    def disable_all(self):
        with self._lock:
            for r in self._rules.values():
                r["enabled"] = False

    def list(self):
        with self._lock:
            return [dict(r) for r in self._rules.values()]

    def active_for(self, instance):
        with self._lock:
            return [dict(r) for r in self._rules.values()
                    if r.get("enabled") and r.get("target") == instance]


def rule_matches(rule, instance, path, headers):
    m = rule.get("match") or {}
    if m.get("instance") and m["instance"] != instance:
        return False
    if m.get("tag") and headers.get(CHAOS_TAG_HEADER) != m["tag"]:
        return False
    if m.get("path_prefix") and not path.startswith(m["path_prefix"]):
        return False
    pct = m.get("percent", 100)
    if pct < 100 and random.random() * 100 >= pct:
        return False
    return True


class FaultProxy:
    """监听 listen_port,转发到 target(服务真实端口),转发前应用注入规则。"""

    def __init__(self, instance, listen_port, target_host, target_port, rules):
        self.instance = instance
        self.listen_port = listen_port
        self.target = (target_host, target_port)
        self.rules = rules
        outer = self

        class H(JsonHandler):
            def do_POST(self):
                outer._proxy(self)

            def do_GET(self):
                outer._proxy(self)

        self.httpd = ThreadingHTTPServer(("127.0.0.1", listen_port), H)
        self.httpd.daemon_threads = True
        self.httpd.handle_error = lambda *a, **k: None  # 客户端断开不刷堆栈

    def start(self):
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    def _proxy(self, h):
        body = h.read_body()
        headers = {k: v for k, v in h.headers.items()
                   if k.lower() not in ("content-length", "host", "connection")}
        for rule in self.rules.active_for(self.instance):
            if not rule_matches(rule, self.instance, h.path, h.headers):
                continue
            rtype = rule.get("type")
            if rtype == "latency":
                ms = rule.get("latency_ms", 0) + random.uniform(
                    0, rule.get("jitter_ms", 0))
                time.sleep(ms / 1000.0)
            elif rtype == "error":
                if random.random() < rule.get("error_rate", 1.0):
                    return h.send_json(rule.get("error_status", 500),
                                       {"error": "injected fault",
                                        "rule": rule["id"]})
            elif rtype == "abort":
                h.close_connection = True
                try:
                    h.connection.close()
                except OSError:
                    pass
                return
        try:
            conn = http.client.HTTPConnection(self.target[0], self.target[1],
                                              timeout=40)
            fwd_headers = dict(headers)
            if body:
                fwd_headers["Content-Length"] = str(len(body))
            conn.request(h.command, h.path, body=body or None, headers=fwd_headers)
            resp = conn.getresponse()
            data = resp.read()
            h.send_response(resp.status)
            for k, v in resp.getheaders():
                if k.lower() in ("transfer-encoding", "connection", "server", "date"):
                    continue
                h.send_header(k, v)
            h.end_headers()
            try:
                h.wfile.write(data)
            except (BrokenPipeError, ConnectionResetError, OSError):
                pass
            conn.close()
        except Exception as e:  # noqa: BLE001
            h.send_json(502, {"error": "proxy upstream error: %s" % e})
