"""故障注入代理测试：规则引擎爆炸半径 + 端到端 delay/error/abort。"""
import http.client
import json
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from chaoslab.model import FaultRule
from chaoslab.proxy import ChaosProxy, RuleEngine


class _UpstreamHandler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_GET(self):
        body = json.dumps({"upstream": "ok", "path": self.path}).encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class RuleEngineTest(unittest.TestCase):
    def test_tag_filter(self):
        engine = RuleEngine()
        engine.upsert(FaultRule(name="r", target="sms", tag_header="X-Chaos-Tag",
                                tag_value="canary"))
        self.assertEqual(engine.match("sms", {"x-chaos-tag": "canary"},
                                      "sms-1", "req1"), engine.list_rules())
        self.assertEqual(engine.match("sms", {"x-chaos-tag": "normal"},
                                      "sms-1", "req2"), [])

    def test_instance_filter(self):
        engine = RuleEngine()
        engine.upsert(FaultRule(name="r", target="sms",
                                instances=["sms-2"]))
        self.assertEqual(len(engine.match("sms", {}, "sms-2", "r1")), 1)
        self.assertEqual(engine.match("sms", {}, "sms-1", "r2"), [])

    def test_percent_sampling_stable(self):
        engine = RuleEngine()
        engine.upsert(FaultRule(name="r", target="sms", percent=50))
        first = bool(engine.match("sms", {}, "sms-1", "stable-req"))
        # 同一请求多次匹配结果一致（稳定抽样，重试不漂移）
        for _ in range(5):
            self.assertEqual(
                bool(engine.match("sms", {}, "sms-1", "stable-req")), first)
        # 总体命中率大约在 50% 附近
        hits = sum(1 for i in range(200)
                   if engine.match("sms", {}, "sms-1", f"r{i}"))
        self.assertGreater(hits, 40)
        self.assertLess(hits, 160)

    def test_target_isolation(self):
        engine = RuleEngine()
        engine.upsert(FaultRule(name="r", target="sms"))
        self.assertEqual(engine.match("push", {}, "psh-1", "r1"), [])


class ProxyEndToEndTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # 上游
        cls.upstream = ThreadingHTTPServer(("127.0.0.1", 0), _UpstreamHandler)
        cls.up_port = cls.upstream.server_address[1]
        threading.Thread(target=cls.upstream.serve_forever, daemon=True).start()
        # 代理（固定测试端口，冲突时整个测试失败，比随机端口更直白）
        topo = {"sms": [{"instance_id": "sms-1", "host": "127.0.0.1",
                         "port": str(cls.up_port)}]}
        cls.proxy = ChaosProxy(18201, topo)
        cls.proxy.start()
        time.sleep(0.2)
        cls.proxy_port = 18201

    @classmethod
    def tearDownClass(cls):
        cls.proxy.stop()
        cls.upstream.shutdown()

    def _get(self, path, headers=None):
        conn = http.client.HTTPConnection("127.0.0.1", self.proxy_port, timeout=5)
        conn.request("GET", path, headers=headers or {})
        return conn.getresponse()

    def test_normal_forward(self):
        resp = self._get("/svc/sms/send")
        self.assertEqual(resp.status, 200)
        self.assertEqual(json.loads(resp.read())["upstream"], "ok")

    def test_delay_rule(self):
        self.proxy.put_rule(FaultRule(name="d", target="sms",
                                      fault_type="delay", delay_ms=500))
        start = time.monotonic()
        resp = self._get("/svc/sms/send")
        self.assertEqual(resp.status, 200)
        self.assertGreaterEqual(time.monotonic() - start, 0.45)
        self.proxy.clear_rules()

    def test_error_rule(self):
        self.proxy.put_rule(FaultRule(name="e", target="sms",
                                      fault_type="error", error_code=503))
        resp = self._get("/svc/sms/send")
        self.assertEqual(resp.status, 503)
        self.proxy.clear_rules()

    def test_abort_rule(self):
        import io
        import contextlib
        self.proxy.put_rule(FaultRule(name="a", target="sms",
                                      fault_type="abort"))
        # 主动断连会让服务端打印一次 I/O 异常堆栈，属于预期行为，静音
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(
                    (http.client.HTTPException, OSError, ConnectionError)):
                resp = self._get("/svc/sms/send")
                resp.read()
        self.proxy.clear_rules()

    def test_blast_radius_tag_only(self):
        self.proxy.put_rule(FaultRule(
            name="tag", target="sms", fault_type="error", error_code=500,
            tag_header="X-Chaos-Tag", tag_value="canary"))
        self.assertEqual(self._get("/svc/sms/send").status, 200)
        self.assertEqual(self._get(
            "/svc/sms/send", {"X-Chaos-Tag": "canary"}).status, 500)
        self.proxy.clear_rules()


if __name__ == "__main__":
    unittest.main()
