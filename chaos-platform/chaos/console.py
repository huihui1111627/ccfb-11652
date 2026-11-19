"""演练控制台:REST API + 静态页面。端口 8600。"""
import json
import os
import threading
import time
from http.server import ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from . import advisor as advisor_mod
from . import report as report_mod
from .core import JsonHandler
from .engine import check_steady

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")


class Console:
    def __init__(self, port, services, settings, rules, engine, collector,
                 loadgen, planstore, runstore, bus):
        self.port = port
        self.services = services
        self.settings = settings
        self.rules = rules
        self.engine = engine
        self.collector = collector
        self.loadgen = loadgen
        self.planstore = planstore
        self.runstore = runstore
        self.bus = bus
        outer = self

        class H(JsonHandler):
            def do_GET(self):
                outer._dispatch(self)

            def do_POST(self):
                outer._dispatch(self)

            def do_DELETE(self):
                outer._dispatch(self)

        self.httpd = ThreadingHTTPServer(("127.0.0.1", port), H)
        self.httpd.daemon_threads = True
        self.httpd.handle_error = lambda *a, **k: None

    def start(self):
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    # ------------------------------------------------------------ 工具

    def _serve_static(self, h, path):
        if path == "/":
            path = "/index.html"
        fs_path = os.path.normpath(os.path.join(STATIC_DIR, path.lstrip("/")))
        if not fs_path.startswith(STATIC_DIR) or not os.path.isfile(fs_path):
            return h.send_json(404, {"error": "not found"})
        ctype = {".html": "text/html; charset=utf-8",
                 ".js": "application/javascript",
                 ".css": "text/css"}.get(os.path.splitext(fs_path)[1],
                                         "application/octet-stream")
        with open(fs_path, "rb") as f:
            body = f.read()
        h.send_response(200)
        h.send_header("Content-Type", ctype)
        h.send_header("Content-Length", str(len(body)))
        h.end_headers()
        h.wfile.write(body)

    def _topology(self):
        svcs, edges = [], []
        for name, svc in self.services.items():
            snap = svc.snapshot()
            svcs.append({"name": name, "port": svc.port, "pool": snap["pool"],
                         "requests": snap["requests"], "errors": snap["errors"],
                         "lat": snap["lat"]})
            for ds_name in svc.downstreams:
                d = snap["downstreams"][ds_name]
                edges.append({"from": name, "to": ds_name, "calls": d["calls"],
                              "errors": d["errors"], "fallbacks": d["fallbacks"],
                              "breaker": d["breaker"], "lat": d["lat"],
                              "timeout_ms": d["timeout_ms"],
                              "bulkhead_in_use": d["bulkhead_in_use"],
                              "bulkhead_size": d["bulkhead_size"],
                              "conn": d["conn"]})
        return {"services": svcs, "edges": edges}

    def _live(self):
        run = self.engine.current
        run_info = None
        if run is not None:
            run_info = {"id": run["id"], "plan": run["plan_name"],
                        "resilience": run["resilience"],
                        "t": round(time.time() - run["t0"], 1),
                        "aborted": run["aborted"],
                        "events": run["events"][-30:],
                        "checks": run["checks"][-10:],
                        "steady_state": run["plan"].get("steady_state", [])}
        return {
            "resilience": self.settings.is_resilience(),
            "run": run_info,
            "rules": self.rules.list(),
            "client": self.loadgen.window_stats(5),
            "events": list(self.bus.history)[-20:],
        }

    # ------------------------------------------------------------ 路由

    def _dispatch(self, h):
        u = urlparse(h.path)
        path, qs = u.path, parse_qs(u.query)
        try:
            if path == "/" or path.startswith("/static/"):
                return self._serve_static(
                    h, path[len("/static"):] if path.startswith("/static/") else path)
            if h.command == "GET" and path == "/api/topology":
                return h.send_json(200, self._topology())
            if h.command == "GET" and path == "/api/live":
                return h.send_json(200, self._live())
            if h.command == "GET" and path == "/api/plans":
                return h.send_json(200, self.planstore.list())
            if h.command == "GET" and path.startswith("/api/plans/"):
                name = path.rsplit("/", 1)[1]
                return h.send_json(200, self.planstore.get(name))
            if h.command == "POST" and path == "/api/plans":
                plan = h.read_json()
                if not plan.get("name"):
                    return h.send_json(400, {"error": "plan requires name"})
                self.planstore.save(plan)
                return h.send_json(200, {"ok": True})
            if h.command == "POST" and path.endswith("/run") \
                    and path.startswith("/api/plans/"):
                name = path[len("/api/plans/"):-len("/run")]
                body = h.read_json()
                plan = self.planstore.get(name)
                rid = self.engine.start_run(plan, bool(body.get("resilience")))
                return h.send_json(200, {"run_id": rid})
            if h.command == "POST" and path == "/api/drill/stop":
                self.engine.stop()
                return h.send_json(200, {"ok": True})
            if h.command == "GET" and path == "/api/runs":
                return h.send_json(200, self.runstore.list())
            if h.command == "GET" and path.startswith("/api/runs/"):
                run = self.runstore.get(path.rsplit("/", 1)[1])
                return h.send_json(200, run) if run else h.send_json(404, {})
            if h.command == "GET" and path == "/api/compare":
                a = self.runstore.get((qs.get("a") or [""])[0])
                b = self.runstore.get((qs.get("b") or [""])[0])
                if not a or not b:
                    return h.send_json(400, {"error": "need valid a and b run ids"})
                return h.send_json(200, report_mod.compare(a, b))
            if h.command == "GET" and path == "/api/rules":
                return h.send_json(200, self.rules.list())
            if h.command == "POST" and path == "/api/rules":
                rule = h.read_json()
                self.rules.upsert(rule)
                return h.send_json(200, {"ok": True})
            if h.command == "POST" and path.endswith("/toggle") \
                    and path.startswith("/api/rules/"):
                rid = path[len("/api/rules/"):-len("/toggle")]
                cur = {r["id"]: r for r in self.rules.list()}.get(rid)
                if cur:
                    self.rules.set_enabled(rid, not cur.get("enabled"))
                return h.send_json(200, {"ok": True})
            if h.command == "DELETE" and path.startswith("/api/rules/"):
                self.rules.delete(path.rsplit("/", 1)[1])
                return h.send_json(200, {"ok": True})
            if h.command == "GET" and path == "/api/advisor":
                return h.send_json(200, advisor_mod.advise(self.services,
                                                           self.settings))
            if h.command == "POST" and path == "/api/advisor/apply":
                rows = advisor_mod.advise(self.services, self.settings)
                advisor_mod.apply(rows, self.settings)
                return h.send_json(200, {"ok": True, "applied": rows})
            if h.command == "POST" and path == "/api/resilience":
                body = h.read_json()
                self.settings.set_resilience(bool(body.get("enabled")))
                return h.send_json(200, {"ok": True})
            if h.command == "GET" and path == "/api/steady-check":
                plan_name = (qs.get("plan") or [""])[0]
                plan = self.planstore.get(plan_name)
                failed = check_steady(plan.get("steady_state", []),
                                      self.collector, self.loadgen)
                return h.send_json(200, {"ok": not failed, "failed": failed})
            h.send_json(404, {"error": "unknown endpoint", "path": path})
        except RuntimeError as e:
            h.send_json(409, {"error": str(e)})
        except Exception as e:  # noqa: BLE001
            h.send_json(500, {"error": "%s: %s" % (type(e).__name__, e)})
