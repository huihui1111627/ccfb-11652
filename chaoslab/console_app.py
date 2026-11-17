"""Web 控制台：基于标准库 http.server 的单页应用 + JSON API。

五大模块（对应需求）：
- 服务拓扑：实时节点状态、熔断器状态、池饱和度
- 演练计划：模板列表、执行、复跑
- 注入规则：代理当前规则查看与增删
- 稳态指标：模板中稳态假设展示 + 实时巡检数据
- 演练报告：run 列表、单报告、双跑对比
"""
from __future__ import annotations

import json
import os
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit, parse_qs

from chaoslab.model import plan_from_dict, to_jsonable
from chaoslab.orchestrator import LabHandle, Orchestrator
from chaoslab.plans import seed_templates
from chaoslab.report import build_comparison_report, build_run_report
from chaoslab.store import Store
from labctl.topology import TOPO_EDGES

CONSOLE_DIR = os.path.join(os.getcwd(), ".chaoslab")
REPORT_DIR = os.path.join(CONSOLE_DIR, "reports")
DB_PATH = os.path.join(CONSOLE_DIR, "chaoslab.db")


class ConsoleState:
    """跨请求共享：存储、靶场句柄、后台运行的实验任务。"""

    def __init__(self, handle: LabHandle):
        os.makedirs(CONSOLE_DIR, exist_ok=True)
        self.store = Store(DB_PATH)
        seed_templates(self.store)
        self.handle = handle
        self.orchestrator = Orchestrator(handle, self.store)
        self.jobs: dict[str, dict] = {}
        self._lock = threading.Lock()

    # ------------------------------------------------ 代理规则
    def proxy_rules(self) -> list[dict]:
        code, data = _get(f"{self.handle.proxy_url}/proxy/rules")
        return data.get("rules", []) if code == 200 else []

    def topology_snapshot(self) -> dict:
        nodes = []
        for inst in self.handle.instances:
            code, data = _get(
                f"http://{self.handle.host}:{inst['port']}/metrics", timeout=1.5)
            node = {"instance_id": inst["instance_id"],
                    "service": inst["service"], "port": inst["port"],
                    "healthy": code == 200}
            if code == 200:
                node["thread_pool"] = data.get("thread_pool", {})
                node["dependencies"] = {
                    dep: {"breaker_state": d.get("breaker_state"),
                          "pool_saturation": d.get("pool_saturation"),
                          "mode": d.get("mode")}
                    for dep, d in data.get("dependencies", {}).items()}
                counters = data.get("counters", {})
                total = counters.get("requests.total", 0)
                errors = counters.get("requests.error", 0)
                node["error_rate"] = round(errors / total, 3) if total else 0.0
            nodes.append(node)
        return {"nodes": nodes, "edges": TOPO_EDGES}

    # ------------------------------------------------ 异步实验
    def start_experiment(self, plan_name: str) -> str:
        plan = self.store.load_plan_template(plan_name)
        if not plan:
            raise KeyError(plan_name)
        job_id = f"job-{int(time.time()*1000)}-{plan_name}"
        with self._lock:
            self.jobs[job_id] = {"status": "running", "plan": plan_name,
                                 "run_ids": [], "started_at": time.time(),
                                 "error": ""}

        def worker():
            try:
                runs = self.orchestrator.run_experiment(plan)
                with self._lock:
                    self.jobs[job_id]["run_ids"] = [r.run_id for r in runs]
                    self.jobs[job_id]["status"] = "done"
                # 自动产出报告
                for r in runs:
                    build_run_report(self.store, r.run_id, REPORT_DIR)
                if len(runs) == 2:
                    details = [self.store.get_run(r.run_id) for r in runs]
                    off = next(d for d in details if d["resilience_mode"] == "off")
                    on = next(d for d in details if d["resilience_mode"] == "on")
                    build_comparison_report(self.store, off, on, REPORT_DIR)
            except Exception as exc:  # 实验失败不拖垮控制台
                with self._lock:
                    self.jobs[job_id]["status"] = "failed"
                    self.jobs[job_id]["error"] = str(exc)

        threading.Thread(target=worker, name=f"exp-{plan_name}",
                         daemon=True).start()
        return job_id

    def list_jobs(self) -> list[dict]:
        with self._lock:
            return [{"job_id": k, **v} for k, v in self.jobs.items()]


def _get(url: str, timeout: float = 3.0) -> tuple[int, dict]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as exc:  # type: ignore[attr-defined]
        try:
            return exc.code, json.loads(exc.read() or b"{}")
        except json.JSONDecodeError:
            return exc.code, {}
    except Exception as exc:
        return 0, {"error": str(exc)}


def _request(method: str, url: str, payload: dict | None = None,
             timeout: float = 3.0) -> tuple[int, dict]:
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        url, data=data, method=method,
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as exc:  # type: ignore[attr-defined]
        return exc.code, json.loads(exc.read() or b"{}")
    except Exception as exc:
        return 0, {"error": str(exc)}


# ---------------------------------------------------------------- HTTP 处理
class ConsoleHandler(BaseHTTPRequestHandler):
    server_version = "ChaosLabConsole/1.0"

    def log_message(self, fmt, *args):
        pass

    def _json(self, code: int, payload: dict) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _body(self) -> dict:
        length = int(self.headers.get("Content-Length", 0))
        if not length:
            return {}
        try:
            return json.loads(self.rfile.read(length))
        except json.JSONDecodeError:
            return {}

    @property
    def state(self) -> ConsoleState:
        return self.server.console_state  # type: ignore[attr-defined]

    # ------------------------------------------------ 路由
    def do_GET(self) -> None:
        parsed = urlsplit(self.path)
        path, qs = parsed.path, parse_qs(parsed.query)
        if path in ("/", "/index.html"):
            self._serve_index()
            return
        if path == "/api/topology":
            self._json(200, self.state.topology_snapshot())
        elif path == "/api/plans":
            self._json(200, {"plans": [
                to_jsonable(self.state.store.load_plan_template(t["name"]))
                for t in self.state.store.list_plan_templates()]})
        elif path == "/api/runs":
            plan = qs.get("plan", [""])[0]
            self._json(200, {"runs": self.state.store.list_runs(plan)})
        elif path == "/api/runs/detail":
            detail = self.state.store.get_run(qs["id"][0])
            self._json(200 if detail else 404, detail or {"error": "not found"})
        elif path == "/api/runs/series":
            self._json(200, {"series": self.state.store.get_series(qs["id"][0])})
        elif path == "/api/rules":
            self._json(200, {"rules": self.state.proxy_rules()})
        elif path == "/api/jobs":
            self._json(200, {"jobs": self.state.list_jobs()})
        elif path == "/api/reports":
            files = sorted(os.listdir(REPORT_DIR)) if os.path.isdir(REPORT_DIR) else []
            self._json(200, {"reports": files})
        elif path.startswith("/reports/"):
            self._serve_report(path.split("/", 2)[2])
        elif path == "/api/advisor":
            # 需求 5：汇总各服务的分位数超时建议
            self._json(200, {"advisors": self._collect_advisors()})
        else:
            self._json(404, {"error": "not found"})

    def do_POST(self) -> None:
        path = urlsplit(self.path).path
        body = self._body()
        if path == "/api/plans/save":
            plan = plan_from_dict(body)
            self.state.store.save_plan_template(plan)
            self._json(200, {"ok": True, "name": plan.name})
        elif path == "/api/experiments/start":
            try:
                job_id = self.state.start_experiment(body["plan"])
                self._json(200, {"ok": True, "job_id": job_id})
            except KeyError:
                self._json(404, {"error": "计划不存在"})
        elif path == "/api/rules":
            code, data = _request("POST",
                                  f"{self.state.handle.proxy_url}/proxy/rules", body)
            self._json(code, data)
        elif path == "/api/control/mode":
            results = []
            for inst in self.state.handle.instances:
                code, data = _request(
                    "POST", f"http://{self.state.handle.host}:{inst['port']}/control",
                    {"action": "set_mode", "mode": body.get("mode", "on")})
                results.append({"instance": inst["instance_id"], "ok": code == 200})
            self._json(200, {"results": results})
        elif path == "/api/control/clear-faults":
            code, data = _request(
                "DELETE", f"{self.state.handle.proxy_url}/proxy/rules")
            self._json(200, {"ok": True})
        elif path == "/api/reports/run":
            path_out = build_run_report(self.state.store, body["run_id"], REPORT_DIR)
            self._json(200, {"ok": True, "path": path_out})
        elif path == "/api/reports/compare":
            off = self.state.store.get_run(body["run_off"])
            on = self.state.store.get_run(body["run_on"])
            out = build_comparison_report(self.state.store, off, on, REPORT_DIR)
            self._json(200, {"ok": True, "path": out})
        else:
            self._json(404, {"error": "not found"})

    def do_DELETE(self) -> None:
        path = urlsplit(self.path).path
        if path == "/api/rules":
            code, data = _request(
                "DELETE", f"{self.state.handle.proxy_url}/proxy/rules")
            self._json(code, data)
        elif path.startswith("/api/rules/"):
            rest = path[len("/api/rules/"):]
            code, data = _request(
                "DELETE", f"{self.state.handle.proxy_url}/proxy/rules/{rest}")
            self._json(code, data)
        elif path.startswith("/api/plans/"):
            ok = self.state.store.delete_plan_template(path.split("/")[-1])
            self._json(200 if ok else 404, {"ok": ok})
        else:
            self._json(404, {"error": "not found"})

    # ------------------------------------------------ 页面
    def _collect_advisors(self) -> list[dict]:
        out = []
        for inst in self.state.handle.instances:
            code, data = _request(
                "POST", f"http://{self.state.handle.host}:{inst['port']}/control",
                {"action": "suggest_timeouts"})
            if code == 200 and data.get("suggested"):
                out.append({"instance": inst["instance_id"],
                            "service": inst["service"],
                            "suggested": data["suggested"]})
        return out

    def _serve_report(self, filename: str) -> None:
        safe = os.path.basename(filename)
        full = os.path.join(REPORT_DIR, safe)
        if not os.path.exists(full):
            self._json(404, {"error": "report not found"})
            return
        with open(full, "rb") as f:
            content = f.read()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def _serve_index(self) -> None:
        here = os.path.dirname(__file__)
        with open(os.path.join(here, "web", "index.html"), "rb") as f:
            content = f.read()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)


def serve_console(handle: LabHandle, port: int) -> ThreadingHTTPServer:
    httpd = ThreadingHTTPServer(("127.0.0.1", port), ConsoleHandler)
    httpd.daemon_threads = True
    httpd.console_state = ConsoleState(handle)  # type: ignore[attr-defined]
    threading.Thread(target=httpd.serve_forever, name="console",
                     daemon=True).start()
    return httpd
