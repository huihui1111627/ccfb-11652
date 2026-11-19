"""演练引擎:指标采集、负载发生、演练执行(步骤编排/稳态检查/自动中止)、存储。"""
import http.client
import json
import os
import random
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor

from . import report as report_mod
from .service import CHAOS_TAG_HEADER

# ---------------------------------------------------------------- 内置演练计划模板

DEFAULT_PLANS = [
    {
        "name": "sms-slow",
        "description": "短信运营商通道延迟 50ms -> 8s(复现去年事故:全站线程被拖垮)",
        "duration_s": 50,
        "abort_after": 3,
        "blast_zone": ["sms"],
        "rules": [
            {"id": "sms-slow-1", "target": "smsprovider-1", "type": "latency",
             "latency_ms": 8000, "match": {}},
            {"id": "sms-slow-2", "target": "smsprovider-2", "type": "latency",
             "latency_ms": 8000, "match": {}},
        ],
        "steps": [
            {"at_s": 8, "action": "inject", "rule": "sms-slow-1"},
            {"at_s": 8, "action": "inject", "rule": "sms-slow-2"},
            {"at_s": 38, "action": "recover", "rule": "sms-slow-1"},
            {"at_s": 38, "action": "recover", "rule": "sms-slow-2"},
        ],
        "steady_state": [
            {"metric": "client.error_rate", "op": "<", "value": 0.3},
            {"metric": "client.p95_ms", "op": "<", "value": 6000},
        ],
    },
    {
        "name": "sms-slow-tagged",
        "description": "同上,但只命中带 X-Chaos-Tag: chaos 的演练流量(爆炸半径限制)",
        "duration_s": 50,
        "abort_after": 3,
        "blast_zone": ["sms"],
        "rules": [
            {"id": "sms-slow-tag-1", "target": "smsprovider-1", "type": "latency",
             "latency_ms": 8000, "match": {"tag": "chaos"}},
            {"id": "sms-slow-tag-2", "target": "smsprovider-2", "type": "latency",
             "latency_ms": 8000, "match": {"tag": "chaos"}},
        ],
        "steps": [
            {"at_s": 8, "action": "inject", "rule": "sms-slow-tag-1"},
            {"at_s": 8, "action": "inject", "rule": "sms-slow-tag-2"},
            {"at_s": 38, "action": "recover", "rule": "sms-slow-tag-1"},
            {"at_s": 38, "action": "recover", "rule": "sms-slow-tag-2"},
        ],
        "steady_state": [
            {"metric": "client.error_rate", "op": "<", "value": 0.15},
        ],
    },
    {
        "name": "paychannel-error",
        "description": "支付渠道 100% 返回 500(关键路径无降级预案时的表现)",
        "duration_s": 50,
        "abort_after": 3,
        "rules": [
            {"id": "pay-err", "target": "paychannel", "type": "error",
             "error_status": 500, "error_rate": 1.0, "match": {}},
        ],
        "steps": [
            {"at_s": 8, "action": "inject", "rule": "pay-err"},
            {"at_s": 38, "action": "recover", "rule": "pay-err"},
        ],
        "steady_state": [
            {"metric": "client.inquiry.error_rate", "op": "<", "value": 0.1},
        ],
    },
]


# ---------------------------------------------------------------- 存储

class PlanStore:
    def __init__(self, directory):
        self.dir = directory
        os.makedirs(self.dir, exist_ok=True)
        # 内置模板随代码版本刷新;自定义模板(其它名字)不受影响
        for plan in DEFAULT_PLANS:
            self.save(plan)

    def _path(self, name):
        return os.path.join(self.dir, "%s.json" % name)

    def save(self, plan):
        with open(self._path(plan["name"]), "w", encoding="utf-8") as f:
            json.dump(plan, f, ensure_ascii=False, indent=2)

    def get(self, name):
        with open(self._path(name), encoding="utf-8") as f:
            return json.load(f)

    def list(self):
        out = []
        for fn in sorted(os.listdir(self.dir)):
            if fn.endswith(".json"):
                try:
                    with open(os.path.join(self.dir, fn), encoding="utf-8") as f:
                        p = json.load(f)
                    out.append({"name": p["name"],
                                "description": p.get("description", "")})
                except Exception:
                    pass
        return out


class RunStore:
    def __init__(self, directory):
        self.dir = directory
        os.makedirs(self.dir, exist_ok=True)
        self._runs = {}
        self._lock = threading.Lock()

    def save(self, run):
        with self._lock:
            self._runs[run["id"]] = run
        path = os.path.join(self.dir, "%s.json" % run["id"])
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(run, f, ensure_ascii=False)
        except Exception:
            pass

    def get(self, run_id):
        with self._lock:
            if run_id in self._runs:
                return self._runs[run_id]
        path = os.path.join(self.dir, "%s.json" % run_id)
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        return None

    def list(self):
        out = []
        for fn in sorted(os.listdir(self.dir), reverse=True):
            if not fn.endswith(".json"):
                continue
            try:
                with open(os.path.join(self.dir, fn), encoding="utf-8") as f:
                    r = json.load(f)
                s = r.get("summary") or {}
                out.append({"id": r["id"], "plan": r["plan_name"],
                            "resilience": r["resilience"],
                            "started_at": r.get("started_at"),
                            "aborted": r.get("aborted", False),
                            "client": s.get("client")})
            except Exception:
                pass
        return out


# ---------------------------------------------------------------- 指标采集

class Collector:
    """每 0.5s 抓取全部服务的指标快照;演练期间写入 run.series。"""

    def __init__(self, services, loadgen):
        self.services = services
        self.loadgen = loadgen
        self.recent = deque(maxlen=600)  # 最近约 5 分钟,供实时视图
        self.run = None
        self._lock = threading.Lock()

    def attach(self, run):
        with self._lock:
            self.run = run

    def detach(self):
        with self._lock:
            self.run = None

    def start(self):
        threading.Thread(target=self._loop, daemon=True).start()

    def _loop(self):
        while True:
            try:
                self._tick()
            except Exception:
                pass
            time.sleep(0.5)

    def _tick(self):
        snap = {"ts": time.time(),
                "services": {n: s.snapshot() for n, s in self.services.items()},
                "client": self.loadgen.window_stats(5)}
        self.recent.append(snap)
        with self._lock:
            run = self.run
        if run is not None:
            run["series"].append({"t": round(snap["ts"] - run["t0"], 1),
                                  "services": snap["services"],
                                  "client": snap["client"]})


# ---------------------------------------------------------------- 负载发生

FLOWS = [
    ("register", "/api/register", 8,
     {"patient_id": "P1001", "name": "测试患者", "dept": "内科"}),
    ("inquiry", "/api/inquiry", 4, {"patient_id": "P1001"}),
]


class LoadGen:
    """固定速率打流量;10% 的请求带演练标记,用于按标记限制爆炸半径。"""

    def __init__(self, host, port, tag_fraction=0.1, client_timeout_s=12):
        self.addr = (host, port)
        self.tag_fraction = tag_fraction
        self.client_timeout_s = client_timeout_s
        self.results = deque(maxlen=30000)  # (ts, flow, ok, degraded, lat_ms)
        self._lock = threading.Lock()
        self._running = False

    def start(self):
        if self._running:
            return
        self._running = True
        for flow, path, rps, payload in FLOWS:
            threading.Thread(target=self._loop, args=(flow, path, rps, payload),
                             daemon=True).start()

    def _loop(self, flow, path, rps, payload):
        pool = ThreadPoolExecutor(max_workers=64)
        interval = 1.0 / rps
        while self._running:
            t0 = time.time()
            try:
                pool.submit(self._fire, flow, path, payload)
            except RuntimeError:
                return  # 解释器退出中
            dt = time.time() - t0
            if dt < interval:
                time.sleep(interval - dt)

    def _fire(self, flow, path, payload):
        headers = {}
        if random.random() < self.tag_fraction:
            headers[CHAOS_TAG_HEADER] = "chaos"
        t0 = time.time()
        ok, degraded = False, False
        try:
            conn = http.client.HTTPConnection(self.addr[0], self.addr[1],
                                              timeout=self.client_timeout_s)
            conn.request("POST", path, body=json.dumps(payload),
                         headers={"Content-Type": "application/json", **headers})
            resp = conn.getresponse()
            data = resp.read()
            conn.close()
            ok = resp.status == 200
            if ok:
                try:
                    degraded = bool(json.loads(data.decode() or "{}").get("degraded"))
                except Exception:
                    pass
        except Exception:
            ok = False
        lat = (time.time() - t0) * 1000
        with self._lock:
            self.results.append((time.time(), flow, ok, degraded, lat))

    def _stats(self, rows, secs):
        n = len(rows)
        if n == 0:
            return {"count": 0, "rps": 0.0, "errors": 0, "error_rate": 0.0,
                    "degraded": 0, "p50": 0.0, "p95": 0.0, "p99": 0.0}
        errors = sum(1 for r in rows if not r[2])
        degraded = sum(1 for r in rows if r[3])
        lats = sorted(r[4] for r in rows)

        def pct(p):
            return round(lats[min(n - 1, int(p * n))], 1)

        return {"count": n, "rps": round(n / max(secs, 0.1), 1), "errors": errors,
                "error_rate": round(errors / n, 4), "degraded": degraded,
                "p50": pct(0.5), "p95": pct(0.95), "p99": pct(0.99)}

    def window_stats(self, secs=5, flow=None):
        cutoff = time.time() - secs
        with self._lock:
            rows = [r for r in self.results
                    if r[0] >= cutoff and (flow is None or r[1] == flow)]
        return self._stats(rows, secs)

    def stats_between(self, t0, t1):
        with self._lock:
            rows = [r for r in self.results if t0 <= r[0] <= t1]
        return self._stats(rows, max(t1 - t0, 0.1))


# ---------------------------------------------------------------- 稳态指标求值

def resolve_metric(expr, collector, loadgen):
    parts = expr.split(".")
    if parts[0] == "client":
        if len(parts) == 2:
            stats, key = loadgen.window_stats(5), parts[1]
        else:
            stats, key = loadgen.window_stats(5, flow=parts[1]), parts[2]
        return {"error_rate": stats["error_rate"], "p95_ms": stats["p95"],
                "p99_ms": stats["p99"], "rps": stats["rps"]}.get(key)
    svc, key = parts[0], ".".join(parts[1:])
    now = time.time()
    snaps = [s["services"][svc] for s in collector.recent
             if now - s["ts"] <= 5 and svc in s["services"]]
    if not snaps:
        return None
    if key == "error_rate":
        a, b = snaps[0], snaps[-1]
        dr = b["requests"] - a["requests"]
        return (b["errors"] - a["errors"]) / dr if dr > 0 else 0.0
    if key == "p95_ms":
        return snaps[-1]["lat"]["p95"]
    if key == "pool_active_ratio":
        p = snaps[-1]["pool"]
        return p["active"] / max(p["size"], 1)
    if key == "rejected_delta":
        return snaps[-1]["pool"]["rejected"] - snaps[0]["pool"]["rejected"]
    return None


def check_steady(steady_state, collector, loadgen):
    """返回未通过的指标描述列表,空列表表示稳态保持。"""
    failed = []
    for cond in steady_state:
        val = resolve_metric(cond["metric"], collector, loadgen)
        if val is None:
            continue
        op, threshold = cond["op"], cond["value"]
        ok = {"<": val < threshold, "<=": val <= threshold,
              ">": val > threshold, ">=": val >= threshold}.get(op, True)
        if not ok:
            failed.append("%s=%.3f (期望 %s %s)" % (cond["metric"], val, op, threshold))
    return failed


# ---------------------------------------------------------------- 演练引擎

class DrillEngine:
    def __init__(self, services, settings, rules, collector, loadgen,
                 runstore, bus):
        self.services = services
        self.settings = settings
        self.rules = rules
        self.collector = collector
        self.loadgen = loadgen
        self.runstore = runstore
        self.bus = bus
        self.current = None
        self._stop = threading.Event()
        self._lock = threading.Lock()
        bus.subscribe(self._on_event)

    def _on_event(self, event):
        run = self.current
        if run is not None and event.get("type") == "breaker":
            run["events"].append({
                "t": round(time.time() - run["t0"], 1), "type": "breaker",
                "msg": "熔断器 %s: %s -> %s" % (event["edge"], event["from"],
                                                event["to"])})

    def _event(self, run, etype, msg):
        run["events"].append({"t": round(time.time() - run["t0"], 1),
                              "type": etype, "msg": msg})
        self.bus.emit({"type": "drill", "msg": msg})

    def start_run(self, plan, resilience):
        with self._lock:
            if self.current is not None:
                raise RuntimeError("已有演练正在进行")
            self._stop.clear()
            self.settings.set_resilience(resilience)
            for svc in self.services.values():
                for ds in svc.downstreams.values():
                    ds.breaker.reset()
            self.rules.disable_all()
            for r in plan.get("rules", []):
                rr = dict(r)
                rr["enabled"] = False
                self.rules.upsert(rr)
            run = {
                "id": "%s-%s-%s" % (time.strftime("%Y%m%d-%H%M%S"),
                                    plan["name"],
                                    "on" if resilience else "off"),
                "plan_name": plan["name"], "plan": plan,
                "resilience": bool(resilience),
                "started_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                "t0": time.time(), "ended_at": None,
                "aborted": False, "abort_reason": None,
                "events": [], "checks": [], "series": [], "summary": None,
            }
            self.current = run
        self.collector.attach(run)
        self._event(run, "start", "演练开始(韧性模式: %s)" % ("开" if resilience else "关"))
        threading.Thread(target=self._execute, args=(run, plan),
                         daemon=True).start()
        return run["id"]

    def stop(self):
        self._stop.set()

    def run_and_wait(self, plan, resilience):
        rid = self.start_run(plan, resilience)
        while True:
            with self._lock:
                done = self.current is None
            if done:
                return rid
            time.sleep(0.5)

    def _do_step(self, run, step):
        action, rule_id = step.get("action"), step.get("rule")
        if action == "inject":
            self.rules.set_enabled(rule_id, True)
            self._event(run, "inject", "注入故障: %s" % rule_id)
        elif action == "recover":
            self.rules.set_enabled(rule_id, False)
            self._event(run, "recover", "恢复故障: %s" % rule_id)

    def _execute(self, run, plan):
        steps = sorted(plan.get("steps", []), key=lambda s: s["at_s"])
        fired = [False] * len(steps)
        steady = plan.get("steady_state", [])
        abort_after = plan.get("abort_after", 3)
        duration = plan.get("duration_s", 60)
        violations = 0
        observe_until = None
        last_check = 0.0
        try:
            while not self._stop.is_set():
                t = time.time() - run["t0"]
                for i, step in enumerate(steps):
                    if not fired[i] and t >= step["at_s"]:
                        fired[i] = True
                        self._do_step(run, step)
                if steady and not run["aborted"] and t - last_check >= 1.0:
                    last_check = t
                    failed = check_steady(steady, self.collector, self.loadgen)
                    run["checks"].append({"t": round(t, 1), "ok": not failed,
                                          "failed": failed})
                    if failed:
                        violations += 1
                        if violations >= abort_after:
                            run["aborted"] = True
                            run["abort_reason"] = "稳态指标持续偏离: " + "; ".join(failed)
                            self._event(run, "abort", run["abort_reason"])
                            self.rules.disable_all()
                            self._event(run, "recover-all",
                                        "自动中止:已撤除全部注入故障,观察恢复 10s")
                            observe_until = t + 10
                    else:
                        violations = 0
                if observe_until is not None and t >= observe_until:
                    break
                if t >= duration:
                    break
                time.sleep(0.5)
        finally:
            self.rules.disable_all()
            run["ended_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
            run["client_summary"] = self.loadgen.stats_between(
                run["t0"], time.time())
            run["summary"] = report_mod.build_summary(
                run, list(self.services.keys()))
            self.runstore.save(run)
            self.collector.detach()
            self._event(run, "end", "演练结束")
            with self._lock:
                self.current = None
