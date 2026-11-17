"""演练编排器：计划执行引擎。

职责（对应需求 1/2/3/6/7）：
- 按步骤时间线 inject / retract / pause / load 顺序执行；
- 演练前统一切换所有服务的韧性模式 on/off；
- 后台采集器周期性抓所有服务 /metrics，落盘指标时序（线程池/连接池占用）；
- 稳态巡检线程按假设检查入口成功率/p95，连续偏离自动中止，
  并撤销代理上全部故障、恢复韧性模式；
- 汇总 run 级 summary、事件时间线，交报告模块消费。
"""
from __future__ import annotations

import json
import threading
import time
import urllib.request
from dataclasses import dataclass
from urllib.error import URLError

from chaoslab.loadgen import LoadGenerator
from chaoslab.model import DrillPlan, FaultRule, RunRecord
from chaoslab.store import Store


def _http(method: str, url: str, payload: dict | None = None,
          timeout: float = 3.0) -> tuple[int, dict]:
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as exc:  # type: ignore[attr-defined]
        return exc.code, json.loads(exc.read() or b"{}")
    except (URLError, OSError, TimeoutError, json.JSONDecodeError) as exc:
        return 0, {"error": str(exc)}


@dataclass
class LabHandle:
    """靶场访问信息：实例列表 + 代理地址 + 网关地址。"""
    instances: list[dict]
    proxy_port: int
    gateway_port: int
    host: str = "127.0.0.1"

    def service_base(self, service: str) -> str:
        for inst in self.instances:
            if inst["service"] == service:
                return f"http://{self.host}:{inst['port']}"
        raise KeyError(service)

    @property
    def proxy_url(self) -> str:
        return f"http://{self.host}:{self.proxy_port}"

    @property
    def gateway_url(self) -> str:
        return f"http://{self.host}:{self.gateway_port}/register"


class MetricsCollector:
    """周期抓取所有实例 /metrics，写入 store 的 metric_series。"""

    def __init__(self, handle: LabHandle, run_id: str, store: Store,
                 interval_s: float = 1.0):
        self.handle = handle
        self.run_id = run_id
        self.store = store
        self.interval_s = interval_s
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.latest: dict[str, dict] = {}
        self._lock = threading.Lock()

    def _loop(self) -> None:
        while not self._stop.is_set():
            ts = time.time()
            for inst in self.handle.instances:
                url = (f"http://{self.handle.host}:{inst['port']}/metrics")
                code, data = _http("GET", url, timeout=1.5)
                if code != 200:
                    continue
                with self._lock:
                    self.latest[inst["instance_id"]] = data
                try:
                    self.store.append_metric(
                        self.run_id, ts, inst["instance_id"], data)
                except Exception:
                    pass
            self._stop.wait(self.interval_s)

    def start(self) -> None:
        self._thread = threading.Thread(target=self._loop, name="collector",
                                        daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=3)

    def snapshot(self) -> dict[str, dict]:
        with self._lock:
            return dict(self.latest)


class SteadyStateWatcher:
    """稳态巡检：从采集器最新指标 + 负载统计判断是否偏离，连续偏离触发中止。"""

    def __init__(self, plan: DrillPlan, collector: MetricsCollector,
                 load: LoadGenerator, on_abort, interval_s: float = 1.0):
        self.plan = plan
        self.collector = collector
        self.load = load
        self.on_abort = on_abort
        self.interval_s = interval_s
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.checks: list[dict] = []
        self.breach_counts: dict[str, int] = {s.name: 0 for s in plan.steady_states}

    def _evaluate(self, ssh) -> tuple[bool, float]:
        """返回 (是否偏离, 当前指标值)。"""
        stats = self.load.stats
        if ssh.metric == "success_rate":
            value = (stats.success / stats.total) if stats.total else 1.0
            return (value < ssh.threshold) if ssh.comparator == "lt" else (value > ssh.threshold), value
        if ssh.metric == "error_rate":
            value = stats.error_rate()
            return (value > ssh.threshold) if ssh.comparator == "gt" else (value < ssh.threshold), value
        if ssh.metric == "latency_p95_ms":
            value = stats.percentile(0.95)
            return (value > ssh.threshold) if ssh.comparator == "gt" else (value < ssh.threshold), value
        if ssh.metric == "pool_saturation":
            # 取指定服务所有实例的最大线程池饱和度
            value = 0.0
            for iid, snap in self.collector.snapshot().items():
                if snap.get("service") == ssh.source_service:
                    value = max(value, snap.get("thread_pool", {}).get("saturation", 0.0))
            return (value > ssh.threshold) if ssh.comparator == "gt" else (value < ssh.threshold), value
        return False, 0.0

    def _loop(self) -> None:
        # 预热期：等流量进来再开始判定
        self._stop.wait(3)
        while not self._stop.is_set():
            for ssh in self.plan.steady_states:
                breached, value = self._evaluate(ssh)
                record = {"ts": time.time(), "name": ssh.name,
                          "metric": ssh.metric, "value": round(value, 4),
                          "threshold": ssh.threshold, "breached": breached}
                self.checks.append(record)
                if breached:
                    self.breach_counts[ssh.name] += 1
                    if self.breach_counts[ssh.name] >= ssh.breach_limit:
                        self.on_abort(
                            f"稳态指标 {ssh.name} 连续 {ssh.breach_limit} 次偏离"
                            f"（当前 {value:.3f} / 阈值 {ssh.threshold}）")
                        return
                else:
                    self.breach_counts[ssh.name] = 0
            self._stop.wait(self.interval_s)

    def start(self) -> None:
        self._thread = threading.Thread(target=self._loop, name="steady-watch",
                                        daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=3)


class Orchestrator:
    def __init__(self, handle: LabHandle, store: Store):
        self.handle = handle
        self.store = store

    # ------------------------------------------------ 代理规则操作
    def inject(self, rule: FaultRule) -> None:
        code, data = _http("POST", f"{self.handle.proxy_url}/proxy/rules",
                           payload=json.loads(json.dumps(rule.__dict__)))
        if code != 200:
            raise RuntimeError(f"注入失败: {data}")

    def retract(self, target: str, name: str) -> None:
        _http("DELETE", f"{self.handle.proxy_url}/proxy/rules/{target}/{name}")

    def clear_all_faults(self) -> None:
        _http("DELETE", f"{self.handle.proxy_url}/proxy/rules")

    # ------------------------------------------------ 韧性模式
    def set_resilience_mode(self, mode: str) -> None:
        for inst in self.handle.instances:
            url = f"http://{self.handle.host}:{inst['port']}/control"
            code, _ = _http("POST", url, {"action": "set_mode", "mode": mode})
            if code != 200:
                raise RuntimeError(f"切换 {inst['instance_id']} 韧性模式失败")

    def reset_breakers(self) -> None:
        for inst in self.handle.instances:
            _http("POST", f"http://{self.handle.host}:{inst['port']}/control",
                  {"action": "reset_breakers"})

    # ------------------------------------------------ 执行单次 run
    def run_once(self, plan: DrillPlan, mode: str) -> RunRecord:
        run = RunRecord(plan_name=plan.name, resilience_mode=mode)
        self.store.save_run(run)
        events = run.events

        def log_event(evt: str, **payload):
            entry = {"ts": time.time(), "event": evt, **payload}
            events.append(entry)

        # 1. 环境准备：清故障、切模式、重置熔断器
        self.clear_all_faults()
        self.set_resilience_mode(mode)
        self.reset_breakers()
        log_event("mode_applied", mode=mode)
        time.sleep(1.5)  # 等系统稳定

        # 2. 启动采集与负载
        collector = MetricsCollector(self.handle, run.run_id, self.store)
        collector.start()
        load = LoadGenerator(self.handle.gateway_url, rps=plan.load_rps,
                             concurrency=plan.load_concurrency)
        load_thread = threading.Thread(
            target=load.run, args=(plan.duration_s,), name="loadgen", daemon=True)

        watcher = None
        abort = {"reason": ""}
        if plan.auto_abort and plan.steady_states:
            def do_abort(reason: str):
                if abort["reason"]:
                    return
                abort["reason"] = reason
                load.stop()
            watcher = SteadyStateWatcher(plan, collector, load, do_abort)

        log_event("load_started", rps=plan.load_rps,
                  concurrency=plan.load_concurrency)
        load_thread.start()
        if watcher:
            watcher.start()

        # 3. 时间线步骤
        t0 = time.time()
        try:
            for step in plan.sorted_steps():
                target_t = t0 + step.offset_s
                wait = target_t - time.time()
                if wait > 0:
                    if abort["reason"]:
                        break
                    time.sleep(min(wait, 0.5) if wait > 0.5 else wait)
                    if abort["reason"]:
                        break
                    # 被打断睡眠后继续精确等待
                    while time.time() < target_t and not abort["reason"]:
                        time.sleep(0.1)
                if abort["reason"]:
                    break
                if step.action == "inject" and step.rule:
                    self.inject(step.rule)
                    log_event("fault_injected", rule=step.rule.name,
                              target=step.rule.target,
                              fault_type=step.rule.fault_type,
                              delay_ms=step.rule.delay_ms, note=step.note)
                elif step.action == "retract":
                    self.retract(step.rule.target if step.rule else "",
                                 step.rule_name or (step.rule.name if step.rule else ""))
                    log_event("fault_retracted", rule=step.rule_name, note=step.note)
                elif step.action == "pause":
                    log_event("pause", note=step.note)
                elif step.action == "load":
                    log_event("load_adjust", rps=step.rps,
                              concurrency=step.concurrency)

            # 等负载结束（可能已被 abort 提前停止）
            load_thread.join(timeout=plan.duration_s + 12)
        finally:
            # 4. 自动中止 / 正常结束都要撤掉全部故障（需求 2 的硬约束）
            self.clear_all_faults()
            log_event("all_faults_cleared")
            if watcher:
                watcher.stop()
            collector.stop()
            load.shutdown()

        # 5. 汇总结果
        run.finished_at = time.time()
        run.status = "aborted" if abort["reason"] else "completed"
        run.abort_reason = abort["reason"]
        run.steady_checks = watcher.checks if watcher else []
        stats = load.stats
        run.summary = {
            "load": stats.summary(),
            "services": self._summarize_services(collector.snapshot()),
            "abort_reason": abort["reason"],
        }
        self.store.save_run(run)
        return run

    def _summarize_services(self, latest: dict[str, dict]) -> dict[str, dict]:
        out: dict[str, dict] = {}
        for iid, snap in latest.items():
            deps = snap.get("dependencies", {})
            out[iid] = {
                "service": snap.get("service"),
                "thread_pool": snap.get("thread_pool", {}),
                "counters": snap.get("counters", {}),
                "dependencies": {
                    dep: {
                        "breaker_state": d.get("breaker_state"),
                        "breaker_rejected": d.get("breaker_rejected"),
                        "pool_saturation": d.get("pool_saturation"),
                        "pool_rejected": d.get("pool_rejected"),
                        "p95_ms": d.get("p95_ms"),
                        "suggested_timeout_s": d.get("suggested_timeout_s"),
                        "mode": d.get("mode"),
                    } for dep, d in deps.items()},
                "events": snap.get("events", [])[-10:],
            }
        return out

    # ------------------------------------------------ 轮次间恢复
    def wait_for_recovery(self, timeout_s: float = 30.0) -> bool:
        """轮询网关 /health，等线程池排空、服务从假死中自愈。

        OFF 轮可能把入口线程占满；故障撤销后，靠 socket 超时让在途请求
        陆续结束。所有实例健康才返回 True，超时返回 False。"""
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            ok_all = True
            for inst in self.handle.instances:
                code, _ = _http(
                    "GET",
                    f"http://{self.handle.host}:{inst['port']}/health",
                    timeout=2)
                if code != 200:
                    ok_all = False
                    break
            if ok_all:
                return True
            time.sleep(1.0)
        return False

    # ------------------------------------------------ 完整双跑实验
    def run_experiment(self, plan: DrillPlan) -> list[RunRecord]:
        """按计划的 resilience_modes 顺序各跑一次，中间自动等系统恢复。"""
        results: list[RunRecord] = []
        for mode in plan.resilience_modes:
            results.append(self.run_once(plan, mode))
            if mode != plan.resilience_modes[-1]:
                self.clear_all_faults()
                recovered = self.wait_for_recovery()
                if not recovered:
                    # 恢复不了就重置熔断器并再等一轮，避免污染下一轮对照
                    self.reset_breakers()
                    self.wait_for_recovery(timeout_s=15.0)
        return results
