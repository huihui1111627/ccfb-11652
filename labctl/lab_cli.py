"""靶场与平台统一命令行入口。

用法：
  python -m labctl up            启动 11 个服务实例 + 故障注入代理
  python -m labctl down          关停并清理故障规则
  python -m labctl status        查看存活状态
  python -m labctl console       启动 Web 控制台（:8300）
  python -m labctl experiment    一键跑头条对照实验并产出对比报告
  python -m labctl reset         清空演练数据库与报告
"""
from __future__ import annotations

import json
import os
import sys
import time

from chaoslab.console_app import CONSOLE_DIR, REPORT_DIR, serve_console
from chaoslab.model import RunRecord
from chaoslab.orchestrator import LabHandle, Orchestrator
from chaoslab.plans import seed_templates, sms_delay_incident
from chaoslab.report import build_comparison_report, build_run_report
from chaoslab.store import Store
from labctl.lab_manager import Lab, read_state
from labctl.topology import CONSOLE_PORT, gateway_port

DB_PATH = os.path.join(CONSOLE_DIR, "chaoslab.db")


def _handle() -> LabHandle:
    state = read_state()
    if not state:
        print("靶场未运行，请先执行: python -m labctl up")
        sys.exit(1)
    # state 里混着代理进程条目，控制面只面向业务服务实例
    instances = [i for i in state["instances"] if i["service"] != "_proxy"]
    gw = gateway_port([type("C", (), {"port": i["port"], "service": i["service"]})()
                       for i in instances])
    return LabHandle(instances=instances,
                     proxy_port=state["proxy_port"], gateway_port=gw)


def cmd_experiment() -> None:
    handle = _handle()
    store = Store(DB_PATH)
    seed_templates(store)
    plan = sms_delay_incident()
    store.save_plan_template(plan)
    orch = Orchestrator(handle, store)

    print("=" * 64)
    print("头条演练：短信服务变慢事故复现（同计划 OFF / ON 双跑对照）")
    print("=" * 64)
    runs: list[RunRecord] = []
    for mode in plan.resilience_modes:
        print(f"\n>>> 第 {len(runs)+1}/2 轮：韧性模式 {mode.upper()}")
        run = orch.run_once(plan, mode)
        runs.append(run)
        load = run.summary["load"]
        print(f"    状态={run.status} 总请求={load['total']} "
              f"错误率={load['error_rate']*100:.1f}% "
              f"P95={load['latency_p95_ms']:.0f}ms "
              f"降级={load['fallback_hits']}")
        impacted = run.summary["services"]
        affected = [iid for iid, s in impacted.items()
                    if s["counters"].get("requests.error", 0) > 0
                    or s["thread_pool"].get("rejected", 0) > 0
                    or s["thread_pool"].get("saturation", 0) >= 0.7]
        print(f"    受影响实例: {', '.join(affected) if affected else '无'}")
        if run.abort_reason:
            print(f"    中止原因: {run.abort_reason}")
        if mode != plan.resilience_modes[-1]:
            print("    等待系统从故障中恢复（轮询健康检查）...")
            if orch.wait_for_recovery():
                print("    全部实例已恢复，开始下一轮")
            else:
                print("    恢复超时，重置熔断器后继续（结果可能受残留影响）")
                orch.reset_breakers()
                orch.wait_for_recovery(timeout_s=15)

    print("\n" + "=" * 64)
    print("生成报告")
    print("=" * 64)
    paths = []
    for r in runs:
        p = build_run_report(store, r.run_id, REPORT_DIR)
        paths.append(p)
        print(f"  单跑报告 ({r.resilience_mode}): {p}")
    details = [store.get_run(r.run_id) for r in runs]
    off = next(d for d in details if d["resilience_mode"] == "off")
    on = next(d for d in details if d["resilience_mode"] == "on")
    cmp_path = build_comparison_report(store, off, on, REPORT_DIR)
    print(f"  双跑对比报告: {cmp_path}")

    lo, ln = off["summary"]["load"], on["summary"]["load"]
    print("\n结论速览：")
    print(f"  错误率  {lo['error_rate']*100:5.1f}%  →  {ln['error_rate']*100:5.1f}%")
    print(f"  P95延迟 {lo['latency_p95_ms']:5.0f}ms →  {ln['latency_p95_ms']:5.0f}ms")
    print(f"  降级命中 {lo['fallback_hits']:5d}  →  {ln['fallback_hits']:5d}")
    store.close()


def cmd_console() -> None:
    handle = _handle()
    httpd = serve_console(handle, CONSOLE_PORT)
    print(f"控制台已启动: http://127.0.0.1:{CONSOLE_PORT}")
    print("Ctrl+C 退出（靶场进程不受影响）")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        httpd.shutdown()


def main() -> None:
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(0)
    cmd = sys.argv[1]
    lab = Lab()
    if cmd == "up":
        lab.up()
    elif cmd == "down":
        lab.down()
    elif cmd == "status":
        lab.status()
    elif cmd == "console":
        cmd_console()
    elif cmd == "experiment":
        cmd_experiment()
    elif cmd == "reset":
        for p in (DB_PATH,):
            if os.path.exists(p):
                os.remove(p)
        if os.path.isdir(REPORT_DIR):
            for f in os.listdir(REPORT_DIR):
                os.remove(os.path.join(REPORT_DIR, f))
        print("已重置数据库与报告")
    else:
        print(f"未知命令: {cmd}")
        print(__doc__)
        sys.exit(2)


if __name__ == "__main__":
    main()
