"""对比实验:同一演练计划(sms-slow),分别在关/开韧性模式下各跑一次。

用法: python demo.py           跑完对比后控制台继续运行,可在浏览器查看报告
      python demo.py --once    跑完对比即退出
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from chaos import advisor, report
from chaos.console import Console
from chaos.engine import Collector, DrillEngine, LoadGen, PlanStore, RunStore
from chaos.hospital import build_cluster
from chaos.proxy import RuleManager
from chaos.service import EventBus, Settings


def brief(runstore, rid, tag):
    run = runstore.get(rid)
    s = run["summary"]
    c = s["client"]
    print("\n----- %s (%s) -----" % (tag, rid))
    print("  客户端: 请求 %d, 错误率 %.1f%%, p95 %sms, p99 %sms, 降级 %d"
          % (c["count"], c["error_rate"] * 100, c["p95"], c["p99"], c["degraded"]))
    print("  结果: %s" % ("稳态失守,自动中止" if run["aborted"] else "稳态保持,跑完全程"))
    if s["propagation"]:
        print("  故障扩散: %s" % " -> ".join(
            ["/".join(s["targets"])] + [p["service"] for p in s["propagation"]]))
    else:
        print("  故障扩散: 无(被限制在注入点)")
    for v in s["verdicts"]:
        print("  · " + v)
    return run


def main():
    once = "--once" in sys.argv
    settings = Settings()
    bus = EventBus()
    rules = RuleManager()
    services, proxies = build_cluster(settings, rules, bus)
    for p in proxies:
        p.start()
    for s in services.values():
        s.start()
    loadgen = LoadGen("127.0.0.1", 8001)
    loadgen.start()
    collector = Collector(services, loadgen)
    collector.start()
    base = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    plans = PlanStore(os.path.join(base, "plans"))
    runs = RunStore(os.path.join(base, "runs"))
    engine = DrillEngine(services, settings, rules, collector, loadgen, runs, bus)
    console = Console(8600, services, settings, rules, engine, collector,
                      loadgen, plans, runs, bus)
    console.start()
    print("集群已启动(%d 个实例),控制台: http://127.0.0.1:8600" % len(services))

    print("\n[1/4] 预热 10s,采集基线延迟 ...")
    time.sleep(10)
    recs = advisor.advise(services, settings)
    advisor.apply(recs, settings)
    print("      超时建议(按历史分位数)已应用,韧性模式下生效:")
    for r in recs:
        print("      %-28s p95=%6.1fms p99=%6.1fms -> 建议超时 %dms"
              % (r["edge"], r["p95"], r["p99"], r["recommended_ms"]))

    plan = plans.get("sms-slow")
    print("\n[2/4] 演练A: 关闭熔断/隔离/降级(复现去年事故) ...")
    ra = engine.run_and_wait(plan, resilience=False)
    run_a = brief(runs, ra, "演练A 关韧性")

    print("\n[3/4] 冷却 20s,等待线程池/连接池恢复 ...")
    time.sleep(20)

    print("\n[4/4] 演练B: 开启熔断/隔离/降级 ...")
    rb = engine.run_and_wait(plan, resilience=True)
    run_b = brief(runs, rb, "演练B 开韧性")

    cmp = report.compare(run_a, run_b)
    print("\n" + "=" * 66)
    print("对比总结(A=关韧性, B=开韧性, 同一演练计划 sms-slow)")
    print("=" * 66)
    for row in cmp["rows"]:
        print("  %-14s A: %-28s B: %s" % (row["metric"], row["a"], row["b"]))
    print("\n完整报告(含线程池曲线、故障传播图)见控制台 [演练报告] 页")
    print("两次演练 ID: %s , %s" % (ra, rb))
    if once:
        return
    print("控制台继续运行,Ctrl-C 退出")
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
