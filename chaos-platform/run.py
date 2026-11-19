"""启动入口:挂号系统集群 + 故障代理 + 负载 + 控制台。

用法: python run.py  然后打开 http://127.0.0.1:8600
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from chaos.console import Console
from chaos.engine import Collector, DrillEngine, LoadGen, PlanStore, RunStore
from chaos.hospital import build_cluster
from chaos.proxy import RuleManager
from chaos.service import EventBus, Settings


def main():
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
    print("=" * 60)
    print("挂号系统集群已启动: %d 个服务实例,每个实例前挂故障注入代理" % len(services))
    print("负载发生: 挂号 8rps + 报告查询 4rps")
    print("控制台: http://127.0.0.1:8600")
    print("=" * 60)
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        print("bye")


if __name__ == "__main__":
    main()
