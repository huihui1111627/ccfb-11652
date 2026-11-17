"""故障注入代理独立进程入口。

代理必须是独立长驻进程：如果跑在 `lab up` 的进程内，命令退出时代理会
随进程一起消失，而业务服务子进程还活着，拓扑就残了。

用法：python -m labctl.proxy_runner <port> <topology.json>
topology.json: {"sms": [{"instance_id":"sms-1","host":"...","port":"..."}]}
"""
from __future__ import annotations

import json
import signal
import sys
import time

from chaoslab.proxy import ChaosProxy


def main() -> None:
    if len(sys.argv) != 3:
        print("usage: python -m labctl.proxy_runner <port> <topology.json>")
        sys.exit(2)
    port = int(sys.argv[1])
    with open(sys.argv[2]) as f:
        topology = json.load(f)

    proxy = ChaosProxy(port, topology)
    proxy.start()

    stopping = {"flag": False}

    def handle(_sig, _frame):
        stopping["flag"] = True

    signal.signal(signal.SIGTERM, handle)
    signal.signal(signal.SIGINT, handle)
    while not stopping["flag"]:
        time.sleep(0.2)
    proxy.stop()


if __name__ == "__main__":
    main()
