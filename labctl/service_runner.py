"""靶场进程入口：python -m labctl.service_runner <service> <instance_id> <port>。

每个服务跑在独立进程里，独立的线程池/连接池才能真实呈现资源耗尽过程。
"""
from __future__ import annotations

import signal
import sys
import time
from types import SimpleNamespace

from labctl.services import BUILDERS
from labctl.topology import HOST, PROXY_PORT


def main() -> None:
    if len(sys.argv) != 4:
        print("usage: python -m labctl.service_runner <service> <instance_id> <port>")
        sys.exit(2)
    service, instance_id, port_s = sys.argv[1], sys.argv[2], int(sys.argv[3])
    builder = BUILDERS[service]

    cfg = SimpleNamespace(
        host=HOST, port=port_s, instance_id=instance_id,
        proxy_base=f"http://{HOST}:{PROXY_PORT}")
    app = builder(cfg)
    app.start()

    stopping = {"flag": False}

    def handle(_sig, _frame):
        stopping["flag"] = True

    signal.signal(signal.SIGTERM, handle)
    signal.signal(signal.SIGINT, handle)
    # 就绪标志：父进程靠 /health 轮询，这里只保活
    while not stopping["flag"]:
        time.sleep(0.2)
    app.stop()


if __name__ == "__main__":
    main()
