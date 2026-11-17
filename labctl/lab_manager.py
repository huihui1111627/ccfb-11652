"""靶场生命周期管理：拉起服务进程 + 代理进程，健康检查，关停，状态查看。

所有组件都是独立长驻子进程，命令本身退出后靶场继续存活，
状态（pid、端口、拓扑）落盘到 .chaoslab/lab/state.json。
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
import urllib.request
from urllib.error import URLError

from labctl.topology import (
    HOST, PROXY_PORT, build_instances, topology_for_proxy,
)

LAB_DIR = os.path.join(os.getcwd(), ".chaoslab", "lab")
STATE_FILE = os.path.join(LAB_DIR, "state.json")
TOPO_FILE = os.path.join(LAB_DIR, "topology.json")


def _http_get(url: str, timeout: float = 2.0):
    return urllib.request.urlopen(url, timeout=timeout)


def _wait_health(url: str, timeout_s: float = 15.0) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            with _http_get(url, timeout=1) as resp:
                if resp.status == 200:
                    return True
        except (URLError, OSError):
            time.sleep(0.2)
    return False


class Lab:
    """靶场控制器：业务服务和故障注入代理都是独立子进程。"""

    def __init__(self) -> None:
        self.project_root = os.path.dirname(
            os.path.dirname(os.path.abspath(__file__)))

    def _spawn(self, module: str, args: list[str], log_name: str,
               env: dict) -> subprocess.Popen:
        log = open(os.path.join(LAB_DIR, f"{log_name}.log"), "w")
        return subprocess.Popen(
            [sys.executable, "-u", "-m", module, *args],
            stdout=log, stderr=subprocess.STDOUT, env=env,
            cwd=self.project_root)

    # ------------------------------------------------ 启动
    def up(self) -> None:
        os.makedirs(LAB_DIR, exist_ok=True)
        if os.path.exists(STATE_FILE):
            print("靶场已在运行，先执行 down 或使用 status 查看")
            return
        cfgs = build_instances()
        env = dict(os.environ, PYTHONPATH=self.project_root)
        procs: list[dict] = []
        for c in cfgs:
            proc = self._spawn(
                "labctl.service_runner",
                [c.service, c.instance_id, str(c.port)], c.instance_id, env)
            procs.append({"pid": proc.pid, "service": c.service,
                          "instance_id": c.instance_id, "port": c.port})
            print(f"  启动 {c.service:<14} {c.instance_id:<8} "
                  f"pid={proc.pid} port={c.port}")

        print("等待服务健康检查...")
        failed = [c.instance_id for c in cfgs
                  if not _wait_health(f"http://{HOST}:{c.port}/health")]
        if failed:
            print(f"健康检查失败: {failed}，执行清理")
            self._kill_procs(procs)
            raise SystemExit(1)

        # 代理作为独立长驻进程启动（拓扑写文件传给它）
        with open(TOPO_FILE, "w") as f:
            json.dump(topology_for_proxy(cfgs), f)
        proxy_proc = self._spawn(
            "labctl.proxy_runner", [str(PROXY_PORT), TOPO_FILE], "proxy", env)
        procs.append({"pid": proxy_proc.pid, "service": "_proxy",
                      "instance_id": "proxy", "port": PROXY_PORT})
        if not _wait_health(f"http://{HOST}:{PROXY_PORT}/proxy/health"):
            print("代理启动失败，执行清理")
            self._kill_procs(procs)
            raise SystemExit(1)

        state = {"instances": procs, "proxy_port": PROXY_PORT,
                 "proxy_pid": proxy_proc.pid, "started_at": time.time()}
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=2)
        print(f"\n靶场就绪：{len(cfgs)} 个服务实例 + 代理(:{PROXY_PORT})，"
              f"共 {len(procs)} 个进程")
        print(f"状态文件: {STATE_FILE}")

    # ------------------------------------------------ 关停
    def down(self) -> None:
        if not os.path.exists(STATE_FILE):
            print("没有运行中的靶场")
            return
        with open(STATE_FILE) as f:
            state = json.load(f)
        # 先清故障规则（尽力而为），再杀进程
        try:
            req = urllib.request.Request(
                f"http://{HOST}:{PROXY_PORT}/proxy/rules", method="DELETE")
            urllib.request.urlopen(req, timeout=2)
        except Exception:
            pass
        self._kill_procs(state["instances"])
        os.remove(STATE_FILE)
        print("靶场已关停，故障规则已清空")

    def status(self) -> None:
        if not os.path.exists(STATE_FILE):
            print("靶场未运行")
            return
        with open(STATE_FILE) as f:
            state = json.load(f)
        alive, dead = [], []
        for inst in state["instances"]:
            if inst["service"] == "_proxy":
                continue
            try:
                with _http_get(f"http://{HOST}:{inst['port']}/health", 1) as r:
                    (alive if r.status == 200 else dead).append(inst)
            except Exception:
                dead.append(inst)
        print(f"实例 {len(alive)}/{len(alive) + len(dead)} 存活")
        for inst in alive:
            print(f"  ✓ {inst['service']:<14} {inst['instance_id']} :{inst['port']}")
        for inst in dead:
            print(f"  ✗ {inst['service']:<14} {inst['instance_id']} :{inst['port']}")
        try:
            with _http_get(f"http://{HOST}:{PROXY_PORT}/proxy/health", 1) as r:
                proxy_ok = r.status == 200
        except Exception:
            proxy_ok = False
        print(f"代理 :{PROXY_PORT} {'✓' if proxy_ok else '✗'}")

    # ------------------------------------------------ 工具
    @staticmethod
    def _kill_procs(procs: list[dict]) -> None:
        for p in procs:
            try:
                os.kill(p["pid"], signal.SIGTERM)
            except ProcessLookupError:
                continue
        deadline = time.time() + 5
        live = {p["pid"] for p in procs}
        while time.time() < deadline and live:
            for pid in list(live):
                try:
                    os.kill(pid, 0)
                except ProcessLookupError:
                    live.discard(pid)
            time.sleep(0.2)
        for pid in live:  # 兜底强杀
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass


def read_state() -> dict | None:
    if not os.path.exists(STATE_FILE):
        return None
    with open(STATE_FILE) as f:
        return json.load(f)
