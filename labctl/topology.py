"""靶场拓扑：服务名、实例、端口、调用边。

代理固定 8200，控制台 8300，业务服务从 8400 开始分配。
"""
from __future__ import annotations

from dataclasses import dataclass


PROXY_PORT = 8200
CONSOLE_PORT = 8300
HOST = "127.0.0.1"


@dataclass
class InstanceCfg:
    service: str
    instance_id: str
    port: int
    host: str = HOST

    @property
    def proxy_base(self) -> str:
        return f"http://{HOST}:{PROXY_PORT}"


# (服务名, 实例 id 后缀) —— sms 双实例，用于演示爆炸半径限定到单实例
INSTANCE_SPECS: list[tuple[str, str]] = [
    ("gateway", "gw-1"),
    ("registration", "reg-1"),
    ("notification", "ntf-1"),
    ("patient", "pat-1"),
    ("schedule", "sch-1"),
    ("payment", "pay-1"),
    ("profile", "prf-1"),
    ("inventory", "inv-1"),
    ("billing", "bil-1"),
    ("sms", "sms-1"),
    ("sms", "sms-2"),
    ("push", "psh-1"),
]

# 同步调用边（父 → 子），控制台拓扑图与报告传播分析共用
TOPO_EDGES: list[tuple[str, str]] = [
    ("gateway", "registration"),
    ("gateway", "notification"),
    ("registration", "patient"),
    ("registration", "schedule"),
    ("registration", "payment"),
    ("patient", "profile"),
    ("schedule", "inventory"),
    ("payment", "billing"),
    ("notification", "sms"),
    ("notification", "push"),
]


def build_instances() -> list[InstanceCfg]:
    cfgs = []
    port = 8400
    for service, instance_id in INSTANCE_SPECS:
        cfgs.append(InstanceCfg(service=service, instance_id=instance_id, port=port))
        port += 1
    return cfgs


def topology_for_proxy(cfgs: list[InstanceCfg]) -> dict[str, list[dict[str, str]]]:
    topo: dict[str, list[dict[str, str]]] = {}
    for c in cfgs:
        topo.setdefault(c.service, []).append({
            "instance_id": c.instance_id, "host": c.host, "port": str(c.port)})
    return topo


def gateway_port(cfgs: list[InstanceCfg]) -> int:
    return next(c.port for c in cfgs if c.service == "gateway")
