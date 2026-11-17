"""平台统一数据模型：注入规则、演练计划、稳态指标、运行记录。

所有对象都可序列化为 JSON 友好的 dict，计划模板即以 JSON 落盘。
"""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field, asdict
from typing import Any


# ---------------------------------------------------------------- 注入规则
@dataclass
class FaultRule:
    """单条故障注入规则（在代理上生效）。

    fault_type:
        - delay   : 增加固定延迟（毫秒），可叠加抖动
        - error   : 直接返回指定 HTTP 错误码，不转发
        - abort   : 直接断开 TCP 连接
    target:
        目标下游服务名（代理按 path 前缀 /service/ 匹配）。
    blast_radius:
        爆炸半径控制，三个条件取交集：
        - tag_header / tag_value : 只命中带该标记的请求（灰度/金丝雀）
        - instances              : 只命中指定实例 id；空列表 = 全部实例
        - percent                : 命中条件内再按概率抽样（0-100）
    """

    name: str
    target: str
    fault_type: str = "delay"          # delay / error / abort
    delay_ms: int = 0
    jitter_ms: int = 0
    error_code: int = 500
    tag_header: str = ""               # 如 X-Chaos-Tag；空串表示不按标记过滤
    tag_value: str = ""
    instances: list[str] = field(default_factory=list)
    percent: int = 100
    enabled: bool = True

    def rule_id(self) -> str:
        return f"{self.target}:{self.name}"


# ---------------------------------------------------------------- 稳态指标
@dataclass
class SteadyStateHypothesis:
    """稳态假设（演练中持续巡检，连续 breach 次偏离即触发自动中止）。

    指标来源：平台在演练期间对入口服务按 interval 秒发探测请求，
    同时汇总各服务 /metrics 暴露的错误率、池占用等指标。
    """

    name: str
    metric: str                        # success_rate / latency_p95_ms / error_rate / pool_saturation
    threshold: float                   # 阈值
    comparator: str = "lt"             # lt: 指标 < 阈值即偏离 / gt: 指标 > 阈值即偏离
    interval_s: float = 2.0
    breach_limit: int = 3              # 连续偏离次数达到该值 → 自动中止
    source_service: str = "gateway"    # 取哪个服务的指标（探测类指标固定走 gateway）


# ---------------------------------------------------------------- 演练步骤
@dataclass
class PlanStep:
    """演练时间线上的一个步骤。offset_s 相对演练开始的偏移秒数。

    action:
        inject  : 向代理下发 rule
        retract : 撤销某条规则（按 rule_name）
        pause   : 纯等待（用于观察恢复）
        load    : 调整负载档位（rps / concurrency），可选
    """

    offset_s: float
    action: str
    rule: FaultRule | None = None
    rule_name: str = ""
    note: str = ""
    rps: int = 0
    concurrency: int = 0


# ---------------------------------------------------------------- 演练计划
@dataclass
class DrillPlan:
    name: str
    description: str = ""
    duration_s: float = 90.0
    # 韧性模式：on = 超时+熔断+隔离+降级全部生效；off = 全部关闭（裸调）
    resilience_modes: list[str] = field(default_factory=lambda: ["off", "on"])
    steps: list[PlanStep] = field(default_factory=list)
    steady_states: list[SteadyStateHypothesis] = field(default_factory=list)
    load_rps: int = 40
    load_concurrency: int = 24
    # auto_abort=True 时稳态偏离会中止演练；对照实验通常关掉以便观察完整崩溃
    auto_abort: bool = True
    tags: list[str] = field(default_factory=list)

    def sorted_steps(self) -> list[PlanStep]:
        return sorted(self.steps, key=lambda s: s.offset_s)


# ---------------------------------------------------------------- 运行记录
@dataclass
class RunRecord:
    """一次具体执行（一个计划 × 一种韧性模式 = 一次 run）。"""

    plan_name: str
    resilience_mode: str               # on / off
    run_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    started_at: float = field(default_factory=time.time)
    finished_at: float = 0.0
    status: str = "running"            # running / completed / aborted / failed
    abort_reason: str = ""
    # 各服务聚合指标快照（结束时汇总）
    summary: dict[str, Any] = field(default_factory=dict)
    # 稳态巡检结果序列
    steady_checks: list[dict[str, Any]] = field(default_factory=list)
    # 关键事件（注入/撤销/熔断翻转/中止…）
    events: list[dict[str, Any]] = field(default_factory=list)
    # 指标时序数据在 store 里按 run_id 分表存，这里只记统计量
    notes: str = ""

    def duration_s(self) -> float:
        return max(0.0, self.finished_at - self.started_at)


# ---------------------------------------------------------------- 序列化辅助
def to_jsonable(obj: Any) -> Any:
    """dataclass / 嵌套结构 → 可 json.dumps 的原生类型。"""
    if hasattr(obj, "__dataclass_fields__"):
        return {k: to_jsonable(v) for k, v in asdict(obj).items()}
    if isinstance(obj, dict):
        return {k: to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [to_jsonable(v) for v in obj]
    return obj


def rule_from_dict(d: dict[str, Any]) -> FaultRule:
    known = {f for f in FaultRule.__dataclass_fields__}  # type: ignore[attr-defined]
    return FaultRule(**{k: v for k, v in d.items() if k in known})


def step_from_dict(d: dict[str, Any]) -> PlanStep:
    rule = rule_from_dict(d["rule"]) if d.get("rule") else None
    known = {f for f in PlanStep.__dataclass_fields__}  # type: ignore[attr-defined]
    kwargs = {k: v for k, v in d.items() if k in known and k != "rule"}
    return PlanStep(rule=rule, **kwargs)


def plan_from_dict(d: dict[str, Any]) -> DrillPlan:
    steps = [step_from_dict(s) for s in d.get("steps", [])]
    ssh = [SteadyStateHypothesis(**s) for s in d.get("steady_states", [])]
    known = {f for f in DrillPlan.__dataclass_fields__}  # type: ignore[attr-defined]
    kwargs = {k: v for k, v in d.items() if k in known and k not in ("steps", "steady_states")}
    return DrillPlan(steps=steps, steady_states=ssh, **kwargs)
