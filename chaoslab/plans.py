"""预置演练计划模板。

模板以 JSON 存库，可在控制台另存/复跑。三个模板覆盖核心场景：
1. sms_delay_incident：复现去年短信变慢事故，双模式对照（auto_abort 关闭，观察完整崩溃）
2. sms_delay_guarded：同样故障但开启稳态守护，演示自动中止与故障撤销
3. blast_radius_demo：只打带标记的请求 + 只打 sms-2 单实例
"""
from __future__ import annotations

from chaoslab.model import (
    DrillPlan, FaultRule, PlanStep, SteadyStateHypothesis,
)


def sms_delay_incident() -> DrillPlan:
    """头条演练：SMS 注入 3s 延迟，持续 40s，关/开韧性各跑一次。"""
    rule = FaultRule(
        name="sms-slow-3s", target="sms", fault_type="delay",
        delay_ms=3000, jitter_ms=200, percent=100)
    return DrillPlan(
        name="短信变慢事故复现",
        description="复现去年短信服务变慢导致全链路线程阻塞的事故；"
                    "先关熔断隔离跑一遍，再开启跑一遍，对比故障扩散范围。",
        duration_s=75.0,
        resilience_modes=["off", "on"],
        load_rps=36, load_concurrency=24,
        auto_abort=False,
        tags=["sms", "delay", "对照实验"],
        steady_states=[
            SteadyStateHypothesis(
                name="入口成功率不低于90%", metric="success_rate",
                threshold=0.90, comparator="lt", breach_limit=3),
            SteadyStateHypothesis(
                name="入口P95延迟不超过3秒", metric="latency_p95_ms",
                threshold=3000, comparator="gt", breach_limit=3),
        ],
        steps=[
            PlanStep(offset_s=0, action="pause", note="基线观察期"),
            PlanStep(offset_s=15, action="inject", rule=rule,
                     note="向短信服务注入3秒延迟"),
            PlanStep(offset_s=55, action="retract", rule=rule,
                     note="撤销故障，观察恢复"),
            PlanStep(offset_s=55, action="pause", note="恢复观察期"),
        ],
    )


def sms_delay_guarded() -> DrillPlan:
    """带稳态守护的演练：偏离自动中止并撤销全部故障。"""
    rule = FaultRule(
        name="sms-slow-3s-guard", target="sms", fault_type="delay",
        delay_ms=3000, jitter_ms=200)
    return DrillPlan(
        name="稳态守护自动中止演练",
        description="同样的短信慢故障，开启稳态指标守护，"
                    "入口成功率连续偏离时自动中止并撤销所有故障。",
        duration_s=75.0,
        resilience_modes=["on"],
        load_rps=36, load_concurrency=24,
        auto_abort=True,
        tags=["sms", "auto-abort"],
        steady_states=[
            SteadyStateHypothesis(
                name="入口成功率不低于95%", metric="success_rate",
                threshold=0.95, comparator="lt", breach_limit=3, interval_s=1.0),
            SteadyStateHypothesis(
                name="网关线程池饱和度不超过85%", metric="pool_saturation",
                threshold=0.85, comparator="gt", breach_limit=2,
                source_service="gateway"),
        ],
        steps=[
            PlanStep(offset_s=10, action="inject", rule=rule),
            PlanStep(offset_s=60, action="retract", rule=rule),
        ],
    )


def blast_radius_demo() -> DrillPlan:
    """爆炸半径：金丝雀标记 + 单实例 + 概率三重限制。"""
    rule = FaultRule(
        name="canary-only", target="sms", fault_type="delay",
        delay_ms=2000, tag_header="X-Chaos-Tag", tag_value="canary",
        instances=["sms-2"], percent=100)
    return DrillPlan(
        name="爆炸半径限制演练",
        description="故障只命中带 X-Chaos-Tag: canary 标记且路由到 sms-2 "
                    "实例的请求，普通用户流量不受影响。",
        duration_s=45.0,
        resilience_modes=["on"],
        load_rps=30, load_concurrency=16,
        auto_abort=False,
        tags=["blast-radius", "canary"],
        steady_states=[
            SteadyStateHypothesis(
                name="入口成功率不低于95%", metric="success_rate",
                threshold=0.95, comparator="lt", breach_limit=4),
        ],
        steps=[
            PlanStep(offset_s=8, action="inject", rule=rule,
                     note="仅金丝雀请求命中 sms-2"),
            PlanStep(offset_s=38, action="retract", rule=rule),
        ],
    )


def sms_error_incident() -> DrillPlan:
    """错误注入：短信直接返回 500，观察熔断快速失败与降级。"""
    rule = FaultRule(
        name="sms-500", target="sms", fault_type="error", error_code=500)
    return DrillPlan(
        name="短信错误故障演练",
        description="短信服务直接返回500，验证熔断器按错误率触发、"
                    "通知服务走降级（异步补发短信），主交易不受影响。",
        duration_s=60.0,
        resilience_modes=["off", "on"],
        load_rps=36, load_concurrency=24,
        auto_abort=False,
        tags=["sms", "error", "对照实验"],
        steady_states=[
            SteadyStateHypothesis(
                name="入口成功率不低于90%", metric="success_rate",
                threshold=0.90, comparator="lt", breach_limit=3),
        ],
        steps=[
            PlanStep(offset_s=10, action="inject", rule=rule),
            PlanStep(offset_s=45, action="retract", rule=rule),
        ],
    )


ALL_TEMPLATES = [sms_delay_incident, sms_delay_guarded,
                 blast_radius_demo, sms_error_incident]


def seed_templates(store) -> None:
    for factory in ALL_TEMPLATES:
        store.save_plan_template(factory())
