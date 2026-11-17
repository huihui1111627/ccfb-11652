"""挂号系统靶场服务定义。

10 个服务、11 个实例，同步调用链（对应真实挂号缴费场景）：

    gateway 网关
      ├─ registration 挂号
      │    ├─ patient 患者信息
      │    │    └─ profile 档案
      │    ├─ schedule 号源排班 ── inventory 号源库存
      │    └─ payment 缴费 ── billing 计费
      └─ notification 通知
           ├─ sms 短信（2 实例，故障演练的主角）
           └─ push 推送

每个下游依赖配置：超时 + 熔断器 + 连接池舱壁 + 降级。
韧性模式 off 时全部退化为裸调，复现“一个慢服务拖垮整条链路”。
"""
from __future__ import annotations

import json
import time
from typing import Any

from chaoslab.http_tools import ServiceApp


def _json_body(ctx: dict) -> dict[str, Any]:
    try:
        return json.loads(ctx.get("body") or b"{}")
    except json.JSONDecodeError:
        return {}


def _call_json(app: ServiceApp, dep: str, path: str, ctx: dict,
               headers: dict[str, str] | None = None) -> dict[str, Any]:
    hdrs = {"X-Request-Id": ctx["request_id"]}
    if headers:
        hdrs.update(headers)
    status, body = app.caller.call(dep, path, headers=hdrs)
    return json.loads(body or b"{}")


# ---------------------------------------------------------------- 叶子服务
def build_profile(cfg) -> ServiceApp:
    app = ServiceApp("profile", cfg.host, cfg.port, cfg.instance_id,
                     proxy_base=cfg.proxy_base)
    app.latency_budget_ms = 8

    @app.route("/info")
    def info(ctx):
        return 200, {"patient_id": "P-1001", "name": "张某某",
                     "id_card_masked": "110***********0011", "level": "普通门诊"}
    return app


def build_inventory(cfg) -> ServiceApp:
    app = ServiceApp("inventory", cfg.host, cfg.port, cfg.instance_id,
                     proxy_base=cfg.proxy_base)
    app.latency_budget_ms = 6

    @app.route("/hold")
    def hold(ctx):
        return 200, {"held": True, "stock_left": 27, "slot": "2026-11-18 上午 18 号"}
    return app


def build_billing(cfg) -> ServiceApp:
    app = ServiceApp("billing", cfg.host, cfg.port, cfg.instance_id,
                     proxy_base=cfg.proxy_base)
    app.latency_budget_ms = 12

    @app.route("/charge")
    def charge(ctx):
        data = _json_body(ctx)
        return 200, {"bill_id": "B-9001", "amount": 50.0,
                     "channel": data.get("channel", "wechat"), "status": "charged"}
    return app


def build_sms(cfg) -> ServiceApp:
    app = ServiceApp("sms", cfg.host, cfg.port, cfg.instance_id,
                     proxy_base=cfg.proxy_base)
    app.latency_budget_ms = 25  # 正常情况下短信就不算快

    @app.route("/send")
    def send(ctx):
        data = _json_body(ctx)
        return 200, {"sent": True, "template": "register_success",
                     "to_masked": "138****6688", "instance": app.instance_id,
                     "msg_id": f"S-{int(time.time()*1000)}-{data.get('patient_id', 'x')}"}
    return app


def build_push(cfg) -> ServiceApp:
    app = ServiceApp("push", cfg.host, cfg.port, cfg.instance_id,
                     proxy_base=cfg.proxy_base)
    app.latency_budget_ms = 10

    @app.route("/send")
    def send(ctx):
        return 200, {"pushed": True, "device": "app-device-a1"}
    return app


# ---------------------------------------------------------------- 中层服务
def build_patient(cfg) -> ServiceApp:
    app = ServiceApp("patient", cfg.host, cfg.port, cfg.instance_id,
                     proxy_base=cfg.proxy_base)
    app.latency_budget_ms = 5
    app.caller.register_dependency(
        "profile", timeout_s=0.5, pool_size=8, slow_call_ms=300,
        fallback=lambda exc: {"patient_id": "P-1001", "degraded": True,
                              "reason": type(exc).__name__})

    @app.route("/lookup")
    def lookup(ctx):
        info = _call_json(app, "profile", "/info", ctx)
        return 200, {**info, "verified": True}
    return app


def build_schedule(cfg) -> ServiceApp:
    app = ServiceApp("schedule", cfg.host, cfg.port, cfg.instance_id,
                     proxy_base=cfg.proxy_base)
    app.latency_budget_ms = 7
    app.caller.register_dependency(
        "inventory", timeout_s=0.5, pool_size=8, slow_call_ms=300,
        fallback=lambda exc: {"held": False, "degraded": True,
                              "reason": type(exc).__name__})

    @app.route("/reserve")
    def reserve(ctx):
        held = _call_json(app, "inventory", "/hold", ctx)
        return 200, {"schedule_id": "SC-77", **held}
    return app


def build_payment(cfg) -> ServiceApp:
    app = ServiceApp("payment", cfg.host, cfg.port, cfg.instance_id,
                     proxy_base=cfg.proxy_base)
    app.latency_budget_ms = 10
    app.caller.register_dependency(
        "billing", timeout_s=1.0, pool_size=8, slow_call_ms=500,
        fallback=lambda exc: {"bill_id": None, "status": "pending_retry",
                              "degraded": True, "reason": type(exc).__name__})

    @app.route("/pay")
    def pay(ctx):
        data = _json_body(ctx)
        bill = _call_json(app, "billing", "/charge", ctx)
        return 200, {"paid": bill.get("status") == "charged", **bill,
                     "order_id": data.get("order_id", "O-1")}
    return app


def build_notification(cfg) -> ServiceApp:
    app = ServiceApp("notification", cfg.host, cfg.port, cfg.instance_id,
                     proxy_base=cfg.proxy_base)
    app.latency_budget_ms = 5
    # 短信：慢调用阈值按正常 p95 的余量设定，演练时慢故障会触发熔断
    app.caller.register_dependency(
        "sms", timeout_s=1.2, pool_size=6, slow_call_ms=800,
        open_hold_s=4.0,
        fallback=lambda exc: {"sent": False, "channel": "sms",
                              "degraded": True, "reason": type(exc).__name__})
    app.caller.register_dependency(
        "push", timeout_s=0.8, pool_size=6, slow_call_ms=400,
        fallback=lambda exc: {"pushed": False, "degraded": True,
                              "reason": type(exc).__name__})

    @app.route("/notify")
    def notify(ctx):
        data = _json_body(ctx)
        # 短信和推送并行发出（两个同步调用，互不依赖结果）
        sms_res = _call_json(app, "sms", "/send", ctx)
        push_res = _call_json(app, "push", "/send", ctx)
        return 200, {"notified": True, "sms": sms_res, "push": push_res,
                     "scene": data.get("scene", "register")}
    return app


def build_registration(cfg) -> ServiceApp:
    app = ServiceApp("registration", cfg.host, cfg.port, cfg.instance_id,
                     proxy_base=cfg.proxy_base)
    app.latency_budget_ms = 8
    app.caller.register_dependency(
        "patient", timeout_s=0.8, pool_size=10, slow_call_ms=400,
        fallback=lambda exc: {"verified": False, "degraded": True,
                              "reason": type(exc).__name__})
    app.caller.register_dependency(
        "schedule", timeout_s=0.8, pool_size=10, slow_call_ms=400,
        fallback=lambda exc: {"schedule_id": None, "held": False,
                              "degraded": True, "reason": type(exc).__name__})
    app.caller.register_dependency(
        "payment", timeout_s=1.5, pool_size=10, slow_call_ms=700,
        fallback=lambda exc: {"paid": False, "status": "pending_retry",
                              "degraded": True, "reason": type(exc).__name__})

    @app.route("/create")
    def create(ctx):
        patient = _call_json(app, "patient", "/lookup", ctx)
        schedule = _call_json(app, "schedule", "/reserve", ctx)
        pay = _call_json(app, "payment", "/pay", ctx)
        return 200, {
            "registration_id": "REG-20261118-0007",
            "patient": patient, "schedule": schedule, "payment": pay,
            "state": "completed" if pay.get("paid") else "pay_pending",
        }
    return app


# ---------------------------------------------------------------- 入口网关
def build_gateway(cfg) -> ServiceApp:
    app = ServiceApp("gateway", cfg.host, cfg.port, cfg.instance_id,
                     pool_size=24, proxy_base=cfg.proxy_base)
    app.latency_budget_ms = 3
    app.caller.register_dependency(
        "registration", timeout_s=3.0, pool_size=16, slow_call_ms=1500,
        fallback=lambda exc: {"state": "degraded",
                              "message": "挂号通道繁忙，请稍后重试",
                              "reason": type(exc).__name__})
    app.caller.register_dependency(
        "notification", timeout_s=2.0, pool_size=12, slow_call_ms=1000,
        fallback=lambda exc: {"notified": False, "degraded": True,
                              "reason": type(exc).__name__})

    @app.route("/register")
    def register(ctx):
        # 完整挂号链路：先挂号（内含患者/排班/缴费），再发通知
        reg = _call_json(app, "registration", "/create", ctx)
        notice = _call_json(app, "notification", "/notify", ctx)
        return 200, {"ok": True, "registration": reg, "notification": notice}

    @app.route("/ping")
    def ping(ctx):
        """稳态探测专用轻量端点：只走通知→短信链，模拟健康检查请求。"""
        notice = _call_json(app, "notification", "/notify", ctx)
        return 200, {"ok": True, "notification": notice}
    return app


# 服务名 → 构建函数
BUILDERS = {
    "gateway": build_gateway,
    "registration": build_registration,
    "notification": build_notification,
    "patient": build_patient,
    "schedule": build_schedule,
    "payment": build_payment,
    "profile": build_profile,
    "inventory": build_inventory,
    "billing": build_billing,
    "sms": build_sms,
    "push": build_push,
}
