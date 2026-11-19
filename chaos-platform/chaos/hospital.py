"""在线挂号缴费系统:11 个服务实例组成的同步调用链。

拓扑(所有下游调用都经过对应实例前面的故障注入代理):

  负载 -> gateway ──┬--> registration ──┬--> schedule --> doctor
       (8001)       │                   ├--> patient
                    │                   ├--> payment --> paychannel
                    │                   └--> sms --> smsprovider-1 / smsprovider-2
                    └--> inquiry --> patient

复现的事故:smsprovider(运营商通道)变慢 -> sms 线程占满 -> registration 线程
全部阻塞在 sms 调用上 -> gateway 线程池耗尽 -> 整个挂号系统不可用。
"""
import random
import time

from .proxy import FaultProxy
from .service import Service


def _sleep_ms(ms):
    time.sleep(ms / 1000.0)


# ---------------------------------------------------------------- 各服务业务逻辑

def doctor_api(ctx, p):
    _sleep_ms(20)
    return 200, {"doctor": "张医生", "dept": p.get("dept", "内科"), "remain": 3}


def patient_api(ctx, p):
    _sleep_ms(15)
    return 200, {"patient_id": p.get("patient_id", "P1001"), "level": "医保"}


def paychannel_api(ctx, p):
    _sleep_ms(80)
    return 200, {"txn": "T%08d" % random.randint(1, 99999999)}


def smsprovider_api(ctx, p):
    _sleep_ms(50)  # 运营商通道正常时 50ms;演练时注入 8s 延迟
    return 200, {"sent": True, "provider": "mock-carrier"}


def schedule_api(ctx, p):
    _, doc = ctx.call("doctor", "/api/doctor", p)
    _sleep_ms(10)
    return 200, {"slot": "A%03d" % random.randint(1, 100), "doctor": doc["doctor"]}


def payment_api(ctx, p):
    _, txn = ctx.call("paychannel", "/api/txn", p)
    _sleep_ms(10)
    return 200, {"txn": txn["txn"], "amount": 25.0}


def sms_api(ctx, p):
    _, r = ctx.call("smsprovider", "/send", p)
    return 200, {"sent": r.get("sent", False)}


def register_api(ctx, p):
    """挂号主流程:锁号源 -> 建档 -> 缴费 -> 发短信,同步串行。"""
    _, sch = ctx.call("schedule", "/api/schedule", p)
    _, pat = ctx.call("patient", "/api/patient", p)
    _, pay = ctx.call("payment", "/api/pay", p)
    _, smsr = ctx.call("sms", "/api/notify", p)
    _sleep_ms(5)
    degraded = bool(smsr.get("degraded"))
    return 200, {
        "ok": True,
        "registration_id": "R%06d" % random.randint(1, 999999),
        "slot": sch["slot"],
        "txn": pay["txn"],
        "degraded": degraded,
        "sms": "deferred" if degraded else "sent",
    }


def inquiry_api(ctx, p):
    _, pat = ctx.call("patient", "/api/patient", p)
    _sleep_ms(5)
    return 200, {"patient_id": pat["patient_id"],
                 "reports": [{"item": "血常规", "status": "已出"}]}


def gw_register(ctx, p):
    return ctx.call("registration", "/api/register", p)


def gw_inquiry(ctx, p):
    return ctx.call("inquiry", "/api/inquiry", p)


# ---------------------------------------------------------------- 集群构建

def _breaker(**over):
    cfg = {"window": 20, "min_calls": 8, "failure_rate": 0.5,
           "slow_rate": 0.5, "slow_ms": 800, "open_ms": 8000, "half_open": 3}
    cfg.update(over)
    return cfg


def sms_fallback(payload):
    """短信降级:挂号照常成功,通知稍后补发。"""
    return {"degraded": True, "sms": "deferred", "msg": "短信通知将延迟补发"}


def build_cluster(settings, rules, bus):
    services = {}
    proxies = []

    def add(name, port, proxy_port, workers, routes, downstreams=None,
            queue_size=100):
        svc = Service(name, port, workers, queue_size, routes,
                      downstreams or {}, settings, bus)
        services[name] = svc
        if proxy_port:
            proxies.append(FaultProxy(name, proxy_port, "127.0.0.1", port, rules))
        return svc

    P = "127.0.0.1"
    add("gateway", 8001, 9001, 30,
        {("POST", "/api/register"): gw_register,
         ("POST", "/api/inquiry"): gw_inquiry},
        downstreams={
            "registration": {"endpoints": [(P, 9002)], "pool_size": 20,
                             "bulkhead_size": 15, "breaker": _breaker()},
            "inquiry": {"endpoints": [(P, 9010)], "pool_size": 10,
                        "bulkhead_size": 8, "breaker": _breaker()},
        })
    add("registration", 8002, 9002, 20,
        {("POST", "/api/register"): register_api},
        downstreams={
            "schedule": {"endpoints": [(P, 9003)], "pool_size": 10,
                         "bulkhead_size": 8, "breaker": _breaker()},
            "patient": {"endpoints": [(P, 9005)], "pool_size": 10,
                        "bulkhead_size": 8, "breaker": _breaker()},
            "payment": {"endpoints": [(P, 9006)], "pool_size": 10,
                        "bulkhead_size": 8, "breaker": _breaker()},
            "sms": {"endpoints": [(P, 9008)], "pool_size": 10,
                    "bulkhead_size": 3, "breaker": _breaker(),
                    "fallback": sms_fallback},
        })
    add("schedule", 8003, 9003, 15,
        {("POST", "/api/schedule"): schedule_api},
        downstreams={
            "doctor": {"endpoints": [(P, 9004)], "pool_size": 10,
                       "bulkhead_size": 8, "breaker": _breaker()},
        })
    add("doctor", 8004, 9004, 10, {("POST", "/api/doctor"): doctor_api})
    add("patient", 8005, 9005, 10, {("POST", "/api/patient"): patient_api})
    add("payment", 8006, 9006, 15,
        {("POST", "/api/pay"): payment_api},
        downstreams={
            "paychannel": {"endpoints": [(P, 9007)], "pool_size": 10,
                           "bulkhead_size": 8, "breaker": _breaker()},
        })
    add("paychannel", 8007, 9007, 10, {("POST", "/api/txn"): paychannel_api})
    add("sms", 8008, 9008, 10,
        {("POST", "/api/notify"): sms_api},
        downstreams={
            "smsprovider": {"endpoints": [(P, 9009), (P, 9011)], "pool_size": 10,
                            "bulkhead_size": 4, "breaker": _breaker()},
        })
    add("smsprovider-1", 8009, 9009, 10, {("POST", "/send"): smsprovider_api})
    add("smsprovider-2", 8011, 9011, 10, {("POST", "/send"): smsprovider_api})
    add("inquiry", 8010, 9010, 10,
        {("POST", "/api/inquiry"): inquiry_api},
        downstreams={
            "patient": {"endpoints": [(P, 9005)], "pool_size": 10,
                        "bulkhead_size": 8, "breaker": _breaker()},
        })
    return services, proxies
