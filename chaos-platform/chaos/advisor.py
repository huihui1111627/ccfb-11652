"""超时建议:按每条调用边的历史延迟分位数给出超时值,不再拍脑袋。

建议值 = max(p99 * 1.5, p95 + 100ms, 200ms),向上取整到 100ms。
"""
import math
import time


def advise(services, settings, window_s=60):
    since = time.time() - window_s
    rows = []
    for caller, svc in sorted(services.items()):
        for ds_name, ds in sorted(svc.downstreams.items()):
            lat = svc.metrics.ds(ds_name).lat.snapshot(since=since)
            if lat["count"] < 20:
                continue
            rec = max(lat["p99"] * 1.5, lat["p95"] + 100, 200)
            rec = int(math.ceil(rec / 100.0) * 100)
            edge = "%s->%s" % (caller, ds_name)
            rows.append({
                "edge": edge,
                "samples": lat["count"],
                "p50": lat["p50"], "p95": lat["p95"], "p99": lat["p99"],
                "recommended_ms": rec,
                "current_ms": settings.timeout_for(caller, ds_name),
            })
    return rows


def apply(rows, settings):
    for r in rows:
        settings.set_timeout(r["edge"], r["recommended_ms"])
