"""演练报告:汇总单次演练、给出故障传播链和"没守住的地方",并支持两次演练对比。"""


def _fmt_pct(x):
    return "%.1f%%" % (x * 100)


def build_summary(run, service_names):
    series = run["series"]
    targets = {r["target"] for r in run["plan"].get("rules", [])}
    per = {}
    for svc in service_names:
        snaps = [(e["t"], e["services"].get(svc)) for e in series]
        snaps = [(t, s) for t, s in snaps if s]
        if not snaps:
            continue
        first, last = snaps[0][1], snaps[-1][1]
        reqs = last["requests"] - first["requests"]
        errs = last["errors"] - first["errors"]
        max_active = max(s["pool"]["active"] for _, s in snaps)
        max_queued = max(s["pool"]["queued"] for _, s in snaps)
        pool_size = last["pool"]["size"]
        rejected = last["pool"]["rejected"] - first["pool"]["rejected"]
        p95_max = max(s["lat"]["p95"] for _, s in snaps)
        first_impact = None
        for i in range(1, len(snaps)):
            t, s = snaps[i]
            _, prev = snaps[i - 1]
            dr = s["requests"] - prev["requests"]
            de = s["errors"] - prev["errors"]
            saturated = (s["pool"]["active"] >= 0.9 * s["pool"]["size"]
                         or s["pool"]["queued"] > 0)
            rej = s["pool"]["rejected"] - prev["pool"]["rejected"] > 0
            if (dr >= 3 and de >= 2 and de / dr > 0.05) or saturated or rej:
                first_impact = t
                break
        per[svc] = {
            "requests": reqs, "errors": errs,
            "error_rate": round(errs / max(reqs, 1), 4),
            "max_pool_active": max_active, "max_pool_queued": max_queued,
            "pool_size": pool_size,
            "pool_saturation": round(max_active / max(pool_size, 1), 3),
            "rejected": rejected, "p95_max": p95_max,
            "first_impact_t": first_impact,
            "is_target": svc in targets,
        }
    client = run.get("client_summary") or {}
    blast_zone = set(run["plan"].get("blast_zone", []))
    propagation = sorted(
        ((svc, p["first_impact_t"]) for svc, p in per.items()
         if p["first_impact_t"] is not None and not p["is_target"]),
        key=lambda x: x[1])
    breached = [(s, t) for s, t in propagation if s not in blast_zone]
    absorbed = [s for s, _ in propagation if s in blast_zone]
    breaker_opens = sum(1 for e in run["events"]
                        if e.get("type") == "breaker" and "-> OPEN" in e.get("msg", ""))

    verdicts = []
    if run.get("aborted"):
        verdicts.append("稳态指标失守,演练被自动中止,全部故障已撤除: %s"
                        % run.get("abort_reason", ""))
    else:
        verdicts.append("稳态指标全程保持,演练按计划完成")
    if breached:
        chain = " -> ".join(["/".join(sorted(targets))]
                            + [s for s, _ in breached])
        verdicts.append("未守住:故障沿调用链扩散: %s" % chain)
    elif absorbed:
        verdicts.append("故障被控制在预期爆炸半径(%s)内,未继续向上扩散"
                        % "/".join(absorbed))
    elif any(p["is_target"] for p in per.values()):
        verdicts.append("故障被限制在注入点(%s),未发生扩散"
                        % "/".join(sorted(targets)))
    if client.get("degraded"):
        verdicts.append("共有 %d 次请求走了降级逻辑(短信延迟补发),挂号主流程未中断"
                        % client["degraded"])
    if breaker_opens:
        verdicts.append("熔断器共断开 %d 次" % breaker_opens)
    if run["resilience"] and not breached and not run.get("aborted"):
        verdicts.append("熔断/隔离/降级生效,系统扛住了本次故障")
    if not run["resilience"] and breached:
        verdicts.append("无熔断隔离时,单个下游的缓慢拖垮了整条同步调用链")

    return {
        "client": client,
        "per_service": per,
        "propagation": [{"service": s, "t": t} for s, t in breached],
        "absorbed": absorbed,
        "targets": sorted(targets),
        "breaker_opens": breaker_opens,
        "verdicts": verdicts,
    }


def compare(run_a, run_b):
    """两次演练对比(典型用法:同一计划,关韧性 vs 开韧性)。"""
    sa, sb = run_a.get("summary") or {}, run_b.get("summary") or {}
    ca, cb = sa.get("client") or {}, sb.get("client") or {}

    def row(name, fa, fb, fmt=str):
        return {"metric": name, "a": fmt(fa), "b": fmt(fb)}

    rows = [
        row("韧性模式", "开" if run_a["resilience"] else "关",
            "开" if run_b["resilience"] else "关"),
        row("客户端总请求", ca.get("count", 0), cb.get("count", 0)),
        row("客户端错误率", ca.get("error_rate", 0), cb.get("error_rate", 0),
            _fmt_pct),
        row("客户端 p95 (ms)", ca.get("p95", 0), cb.get("p95", 0)),
        row("客户端 p99 (ms)", ca.get("p99", 0), cb.get("p99", 0)),
        row("降级请求数", ca.get("degraded", 0), cb.get("degraded", 0)),
        row("稳态", "失守(自动中止)" if run_a.get("aborted") else "保持",
            "失守(自动中止)" if run_b.get("aborted") else "保持"),
        row("故障扩散服务数", len(sa.get("propagation", [])),
            len(sb.get("propagation", []))),
        row("扩散链",
            " -> ".join(p["service"] for p in sa.get("propagation", [])) or "无",
            " -> ".join(p["service"] for p in sb.get("propagation", [])) or "无"),
        row("熔断器断开次数", sa.get("breaker_opens", 0), sb.get("breaker_opens", 0)),
    ]
    per_svc = {}
    for svc in sorted(set(list((sa.get("per_service") or {}).keys())
                          + list((sb.get("per_service") or {}).keys()))):
        pa = (sa.get("per_service") or {}).get(svc, {})
        pb = (sb.get("per_service") or {}).get(svc, {})
        per_svc[svc] = {
            "a": {"error_rate": pa.get("error_rate", 0),
                  "pool_saturation": pa.get("pool_saturation", 0),
                  "rejected": pa.get("rejected", 0)},
            "b": {"error_rate": pb.get("error_rate", 0),
                  "pool_saturation": pb.get("pool_saturation", 0),
                  "rejected": pb.get("rejected", 0)},
        }
    return {"a": {"id": run_a["id"], "plan": run_a["plan_name"]},
            "b": {"id": run_b["id"], "plan": run_b["plan_name"]},
            "rows": rows, "per_service": per_svc}
