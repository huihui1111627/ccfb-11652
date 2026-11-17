"""演练报告：纯 Python 生成自包含 HTML（内联 SVG，无外部依赖）。

报告内容（对应需求 6、8）：
- 总览：状态、是否触发自动中止、负载成功/错误/降级统计；
- 稳态指标巡检时间线；
- 线程池/连接池（舱壁）占用曲线，标注故障注入/撤销时刻；
- 熔断器状态翻转时间线；
- 故障传播分析：按错误率/池饱和抬头的时间排序，还原一层层向上传播的路径；
- 每个故障下系统表现与未守住项（自动 findings）；
- 双跑对比：off vs on 的错误率、延迟、扩散服务数。
"""
from __future__ import annotations

import html
import json
import os
import time
from typing import Any

from chaoslab.store import Store

COLORS = ["#2563eb", "#dc2626", "#16a34a", "#d97706", "#7c3aed",
          "#0891b2", "#be185d", "#65a30d", "#ea580c", "#4f46e5",
          "#0d9488", "#a16207"]


# ---------------------------------------------------------------- SVG 基础
def _svg_line_chart(series: list[dict[str, Any]], width: int = 860,
                    height: int = 260, title: str = "",
                    y_max: float | None = None, y_label: str = "") -> str:
    """series: [{name, points:[(t, v)], color?}]；t 为相对秒。"""
    if not any(s["points"] for s in series):
        return f'<p class="empty">{html.escape(title)}：无数据</p>'
    pad_l, pad_r, pad_t, pad_b = 56, 160, 28, 34
    plot_w = width - pad_l - pad_r
    plot_h = height - pad_t - pad_b
    all_t = [p[0] for s in series for p in s["points"]]
    all_v = [p[1] for s in series for p in s["points"]]
    t_min, t_max = min(all_t), max(all_t) or 1
    v_max = y_max if y_max is not None else (max(all_v) * 1.15 or 1)
    v_min = 0.0

    def x(t: float) -> float:
        return pad_l + (t - t_min) / max(1e-9, t_max - t_min) * plot_w

    def y(v: float) -> float:
        return pad_t + plot_h - (v - v_min) / max(1e-9, v_max - v_min) * plot_h

    parts = [f'<svg viewBox="0 0 {width} {height}" class="chart">']
    parts.append(f'<text x="{width/2}" y="18" text-anchor="middle" class="chart-title">'
                 f'{html.escape(title)}</text>')
    # 网格与 y 轴刻度
    for i in range(5):
        v = v_min + (v_max - v_min) * i / 4
        yy = y(v)
        parts.append(f'<line x1="{pad_l}" y1="{yy:.1f}" x2="{pad_l+plot_w}" '
                     f'y2="{yy:.1f}" class="grid"/>')
        parts.append(f'<text x="{pad_l-8}" y="{yy+4:.1f}" text-anchor="end" '
                     f'class="axis">{v:.2f}{html.escape(y_label)}</text>')
    # x 轴刻度
    for i in range(6):
        t = t_min + (t_max - t_min) * i / 5
        xx = x(t)
        parts.append(f'<text x="{xx:.1f}" y="{height-10}" text-anchor="middle" '
                     f'class="axis">{t:.0f}s</text>')
    parts.append(f'<line x1="{pad_l}" y1="{pad_t}" x2="{pad_l}" '
                 f'y2="{pad_t+plot_h}" class="axis-line"/>')
    parts.append(f'<line x1="{pad_l}" y1="{pad_t+plot_h}" x2="{pad_l+plot_w}" '
                 f'y2="{pad_t+plot_h}" class="axis-line"/>')
    # 折线
    for idx, s in enumerate(series):
        color = s.get("color") or COLORS[idx % len(COLORS)]
        pts = s["points"]
        if len(pts) < 2:
            continue
        d = "M " + " L ".join(f"{x(t):.1f},{y(v):.1f}" for t, v in pts)
        parts.append(f'<path d="{d}" fill="none" stroke="{color}" stroke-width="1.8"/>')
        lx, ly = pad_l + plot_w + 10, pad_t + 14 + idx * 20
        parts.append(f'<rect x="{lx}" y="{ly-9}" width="12" height="3" fill="{color}"/>')
        parts.append(f'<text x="{lx+16}" y="{ly-4}" class="legend">'
                     f'{html.escape(s["name"])}</text>')
    parts.append("</svg>")
    return "".join(parts)


def _svg_timeline(events: list[dict[str, Any]], t0: float,
                  width: int = 860, title: str = "") -> str:
    """事件时间线条带。"""
    if not events:
        return f'<p class="empty">{html.escape(title)}：无事件</p>'
    height = 38 + 26 * len(events)
    pad_l, pad_r = 120, 20
    plot_w = width - pad_l - pad_r
    t_max = max((e["ts"] - t0 for e in events), default=1) or 1
    colors = {"fault_injected": "#dc2626", "fault_retracted": "#16a34a",
              "breaker_opened": "#d97706", "breaker_half_open": "#7c3aed",
              "breaker_closed": "#16a34a", "breaker_reopened": "#dc2626",
              "all_faults_cleared": "#0891b2", "bulkhead_rejected": "#be185d"}
    parts = [f'<svg viewBox="0 0 {width} {height}" class="chart">']
    parts.append(f'<text x="{width/2}" y="18" text-anchor="middle" class="chart-title">'
                 f'{html.escape(title)}</text>')
    for i, e in enumerate(events):
        yy = 38 + i * 26
        rel = e["ts"] - t0
        xx = pad_l + rel / t_max * plot_w
        color = colors.get(e["event"], "#475569")
        parts.append(f'<text x="{pad_l-8}" y="{yy+4}" text-anchor="end" class="axis">'
                     f'{html.escape(e["event"])}</text>')
        parts.append(f'<line x1="{pad_l}" y1="{yy}" x2="{width-pad_r}" y2="{yy}" '
                     f'class="grid"/>')
        parts.append(f'<circle cx="{xx:.1f}" cy="{yy}" r="5" fill="{color}"/>')
        label = e.get("rule") or e.get("breaker") or e.get("mode") or ""
        parts.append(f'<text x="{xx+8:.1f}" y="{yy+4}" class="axis">'
                     f'{rel:.1f}s {html.escape(str(label))}</text>')
    parts.append("</svg>")
    return "".join(parts)


# ---------------------------------------------------------------- 数据加工
def _series_from_metrics(rows: list[dict], t0: float) -> dict[str, Any]:
    """把采集时序转成各图表需要的序列。"""
    thread_pool: dict[str, list[tuple[float, float]]] = {}
    pool_reject: dict[str, list[tuple[float, float]]] = {}
    breaker_state: dict[str, list[tuple[float, float]]] = {}
    breaker_reject: dict[str, list[tuple[float, float]]] = {}
    state_num = {"closed": 0, "half-open": 0.5, "open": 1}
    for r in rows:
        rel = r["ts"] - t0
        iid = r["service"]
        tp = r.get("thread_pool", {})
        thread_pool.setdefault(iid, []).append((rel, tp.get("saturation", 0.0)))
        pool_reject.setdefault(iid, []).append((rel, tp.get("rejected", 0)))
        for dep, d in r.get("dependencies", {}).items():
            key = f"{iid}→{dep}"
            breaker_state.setdefault(key, []).append(
                (rel, state_num.get(d.get("breaker_state"), 0)))
            breaker_reject.setdefault(key, []).append(
                (rel, d.get("breaker_rejected", 0)))
    return {"thread_pool": thread_pool, "pool_reject": pool_reject,
            "breaker_state": breaker_state, "breaker_reject": breaker_reject}


def _propagation_analysis(run_detail: dict, rows: list[dict],
                          t0: float) -> list[dict[str, Any]]:
    """需求 6：找出每个服务错误/饱和/池拒绝首次抬头的时刻，按时间排序即传播路径。

    受影响判据有三类，覆盖不同韧性模式：
    - requests.error：请求进了业务但处理失败（ON 模式降级前的错误）
    - 线程池饱和≥70% 或池拒绝：OFF 模式下请求在入口就被挡下，
      不会产生 requests.error，但池被占光本身就是故障命中的证据
    - 熔断器打开 / 舱壁拒绝：韧性组件动作
    """
    first_impact: dict[str, dict[str, Any]] = {}
    for r in rows:
        rel = r["ts"] - t0
        iid = r["service"]
        svc = r.get("service_name") or r.get("service") or iid
        tp = r.get("thread_pool", {})
        counters = r.get("counters", {})
        err = counters.get("requests.error", 0)
        sat = tp.get("saturation", 0.0)
        pool_rejected = tp.get("rejected", 0)
        entry = first_impact.setdefault(iid, {
            "instance": iid, "service": svc,
            "first_error_at": None, "first_saturation_at": None,
            "first_pool_reject_at": None,
            "max_saturation": 0.0, "total_pool_rejected": 0,
            "breaker_opened_at": None, "bulkhead_rejected": 0})
        entry["max_saturation"] = round(max(entry["max_saturation"], sat), 3)
        entry["total_pool_rejected"] = max(
            entry["total_pool_rejected"], pool_rejected)
        if err > 0 and entry["first_error_at"] is None:
            entry["first_error_at"] = round(rel, 1)
        if sat >= 0.7 and entry["first_saturation_at"] is None:
            entry["first_saturation_at"] = round(rel, 1)
        if pool_rejected > 0 and entry["first_pool_reject_at"] is None:
            entry["first_pool_reject_at"] = round(rel, 1)
        for evt in r.get("events", []):
            if evt.get("event") == "breaker_opened" and entry["breaker_opened_at"] is None:
                entry["breaker_opened_at"] = round(rel, 1)
        for dep, d in r.get("dependencies", {}).items():
            entry["bulkhead_rejected"] = max(entry["bulkhead_rejected"],
                                             d.get("pool_rejected", 0))
    impacted = [e for e in first_impact.values()
                if e["first_error_at"] is not None
                or (e["first_saturation_at"] is not None)
                or e["total_pool_rejected"] > 0
                or e["breaker_opened_at"] is not None]
    # 按最早受影响时刻排序：错误优先，其次饱和，再次池拒绝
    def _earliest(e):
        candidates = [x for x in (e["first_error_at"],
                                  e["first_saturation_at"],
                                  e["first_pool_reject_at"]) if x is not None]
        return min(candidates) if candidates else 9999
    impacted.sort(key=_earliest)
    return impacted


def _findings(run_detail: dict, impacted: list[dict]) -> list[str]:
    """需求 8：自动列出每个故障下没守住的地方。"""
    out: list[str] = []
    summary = run_detail.get("summary", {})
    load = summary.get("load", {})
    if load.get("error_rate", 0) > 0.05:
        out.append(f"入口错误率 {load['error_rate']*100:.1f}% 超过 5% 红线，"
                   f"错误分类：{load.get('error_kinds', {})}")
    if load.get("latency_p95_ms", 0) > 2000:
        out.append(f"入口 P95 延迟 {load['latency_p95_ms']:.0f}ms，用户侧已明显感知卡顿")
    if run_detail.get("status") == "aborted":
        out.append(f"触发自动中止：{run_detail.get('abort_reason')}（保护机制生效）")
    for e in impacted:
        if e["max_saturation"] >= 0.9:
            out.append(f"{e['instance']} 线程池峰值饱和度 {e['max_saturation']*100:.0f}%，"
                       f"接近资源耗尽")
        if e["bulkhead_rejected"]:
            out.append(f"{e['instance']} 连接池舱壁拒绝 {e['bulkhead_rejected']} 次"
                       f"（隔离生效，未拖垮本服务）")
        if e["breaker_opened_at"] is not None:
            out.append(f"{e['instance']} 熔断器在 {e['breaker_opened_at']}s 打开，"
                       f"后续请求快速失败")
    if not out:
        out.append("各项稳态指标均在红线内，韧性防线守住了本次故障")
    return out


# ---------------------------------------------------------------- 单 run 报告
def build_run_report(store: Store, run_id: str, out_dir: str) -> str:
    detail = store.get_run(run_id)
    if not detail:
        raise KeyError(run_id)
    rows = store.get_series(run_id)
    t0 = detail["started_at"]
    series = _series_from_metrics(rows, t0)
    impacted = _propagation_analysis(detail, rows, t0)
    findings = _findings(detail, impacted)
    load = detail.get("summary", {}).get("load", {})

    tp_chart = _svg_line_chart(
        [{"name": k, "points": v} for k, v in
         sorted(series["thread_pool"].items())],
        title="各服务工作线程池饱和度（0~1）", y_max=1.0)
    reject_chart = _svg_line_chart(
        [{"name": k, "points": v} for k, v in
         sorted(series["pool_reject"].items())],
        title="线程池累计拒绝数（满员快速失败）")
    brk_chart = _svg_line_chart(
        [{"name": k, "points": v} for k, v in
         sorted(series["breaker_state"].items())],
        title="熔断器状态（0=closed 0.5=half-open 1=open）", y_max=1.2)
    event_tl = _svg_timeline(detail.get("events", []), t0,
                             title="演练事件时间线")

    # 传播路径表
    if impacted:
        prop_rows = "".join(
            f"<tr><td>{i+1}</td><td>{html.escape(e['instance'])}</td>"
            f"<td>{e['first_error_at'] if e['first_error_at'] is not None else '—'}</td>"
            f"<td>{e['first_saturation_at'] if e['first_saturation_at'] is not None else '—'}</td>"
            f"<td>{e['first_pool_reject_at'] if e['first_pool_reject_at'] is not None else '—'}</td>"
            f"<td>{e['max_saturation']}</td>"
            f"<td>{e['breaker_opened_at'] if e['breaker_opened_at'] is not None else '—'}</td>"
            f"<td>{e['bulkhead_rejected']}</td></tr>"
            for i, e in enumerate(impacted))
        prop_table = (
            '<table><thead><tr><th>#</th><th>实例</th><th>首次错误(s)</th>'
            '<th>饱和≥70%(s)</th><th>池拒绝首现(s)</th><th>峰值饱和度</th>'
            '<th>熔断打开(s)</th><th>舱壁拒绝</th></tr></thead>'
            f'<tbody>{prop_rows}</tbody></table>')
        chain = " → ".join(e["instance"] for e in impacted)
    else:
        prop_table = '<p class="empty">没有服务受到明显影响，故障未扩散。</p>'
        chain = "未扩散"

    steady_rows = "".join(
        f"<tr class='{'bad' if c['breached'] else 'good'}'><td>{c['ts']-t0:.1f}</td>"
        f"<td>{html.escape(c['name'])}</td><td>{c['metric']}</td>"
        f"<td>{c['value']}</td><td>{c['threshold']}</td>"
        f"<td>{'偏离' if c['breached'] else '正常'}</td></tr>"
        for c in detail.get("steady_checks", [])[-40:]) or \
        '<p class="empty">本次演练未启用稳态巡检</p>'

    finding_html = "".join(f"<li>{html.escape(f)}</li>" for f in findings)
    status_badge = {"completed": "完成", "aborted": "自动中止",
                    "failed": "失败"}.get(detail["status"], detail["status"])

    body = f"""
    <h2>演练报告：{html.escape(detail['plan_name'])}
      <span class="badge mode-{detail['resilience_mode']}">
      韧性 {html.escape(detail['resilience_mode'].upper())}</span>
      <span class="badge">{status_badge}</span></h2>
    <p class="meta">run_id: {run_id} ｜ 开始 {time.strftime('%Y-%m-%d %H:%M:%S',
       time.localtime(t0))} ｜ 时长 {detail['finished_at']-t0:.1f}s</p>

    <h3>① 总览</h3>
    <div class="cards">
      <div class="card"><b>{load.get('total', 0)}</b><span>总请求</span></div>
      <div class="card"><b>{load.get('error_rate', 0)*100:.1f}%</b><span>错误率</span></div>
      <div class="card"><b>{load.get('latency_p95_ms', 0):.0f}ms</b><span>P95 延迟</span></div>
      <div class="card"><b>{load.get('fallback_hits', 0)}</b><span>降级命中</span></div>
      <div class="card"><b>{len(impacted)}</b><span>受影响实例</span></div>
    </div>
    <p><b>故障传播路径：</b><code>{html.escape(chain)}</code></p>

    <h3>② 事件时间线（注入 / 撤销 / 熔断翻转）</h3>
    {event_tl}

    <h3>③ 资源占用：故障如何一层层向上传</h3>
    {tp_chart}{reject_chart}{brk_chart}

    <h3>④ 故障传播分析</h3>
    {prop_table}

    <h3>⑤ 稳态指标巡检（最近 40 条）</h3>
    <table><thead><tr><th>相对时间(s)</th><th>假设</th><th>指标</th>
      <th>当前值</th><th>阈值</th><th>判定</th></tr></thead>
    <tbody>{steady_rows}</tbody></table>

    <h3>⑥ 系统表现与未守住项</h3>
    <ul class="findings">{finding_html}</ul>
    """
    return _wrap_html(f"演练报告 - {detail['plan_name']}", body, out_dir,
                      f"run-{run_id}.html")


# ---------------------------------------------------------------- 双跑对比
def build_comparison_report(store: Store, run_off: dict, run_on: dict,
                            out_dir: str) -> str:
    """需求 1/用户场景：同一计划 off vs on 并排对比。"""
    lo = run_off.get("summary", {}).get("load", {})
    ln = run_on.get("summary", {}).get("load", {})
    rows_off = store.get_series(run_off["run_id"])
    rows_on = store.get_series(run_on["run_id"])
    imp_off = _propagation_analysis(run_off, rows_off, run_off["started_at"])
    imp_on = _propagation_analysis(run_on, rows_on, run_on["started_at"])

    def row(label, key, suffix="", better="lower"):
        vo, vn = lo.get(key, 0), ln.get(key, 0)
        good = (vn < vo) if better == "lower" else (vn > vo)
        cls = "good" if good else "bad"
        return (f"<tr><td>{label}</td><td>{vo}{suffix}</td>"
                f"<td class='{cls}'>{vn}{suffix}</td></tr>")

    cmp_table = (
        "<table><thead><tr><th>指标</th><th>韧性 OFF（裸调）</th>"
        "<th>韧性 ON（熔断+隔离+降级）</th></tr></thead><tbody>"
        f"{row('总请求数', 'total')}"
        f"{row('错误率', 'error_rate', suffix='', better='lower')}"
        f"{row('P50 延迟(ms)', 'latency_p50_ms')}"
        f"{row('P95 延迟(ms)', 'latency_p95_ms')}"
        f"{row('P99 延迟(ms)', 'latency_p99_ms')}"
        f"{row('降级命中数', 'fallback_hits', better='higher')}"
        "</tbody></table>")
    # 错误率那行做百分比替换展示
    cmp_table = cmp_table.replace(
        f"<td>{lo.get('error_rate',0)}</td>",
        f"<td>{lo.get('error_rate',0)*100:.1f}%</td>").replace(
        f"<td class='{'good' if ln.get('error_rate',0)<lo.get('error_rate',1) else 'bad'}'>{ln.get('error_rate',0)}</td>",
        f"<td class='{'good' if ln.get('error_rate',0)<lo.get('error_rate',1) else 'bad'}'>{ln.get('error_rate',0)*100:.1f}%</td>")

    spread_off = " → ".join(e["instance"] for e in imp_off) or "无"
    spread_on = " → ".join(e["instance"] for e in imp_on) or "未扩散"

    # 网关线程池饱和度对比曲线（最能体现线程被占光 vs 被隔离保护）
    def gw_series(rows, t0):
        pts = []
        for r in rows:
            if r.get("service_name") or r.get("service", "").startswith("gw"):
                pts.append((r["ts"] - t0,
                            r.get("thread_pool", {}).get("saturation", 0)))
        return pts
    gw_off = gw_series(rows_off, run_off["started_at"])
    gw_on = gw_series(rows_on, run_on["started_at"])
    gw_chart = _svg_line_chart(
        [{"name": "网关线程池饱和度 OFF", "points": gw_off, "color": "#dc2626"},
         {"name": "网关线程池饱和度 ON", "points": gw_on, "color": "#16a34a"}],
        title="网关线程池饱和度对比：OFF vs ON", y_max=1.0)

    # 结论
    err_improve = (lo.get("error_rate", 0) - ln.get("error_rate", 0)) * 100
    p95_improve = lo.get("latency_p95_ms", 0) - ln.get("latency_p95_ms", 0)
    verdict = [
        f"错误率从 {lo.get('error_rate',0)*100:.1f}% 降到 "
        f"{ln.get('error_rate',0)*100:.1f}%（改善 {err_improve:.1f} 个百分点）",
        f"P95 延迟从 {lo.get('latency_p95_ms',0):.0f}ms 降到 "
        f"{ln.get('latency_p95_ms',0):.0f}ms（改善 {p95_improve:.0f}ms）",
        f"故障扩散实例数：{len(imp_off)} → {len(imp_on)}",
        "ON 模式下降级命中 "
        f"{ln.get('fallback_hits',0)} 次，说明熔断后请求走了兜底逻辑而不是堆死在线程上",
    ]

    body = f"""
    <h2>双跑对比：{html.escape(run_off['plan_name'])}</h2>
    <p class="meta">OFF run: {run_off['run_id']} ｜ ON run: {run_on['run_id']}</p>
    <h3>① 核心指标对比</h3>
    {cmp_table}
    <h3>② 网关线程池饱和度对比</h3>
    {gw_chart}
    <h3>③ 故障扩散范围</h3>
    <p><b>OFF：</b><code>{html.escape(spread_off)}</code></p>
    <p><b>ON：</b><code>{html.escape(spread_on)}</code></p>
    <h3>④ 结论：熔断/隔离到底管不管用</h3>
    <ul class="findings">{"".join(f"<li>{html.escape(v)}</li>" for v in verdict)}</ul>
    <p><a href="run-{run_off['run_id']}.html">查看 OFF 完整报告</a> ｜
       <a href="run-{run_on['run_id']}.html">查看 ON 完整报告</a></p>
    """
    return _wrap_html("双跑对比报告", body, out_dir,
                      f"compare-{run_off['run_id']}-{run_on['run_id']}.html")


# ---------------------------------------------------------------- HTML 骨架
def _wrap_html(title: str, body: str, out_dir: str, filename: str) -> str:
    os.makedirs(out_dir, exist_ok=True)
    doc = f"""<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="utf-8">
<title>{html.escape(title)}</title>
<style>
body {{ font-family: -apple-system, "PingFang SC", "Microsoft YaHei", sans-serif;
  margin: 0; background: #f1f5f9; color: #0f172a; }}
.container {{ max-width: 960px; margin: 0 auto; padding: 28px; }}
h2 {{ margin-top: 8px; }} h3 {{ margin-top: 32px; border-left: 4px solid #2563eb;
  padding-left: 10px; }}
.badge {{ font-size: 12px; padding: 3px 10px; border-radius: 10px;
  background: #e2e8f0; margin-left: 8px; vertical-align: middle; }}
.mode-on {{ background: #dcfce7; color: #166534; }}
.mode-off {{ background: #fee2e2; color: #991b1b; }}
.meta {{ color: #64748b; font-size: 13px; }}
.cards {{ display: flex; gap: 14px; flex-wrap: wrap; margin: 16px 0; }}
.card {{ background: #fff; border-radius: 10px; padding: 16px 22px; min-width: 110px;
  box-shadow: 0 1px 3px rgba(0,0,0,.08); text-align: center; }}
.card b {{ font-size: 26px; display: block; color: #1d4ed8; }}
.card span {{ color: #64748b; font-size: 13px; }}
table {{ border-collapse: collapse; width: 100%; background: #fff; font-size: 13px;
  border-radius: 8px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,.08); }}
th, td {{ border: 1px solid #e2e8f0; padding: 7px 10px; text-align: left; }}
th {{ background: #f8fafc; }}
tr.bad td {{ background: #fef2f2; }} tr.good td {{ background: #f0fdf4; }}
td.good {{ color: #166534; font-weight: 600; }}
td.bad {{ color: #991b1b; font-weight: 600; }}
.chart {{ width: 100%; background: #fff; border-radius: 8px; margin: 10px 0;
  box-shadow: 0 1px 3px rgba(0,0,0,.08); }}
.chart-title {{ font-weight: 600; font-size: 14px; }}
.grid {{ stroke: #e2e8f0; stroke-width: 1; }}
.axis {{ font-size: 11px; fill: #64748b; }}
.axis-line {{ stroke: #94a3b8; }}
.legend {{ font-size: 11px; fill: #334155; }}
.empty {{ color: #94a3b8; font-size: 13px; }}
.findings li {{ margin: 6px 0; }}
code {{ background: #e2e8f0; padding: 2px 6px; border-radius: 4px; font-size: 12px; }}
a {{ color: #2563eb; }}
</style></head><body><div class="container">{body}</div></body></html>"""
    path = os.path.join(out_dir, filename)
    with open(path, "w", encoding="utf-8") as f:
        f.write(doc)
    return path
