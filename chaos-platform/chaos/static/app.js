/* 故障演练平台控制台前端 */
const $ = (s) => document.querySelector(s);
const $$ = (s) => document.querySelectorAll(s);

async function api(path, method, body) {
  const opt = { method: method || "GET" };
  if (body !== undefined) {
    opt.headers = { "Content-Type": "application/json" };
    opt.body = JSON.stringify(body);
  }
  const r = await fetch(path, opt);
  return r.json();
}
const pct = (x) => (x * 100).toFixed(1) + "%";
const esc = (s) => String(s).replace(/[&<>"]/g, (c) =>
  ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));

/* ---------------- tabs ---------------- */
$$("nav button").forEach((b) => b.addEventListener("click", () => {
  $$("nav button").forEach((x) => x.classList.remove("active"));
  b.classList.add("active");
  $$(".tab").forEach((t) => t.classList.remove("active"));
  $("#tab-" + b.dataset.tab).classList.add("active");
  refreshTab(b.dataset.tab);
}));

function refreshTab(tab) {
  if (tab === "topology") loadTopology();
  if (tab === "plans") { loadPlans(); loadRuns(); }
  if (tab === "rules") loadRules();
  if (tab === "steady") { loadAdvisor(); }
  if (tab === "reports") loadReportList();
}

/* ---------------- status bar ---------------- */
async function pollLive() {
  try {
    const d = await api("/api/live");
    const rb = $("#resilience-badge");
    rb.textContent = "韧性模式: " + (d.resilience ? "开" : "关");
    rb.className = "badge " + (d.resilience ? "on" : "off");
    const runb = $("#run-badge");
    if (d.run) {
      runb.textContent = `演练中: ${d.run.plan} t=${d.run.t}s` +
        (d.run.aborted ? " (已自动中止)" : "");
      runb.className = "badge run";
    } else {
      runb.textContent = "空闲";
      runb.className = "badge";
    }
    const c = d.client;
    $("#client-stats").textContent =
      `客户端: ${c.rps}rps 错误率 ${pct(c.error_rate)} p95 ${c.p95}ms 降级 ${c.degraded}`;
    renderSteadyLive(d.run);
  } catch (e) { /* console not ready */ }
}
setInterval(pollLive, 1000);

/* ---------------- topology ---------------- */
async function loadTopology() {
  const d = await api("/api/topology");
  $("#topo-services").innerHTML = d.services.map((s) => {
    const sat = s.pool.active / s.pool.size;
    const err = s.requests ? s.errors / s.requests : 0;
    const cls = err > 0.05 || sat > 0.9 ? "bad" : (sat > 0.5 || s.pool.queued > 0 ? "warn" : "");
    return `<div class="card ${cls}">
      <h4>${esc(s.name)} <span class="muted">:${s.port}</span></h4>
      <div class="row"><span>线程池</span><span>${s.pool.active}/${s.pool.size} 排队${s.pool.queued} 拒绝${s.pool.rejected}</span></div>
      <div class="bar ${sat > 0.8 ? "hot" : ""}"><div style="width:${(sat * 100).toFixed(0)}%"></div></div>
      <div class="row"><span>请求</span><span>${s.requests} (错 ${s.errors})</span></div>
      <div class="row"><span>p95</span><span>${s.lat.p95}ms</span></div>
    </div>`;
  }).join("");
  $("#topo-edges").innerHTML = `<tr><th>调用边</th><th>熔断器</th><th>调用/错误/降级</th>
    <th>p95</th><th>舱壁</th><th>连接池</th><th>超时配置</th></tr>` + d.edges.map((e) =>
    `<tr><td>${esc(e.from)} → ${esc(e.to)}</td>
     <td class="state-${e.breaker}">${e.breaker}</td>
     <td>${e.calls} / ${e.errors} / ${e.fallbacks}</td>
     <td>${e.lat.p95}ms</td>
     <td>${e.bulkhead_in_use}/${e.bulkhead_size}</td>
     <td>${e.conn.in_use}/${e.conn.size}</td>
     <td>${e.timeout_ms}ms</td></tr>`).join("");
}
setInterval(() => { if ($("#tab-topology").classList.contains("active")) loadTopology(); }, 2000);

/* ---------------- plans ---------------- */
let selectedPlan = null;
async function loadPlans() {
  const plans = await api("/api/plans");
  $("#plan-list").innerHTML = plans.map((p) =>
    `<li data-name="${esc(p.name)}" class="${p.name === selectedPlan ? "sel" : ""}">
       ${esc(p.name)}<small>${esc(p.description)}</small></li>`).join("");
  $$("#plan-list li").forEach((li) => li.addEventListener("click", async () => {
    selectedPlan = li.dataset.name;
    const p = await api("/api/plans/" + selectedPlan);
    $("#plan-json").value = JSON.stringify(p, null, 2);
    loadPlans();
  }));
}
$("#plan-save").addEventListener("click", async () => {
  try {
    const plan = JSON.parse($("#plan-json").value);
    await api("/api/plans", "POST", plan);
    selectedPlan = plan.name;
    loadPlans();
    alert("已保存模板: " + plan.name);
  } catch (e) { alert("JSON 格式错误: " + e); }
});
async function runPlan(resilience) {
  if (!selectedPlan) return alert("先选择一个计划");
  try {
    const r = await api(`/api/plans/${selectedPlan}/run`, "POST", { resilience });
    alert("演练已启动: " + r.run_id);
  } catch (e) { alert(e.error || e); }
}
$("#plan-run-on").addEventListener("click", () => runPlan(true));
$("#plan-run-off").addEventListener("click", () => runPlan(false));
$("#drill-stop").addEventListener("click", () => api("/api/drill/stop", "POST", {}));

async function loadRuns() {
  const runs = await api("/api/runs");
  $("#run-list").innerHTML = `<tr><th></th><th>ID</th><th>计划</th><th>韧性</th>
    <th>开始时间</th><th>结果</th><th>错误率</th><th>p95</th></tr>` + runs.map((r) =>
    `<tr><td><input type="checkbox" class="cmp" value="${esc(r.id)}"></td>
     <td>${esc(r.id)}</td><td>${esc(r.plan)}</td>
     <td>${r.resilience ? "开" : "关"}</td><td>${esc(r.started_at || "")}</td>
     <td class="${r.aborted ? "fail" : "ok"}">${r.aborted ? "自动中止" : "完成"}</td>
     <td>${r.client ? pct(r.client.error_rate) : "-"}</td>
     <td>${r.client ? r.client.p95 + "ms" : "-"}</td></tr>`).join("");
}
$("#compare-btn").addEventListener("click", async () => {
  const ids = [...$$(".cmp:checked")].map((c) => c.value);
  if (ids.length !== 2) return alert("勾选恰好两次演练");
  const d = await api(`/api/compare?a=${ids[0]}&b=${ids[1]}`);
  $("#compare-view").innerHTML = `<h3>对比: ${esc(d.a.id)} vs ${esc(d.b.id)}</h3>
    <table class="tbl"><tr><th>指标</th><th>${esc(d.a.id)}</th><th>${esc(d.b.id)}</th></tr>
    ${d.rows.map((r) => `<tr><td>${esc(r.metric)}</td><td>${esc(r.a)}</td><td>${esc(r.b)}</td></tr>`).join("")}
    </table>
    <h3>各服务影响对比</h3>
    <table class="tbl"><tr><th>服务</th><th>A 错误率</th><th>A 线程池峰值</th><th>A 拒绝</th>
    <th>B 错误率</th><th>B 线程池峰值</th><th>B 拒绝</th></tr>
    ${Object.entries(d.per_service).map(([s, v]) =>
      `<tr><td>${esc(s)}</td><td>${pct(v.a.error_rate)}</td><td>${pct(v.a.pool_saturation)}</td>
       <td>${v.a.rejected}</td><td>${pct(v.b.error_rate)}</td><td>${pct(v.b.pool_saturation)}</td>
       <td>${v.b.rejected}</td></tr>`).join("")}
    </table>`;
});

/* ---------------- rules ---------------- */
async function loadRules() {
  const topo = await api("/api/topology");
  $("#rf-target").innerHTML = topo.services.map((s) =>
    `<option>${esc(s.name)}</option>`).join("");
  const rules = await api("/api/rules");
  $("#rule-list").innerHTML = `<tr><th>ID</th><th>目标</th><th>类型</th><th>参数</th>
    <th>匹配条件</th><th>状态</th><th>操作</th></tr>` + rules.map((r) =>
    `<tr><td>${esc(r.id)}</td><td>${esc(r.target)}</td><td>${esc(r.type)}</td>
     <td class="muted">${esc(JSON.stringify({ latency_ms: r.latency_ms, error_status: r.error_status, error_rate: r.error_rate }))}</td>
     <td class="muted">${esc(JSON.stringify(r.match || {}))}</td>
     <td class="${r.enabled ? "fail" : "ok"}">${r.enabled ? "注入中" : "停用"}</td>
     <td><button data-toggle="${esc(r.id)}">${r.enabled ? "停用" : "启用"}</button>
         <button data-del="${esc(r.id)}">删除</button></td></tr>`).join("");
  $$("#rule-list [data-toggle]").forEach((b) => b.addEventListener("click", async () => {
    await api(`/api/rules/${b.dataset.toggle}/toggle`, "POST", {}); loadRules();
  }));
  $$("#rule-list [data-del]").forEach((b) => b.addEventListener("click", async () => {
    await api(`/api/rules/${b.dataset.del}`, "DELETE"); loadRules();
  }));
}
$("#rule-add").addEventListener("click", async () => {
  const tag = $("#rf-tag").value.trim();
  await api("/api/rules", "POST", {
    id: $("#rf-id").value.trim(),
    target: $("#rf-target").value,
    type: $("#rf-type").value,
    latency_ms: +$("#rf-latency").value,
    error_status: +$("#rf-status").value,
    error_rate: +$("#rf-errrate").value,
    enabled: true,
    match: Object.assign(tag ? { tag } : {}, { percent: +$("#rf-percent").value }),
  });
  loadRules();
});

/* ---------------- steady state & advisor ---------------- */
function renderSteadyLive(run) {
  const el = $("#steady-live");
  if (!el) return;
  if (!run) { el.innerHTML = '<p class="muted">当前没有演练。启动演练后这里实时显示稳态检查结果。</p>'; return; }
  const defs = (run.steady_state || []).map((c) =>
    `<span class="badge">${esc(c.metric)} ${esc(c.op)} ${c.value}</span>`).join("");
  const checks = (run.checks || []).slice(-8).reverse().map((c) =>
    `<div class="${c.ok ? "ok" : "fail"}">t=${c.t}s ${c.ok ? "✓ 稳态保持" : "✗ " + esc(c.failed.join("; "))}</div>`).join("");
  el.innerHTML = `<h3>当前演练 ${esc(run.id)} 的稳态定义</h3>${defs}<h3>最近检查</h3>${checks || '<p class="muted">暂无</p>'}`;
}
async function loadAdvisor() {
  const rows = await api("/api/advisor");
  $("#advisor-table").innerHTML = `<tr><th>调用边</th><th>样本</th><th>p50</th><th>p95</th>
    <th>p99</th><th>建议超时</th><th>当前配置</th></tr>` + rows.map((r) =>
    `<tr><td>${esc(r.edge)}</td><td>${r.samples}</td><td>${r.p50}ms</td><td>${r.p95}ms</td>
     <td>${r.p99}ms</td><td class="ok">${r.recommended_ms}ms</td>
     <td>${r.current_ms ? r.current_ms + "ms" : '<span class="muted">未配置</span>'}</td></tr>`).join("");
}
$("#advisor-refresh").addEventListener("click", loadAdvisor);
$("#advisor-apply").addEventListener("click", async () => {
  const r = await api("/api/advisor/apply", "POST", {});
  alert(`已应用 ${r.applied.length} 条超时建议(韧性模式下生效)`);
  loadAdvisor();
});

/* ---------------- reports ---------------- */
async function loadReportList() {
  const runs = await api("/api/runs");
  $("#report-select").innerHTML = `<option value="">选择一次演练…</option>` +
    runs.map((r) => `<option value="${esc(r.id)}">${esc(r.id)} (${r.resilience ? "开韧性" : "关韧性"})</option>`).join("");
}
$("#report-select").addEventListener("change", async (e) => {
  if (!e.target.value) return;
  const run = await api("/api/runs/" + e.target.value);
  renderReport(run);
});

function drawChart(canvas, series, yLabel) {
  const W = canvas.width = canvas.offsetWidth || 860;
  const H = canvas.height = 180;
  const ctx = canvas.getContext("2d");
  ctx.clearRect(0, 0, W, H);
  const all = series.flatMap((s) => s.points);
  if (!all.length) return;
  const t0 = Math.min(...all.map((p) => p[0])), t1 = Math.max(...all.map((p) => p[0]));
  const vmax = Math.max(...all.map((p) => p[1]), 0.001) * 1.1;
  const X = (t) => 40 + ((t - t0) / Math.max(t1 - t0, 0.1)) * (W - 60);
  const Y = (v) => H - 24 - (v / vmax) * (H - 44);
  ctx.strokeStyle = "#262f45"; ctx.fillStyle = "#7f8ba3"; ctx.font = "10px sans-serif";
  for (let i = 0; i <= 4; i++) {
    const v = (vmax / 4) * i, y = Y(v);
    ctx.beginPath(); ctx.moveTo(40, y); ctx.lineTo(W - 20, y); ctx.stroke();
    ctx.fillText(v.toFixed(vmax < 5 ? 2 : 0), 4, y + 3);
  }
  for (let i = 0; i <= 6; i++) {
    const t = t0 + ((t1 - t0) / 6) * i;
    ctx.fillText(t.toFixed(0) + "s", X(t) - 6, H - 8);
  }
  ctx.fillText(yLabel || "", 4, 12);
  series.forEach((s) => {
    ctx.strokeStyle = s.color; ctx.lineWidth = 1.5; ctx.beginPath();
    s.points.forEach((p, i) => { const x = X(p[0]), y = Y(p[1]);
      i ? ctx.lineTo(x, y) : ctx.moveTo(x, y); });
    ctx.stroke();
    ctx.fillStyle = s.color; ctx.fillText(s.name, W - 20 - 90 * series.indexOf(s) - 60, 12);
  });
}

function renderReport(run) {
  const s = run.summary || {};
  const c = s.client || {};
  const v = $("#report-view");
  const verdicts = (s.verdicts || []).map((t) => {
    const cls = /未守住|失守|拖垮/.test(t) ? "bad" : (/保持|生效|限制/.test(t) ? "good" : "");
    return `<div class="verdict ${cls}">${esc(t)}</div>`;
  }).join("");
  const perSvc = Object.entries(s.per_service || {}).map(([name, p]) =>
    `<tr class="${p.is_target ? "muted" : ""}">
     <td>${esc(name)}${p.is_target ? " (注入点)" : ""}</td>
     <td>${p.requests}</td><td>${pct(p.error_rate)}</td>
     <td>${p.max_pool_active}/${p.pool_size} (排队峰值${p.max_pool_queued})</td>
     <td>${p.rejected}</td><td>${p.p95_max}ms</td>
     <td>${p.first_impact_t === null ? "-" : "t=" + p.first_impact_t + "s"}</td></tr>`).join("");
  const events = (run.events || []).map((e) =>
    `<div class="muted">t=${e.t}s [${esc(e.type)}] ${esc(e.msg)}</div>`).join("");
  v.innerHTML = `
    <h3>结论</h3>${verdicts}
    <h3>客户端表现</h3>
    <table class="tbl"><tr><th>总请求</th><th>错误率</th><th>p50</th><th>p95</th><th>p99</th><th>降级</th></tr>
    <tr><td>${c.count || 0}</td><td>${pct(c.error_rate || 0)}</td><td>${c.p50 || 0}ms</td>
    <td>${c.p95 || 0}ms</td><td>${c.p99 || 0}ms</td><td>${c.degraded || 0}</td></tr></table>
    <h3>客户端错误率 / p95 随时间</h3><canvas class="chart" id="ch-client"></canvas>
    <h3>各服务线程池占用(故障如何一层层往上传)</h3><canvas class="chart" id="ch-pool"></canvas>
    <h3>故障传播(首次受影响时间)</h3>
    <p>${(s.propagation || []).length
        ? (s.targets || []).join("/") + " → " + s.propagation.map((p) => `${esc(p.service)}(t=${p.t}s)`).join(" → ")
        : "未发生扩散"}</p>
    <h3>各服务明细</h3>
    <table class="tbl"><tr><th>服务</th><th>请求</th><th>错误率</th><th>线程池峰值</th>
    <th>拒绝</th><th>p95峰值</th><th>首次受影响</th></tr>${perSvc}</table>
    <h3>事件时间线</h3>${events}`;
  const series = run.series || [];
  drawChart($("#ch-client"), [
    { name: "错误率", color: "#ff5d6c", points: series.map((e) => [e.t, e.client.error_rate]) },
    { name: "p95/1000ms", color: "#ffd479", points: series.map((e) => [e.t, e.client.p95 / 1000]) },
  ], "错误率");
  const colors = ["#4f8cff", "#6fe3a1", "#ffd479", "#ff9aa8", "#b48cff", "#5cd6d6",
                  "#e3a16f", "#9fe36f", "#e36fd8", "#8fa0c0", "#ff5d6c"];
  drawChart($("#ch-pool"), Object.keys(s.per_service || {}).map((name, i) => ({
    name, color: colors[i % colors.length],
    points: series.map((e) => {
      const sv = e.services[name];
      return [e.t, sv ? sv.pool.active / sv.pool.size : 0];
    }),
  })), "线程池占用率");
}

/* ---------------- init ---------------- */
loadPlans();
pollLive();
loadTopology();
