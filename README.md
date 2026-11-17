# ChaosLab 故障演练平台

面向在线挂号缴费系统的故障演练（Chaos Engineering）平台。纯 Python 标准库实现，零第三方依赖。

## 背景

挂号系统曾因短信服务变慢，所有服务线程同步阻塞等待，整个系统挂掉半小时。加装熔断器后从未验证过是否真的有效。本平台用来：

1. 在服务调用链前挂代理，**注入延迟 / 错误 / 断连故障**；
2. 提供**熔断、舱壁隔离、超时、降级**一整套韧性组件；
3. 通过控制台编排**演练计划**，演练前定义**稳态指标**，偏离自动中止并撤销故障；
4. 记录每个服务**线程池 / 连接池占用**，报告中还原故障如何一层层向上传播；
5. 同一计划在**开 / 关熔断隔离**两种模式下复跑，对比错误率、延迟和故障扩散范围。

## 一分钟跑起来

```bash
# 1. 启动靶场（10 个服务、短信双实例共 12 个实例 + 1 个代理，13 个独立进程）
python -m labctl up

# 2. 跑头条演练：SMS 延迟故障，关熔断 vs 开熔断，自动对比
python -m labctl experiment

# 3. 打开控制台
python -m labctl console      # http://127.0.0.1:8300

# 4. 查看状态 / 收尾
python -m labctl status
python -m labctl down
```

演练结束后报告写到 `./.chaoslab/reports/`，控制台里可直接点开，也能用浏览器打开本地文件。

## 实测效果（短信变慢事故复现）

同一演练计划（向短信服务注入 3s 延迟），OFF / ON 双跑对照：

| 指标 | 韧性 OFF（裸调） | 韧性 ON（熔断+隔离+降级） |
|---|---|---|
| 入口错误率 | **47.3%** | **12.2%** |
| 入口 P95 延迟 | 3224ms | 1309ms |
| 降级命中 | 0（无兜底，错误硬扛） | 1624（快速失败后兜底） |
| 短信错误故障（另一计划） | 错误率 58.4% | **错误率 0.0%**，1127 次降级 |

报告还会画出网关线程池饱和度曲线、熔断器 open→half-open→closed 翻转时间线，
以及按"首次受影响时刻"排序的故障传播链。

## 八项能力与落点

| # | 需求 | 实现位置 |
|---|---|---|
| 1 | 计划存模板、一键复跑、两次结果对比 | `model.py` / `store.py` / `report.py` 双跑对比 |
| 2 | 稳态指标持续巡检，偏离自动中止并撤销全部故障 | `orchestrator.py` `SteadyStateWatcher` |
| 3 | 爆炸半径：请求标记 / 指定实例 / 概率 | `proxy.py` `RuleEngine.match` |
| 4 | 熔断按错误率+慢调用比例触发，半开探测恢复 | `resilience.py` `CircuitBreaker` |
| 5 | 超时按历史延迟分位数给建议 | `resilience.py` `Histogram.suggest_timeout` |
| 6 | 记录线程池/连接池占用，画出故障向上传播 | `http_tools.py` 指标 + `report.py` 传播分析 |
| 7 | 多步骤按时间线注入/恢复 | `model.py` `PlanStep` + `orchestrator.py` 步骤循环 |
| 8 | 报告给出每个故障下的表现和未守住项 | `report.py` `_findings` 自动结论 |

## 控制台模块（http://127.0.0.1:8300）

- **服务拓扑**：实时节点状态、线程池饱和度、熔断器状态，可一键切韧性模式/紧急撤销故障
- **演练计划**：模板列表、执行实验、JSON 在线编辑另存
- **注入规则**：代理当前规则查看、手动下发/撤销，三重爆炸半径配置
- **稳态指标**：各计划稳态假设 + 最近 run 的巡检记录
- **演练报告**：run 列表、单跑报告、双跑对比报告

## 目录结构

```
chaoslab/            # 平台核心
  resilience.py      # 熔断器 / 舱壁 / 超时 / 降级 / 分位数建议
  proxy.py           # 故障注入代理（含爆炸半径控制）
  http_tools.py      # 池化服务框架 / 下游调用器 / 指标
  orchestrator.py    # 演练编排、稳态巡检、自动中止、轮次间恢复
  loadgen.py         # 负载生成器
  report.py          # 自包含 HTML/SVG 报告与双跑对比
  console_app.py     # Web 控制台后端 + JSON API
  web/index.html     # 控制台前端（原生 JS，无构建）
  model.py           # 计划/规则/稳态/运行记录数据模型
  store.py           # SQLite 持久化
  plans.py           # 预置演练计划模板
labctl/              # 挂号系统靶场（10 服务同步调用链）+ 命令行
  topology.py        # 拓扑、端口、调用边
  services.py        # 10 个服务的业务路由与降级逻辑
  service_runner.py  # 服务进程入口
  proxy_runner.py    # 代理进程入口
  lab_manager.py     # 靶场生命周期（拉起/健康检查/关停）
  lab_cli.py         # up/down/experiment/console 命令
tests/               # 单元测试（26 个，标准库 unittest）
```

## 靶场调用链

```
gateway 网关
  ├─ registration 挂号
  │    ├─ patient 患者信息 ── profile 档案
  │    ├─ schedule 号源排班 ── inventory 号源库存
  │    └─ payment 缴费 ── billing 计费
  └─ notification 通知
       ├─ sms 短信（2 实例，故障演练主角）
       └─ push 推送
```

所有服务间调用都经过 `:8200` 故障注入代理；每个下游依赖独占连接池舱壁、
熔断器和超时配置，韧性模式可在线 on/off 切换。

## 测试

```bash
python -m unittest discover -s tests -t . -v
```

覆盖：熔断器三态转换与双触发条件、舱壁拒绝、超时释放、降级、分位数建议、
代理三类故障注入与三重爆炸半径、存储层序列化往返。
