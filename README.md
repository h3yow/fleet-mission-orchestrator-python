# Fleet Mission Orchestrator

面向机器人车队的任务编排与调度服务。以本地持久化模拟机器人与任务状态，
保证任务的领取、执行、抢占、超时回收与重试在竞争和故障下可解释、可恢复。

## 架构

```
main.py                    入口（uvicorn main:app）
fleet_orchestrator/
  api.py                   FastAPI HTTP 层，拒绝原因 -> HTTP 状态码 + error 字段
  service.py               编排核心：租约、状态机、调度、回收、抢占（单锁原子变更）
  store.py                 持久化：原子快照(state.json) + 事件日志(events.jsonl)
  models.py                领域模型与状态机跳转表（extra="allow" 容忍未知字段）
  config.py                调度/超时参数，范围校验，非法整体拒绝
  errors.py                可区分拒绝原因（每个 reason 稳定且唯一）
tests/                     64 个测试，覆盖超时、竞争、抢占、幂等、依赖编排、恢复
```

## 任务状态机

```
PENDING ──> ASSIGNED ──> RUNNING ──> SUCCEEDED (终态)
  ^           │  │         │  │
  │           │  │         │  └──> FAILED (终态, 可 retry 回 PENDING)
  │           │  │         └─────> PREEMPTED ──> ASSIGNED (重分配, 进度保留)
  │           │  └──────────────> PREEMPTED
  └───────────┴── 回收(reclaim, attempts 未用尽)
ASSIGNED/RUNNING ──回收且次数用尽──> FAILED
任意非终态 ──> CANCELLED (终态)

依赖编排扩展：
WAITING_DEPS ──依赖全部成功──> PENDING
WAITING_DEPS ──前置失败/取消──> BLOCKED (记录 blocked_by)
BLOCKED ──前置恢复(重试/解阻)──> WAITING_DEPS
WAITING_DEPS / BLOCKED ──> CANCELLED
```

- 非法跳转（如 ASSIGNED→SUCCEEDED、PENDING→RUNNING）抛出
  `invalid_state_transition`。
- 状态上报必须携带当前租约凭证（`lease_id`），否则 `lease_not_held`；
  租约过期则 `lease_expired`。
- 不变量：任务处于 ASSIGNED/RUNNING 当且仅当存在对应租约，且持有机器人的
  `current_mission_id` 指向该任务。所有变更在单把可重入锁内原子完成。

## 租约与超时回收

- 领取任务即授予租约：`lease_id`（每次授予重新生成）+ `generation`
  （随任务持久化、单调递增，回收/重启后旧凭证仍可识别并拒绝）。
- 回收条件（`sweep()`，也可由外部定时触发）：持有者心跳超时
  （`now - last_heartbeat > heartbeat_timeout_s`）或租约过期
  （`now >= expires_at`）。
- 回收语义：`attempts < max_attempts` 时任务回到 PENDING 等待重分配，
  否则进入 FAILED 终态。回收与完成上报并发时由锁串行化，先到者生效，
  另一方收到 `lease_not_held`，不会双重持有也不会丢失。
- 续约：`renew` 延长 `renew_extension_s`，次数受 `max_lease_renewals` 限制，
  超限拒绝（`lease_expired`）。

## 调度规则

- 自动分配（`acquire` 不带 mission_id）：优先级降序，同优先级按入队序号
  FIFO，跳过能力不匹配的任务。
- 能力匹配：机器人能力集合必须覆盖任务 `required_capabilities`，
  否则 `capability_mismatch`；离线/心跳超时/已持有租约的机器人
  拒绝为 `robot_not_available`。
- 幂等下发：同一 `mission_id` 同内容重复下发返回首次结果（`created=false`）；
  内容不同拒绝 `mission_payload_conflict`（内容指纹 = 全部提交参数的
  规范化 SHA-256）。
- 抢占：目标须处于 ASSIGNED/RUNNING 且 `preemptible=true`；指定发起方时
  其优先级必须严格更高。被抢占任务进入 PREEMPTED 并保留 `progress`，
  可被重新领取续执行。失败原因可区分：`mission_not_preemptible` /
  `preempt_target_not_running` / `preempt_priority_too_low`。
- 积压控制：PENDING + PREEMPTED 数量达到 `max_backlog` 时新任务被拒绝
  （`backlog_full`, HTTP 429），不无界占用资源。

## 任务依赖编排

- 下发时可声明 `depends_on: [mission_id, ...]`，允许引用尚未下发的任务
  （先声明、后补建）。缺省为空，等价于无依赖（老记录行为不变）。
- 状态语义：**等待依赖**（`WAITING_DEPS`）与**等待调度**（`PENDING`）是
  两个可区分的状态。只有全部前置任务 `SUCCEEDED`，任务才从
  `WAITING_DEPS` 晋升为 `PENDING` 进入可领取范围；此前显式领取与自动
  调度都不会选中它（`mission_not_assignable`）。
- 查询：`GET /missions/{id}/dependencies` 返回每个前置任务的状态
  （未出现的前置显示 `MISSING`）、是否已满足，以及任务当前被哪个前置
  阻断（`blocked_by`）。
- 成环拒绝：依赖关系直接或间接绕回自身（含自己依赖自己）在下发时
  被拒绝，reason 为 `dependency_cycle`（HTTP 409），不影响其它任务，
  也不留下半截记录。尚未下发的前置没有出边，不可能参与成环。
- 阻断传播：前置任务进入 `FAILED` / `CANCELLED`（或自身被阻断）时，
  依赖它的任务进入 `BLOCKED` 并记录 `blocked_by`（阻断沿依赖链传递）。
  `BLOCKED` 任务不可领取、不可抢占、不持有租约，回收逻辑不会误把它
  当作在执行。
- 阻断恢复：前置经重试回到等待并最终成功时，被阻断的任务按同样规则
  恢复（`BLOCKED -> WAITING_DEPS -> PENDING`）。所有依赖状态刷新在
  同一把锁内随触发操作同事务持久化，无半更新、无自相矛盾的可见状态。
- 容量约束：解阻晋升 `PENDING` 与下发共用同一 `max_backlog` 上限。
  一个前置成功同时解阻一批任务时，按入队序号逐个晋升，槽位不足的任务
  留在 `WAITING_DEPS` 排队（下次释放槽位时再评估），不得绕开上限
  无界堆积。
- 幂等：`depends_on` 参与内容指纹。同标识同依赖重复下发幂等返回首次
  结果；依赖不同按 `mission_payload_conflict` 拒绝。
- 持久化：`depends_on` / `blocked_by` / 新状态均随任务记录持久化，
  重启恢复与事件重放后与故障前一致，不会凭空多出可领取任务。

## 配置项与上限

| 参数 | 默认 | 范围 |
|---|---|---|
| `lease_duration_s` | 30 | [1, 3600] |
| `heartbeat_timeout_s` | 15 | [1, 3600] |
| `max_backlog` | 1000 | [1, 100000] |
| `max_attempts_default` | 3 | [1, 100] |
| `max_lease_renewals` | 10 | [0, 1000] |
| `renew_extension_s` | 30 | [1, 3600] |

越界或未知键抛出 `config_out_of_range`（HTTP 422），非法更新整体拒绝，
不产生半更新。运行时可通过 `POST /admin/config` 更新。

## 持久化与故障恢复

- 每次变更先追加事件日志（`events.jsonl`，fsync），再原子替换快照
  （临时文件 + `os.replace`），杜绝半更新状态。
- 重启恢复优先读快照；快照缺失/损坏时回退到事件重放（从空状态折叠全部
  事件）。事件日志尾部半行（崩溃截断）被安全忽略。
- 重放结果与快照一致由 `tests/test_recovery.py` 验证；回收过的租约不会在
  重启后复活，不产生幽灵任务或重复分配。

## 数据格式演进

- 所有持久化模型 `extra="allow"`：含未知字段的记录解析不报错、字段原样
  保留并随重新持久化写回（不静默丢弃），但不参与调度判定。
- 新增字段一律带默认值，旧记录缺省语义确定（见
  `tests/test_evolution.py`）。

## HTTP API 摘要

| 方法/路径 | 说明 |
|---|---|
| `POST /robots/register` `/robots/heartbeat` `/robots/deregister` | 机器人生命周期 |
| `POST /missions` | 幂等下发 |
| `POST /missions/acquire` | 领取（可指定 mission_id 或自动调度） |
| `POST /missions/{id}/start` `/progress` `/complete` `/fail` `/renew` | 租约内操作 |
| `POST /missions/{id}/retry` `/cancel` `/preempt` | 重试/取消/抢占 |
| `POST /admin/sweep` `/admin/config` | 超时回收 / 配置 |
| `GET /missions` `/robots` `/missions/{id}/lease` | 查询 |
| `GET /missions/{id}/dependencies` | 依赖编排视图（前置状态/阻断来源） |

所有拒绝返回 `{"error": <reason>, "detail": <说明>}`，reason 取值见
`fleet_orchestrator/errors.py`。

## 验证方法

```bash
uv run pytest tests/ -q          # 全部 64 个测试
uv run pytest tests/ -q -s       # 打印每个用例的输入与判定依据 ([CASE] 日志)
uv run uvicorn main:app          # 启动服务，文档见 /docs
```

测试主题与文件对应：

| 主题 | 文件 |
|---|---|
| 心跳超时、租约过期、回收与重试 | `tests/test_lease_timeout.py` |
| 并发竞争（领取、完成 vs 回收、重复下发） | `tests/test_concurrency.py` |
| 抢占与优先级 | `tests/test_preemption.py` |
| 幂等下发与积压上限 | `tests/test_mission_submission.py` |
| 注册、能力与可用性 | `tests/test_robot_registration.py` |
| 非法跳转与非持有者上报 | `tests/test_state_machine.py` |
| 重启恢复与事件重放 | `tests/test_recovery.py` |
| 依赖编排（等待/阻断/成环/容量/重放） | `tests/test_dependencies.py` |
| 配置校验 | `tests/test_config.py` |
| 数据格式演进 | `tests/test_evolution.py` |
| HTTP 拒绝原因映射 | `tests/test_api.py` |
