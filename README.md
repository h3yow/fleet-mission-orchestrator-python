# Fleet Mission Orchestrator

面向机器人车队的任务编排与调度服务。以本地持久化模拟机器人与任务状态，
保证任务的领取、执行、抢占、超时回收与重试在竞争和故障下可解释、可恢复。
在单机任务之上提供**任务间依赖编排**（如：导航到位 → 举升 → 返航），
后一步必须等前置全部成功完成才进入可领取范围。

## 架构

```
main.py                    入口（uvicorn main:app）
fleet_orchestrator/
  api.py                   FastAPI HTTP 层，拒绝原因 -> HTTP 状态码 + error 字段
  service.py               编排核心：依赖编排、租约、状态机、调度、回收、抢占（单锁原子变更）
  store.py                 持久化：原子快照(state.json) + 事件日志(events.jsonl)
  models.py                领域模型与状态机跳转表（extra="allow" 容忍未知字段）
  config.py                调度/超时参数，范围校验，非法整体拒绝
  errors.py                可区分拒绝原因（每个 reason 稳定且唯一）
tests/                     65 个测试，覆盖依赖编排、超时、竞争、抢占、幂等、非法跳转、恢复
```

## 任务状态机

```
PENDING ──> ASSIGNED ──> RUNNING ──> SUCCEEDED (终态)
  ^ ^         │  │         │  │
  │ │         │  │         │  └──> FAILED (终态, 可 retry 回 PENDING)
  │ │         │  │         └─────> PREEMPTED ──> ASSIGNED (重分配, 进度保留)
  │ │         │  └──────────────> PREEMPTED
  │ └─────────┴── 回收(reclaim, attempts 未用尽) / 依赖满足(WAITING/BLOCKED -> PENDING)
  │
WAITING ──前置全部成功──> PENDING；前置失败/取消/被阻断──> BLOCKED
BLOCKED ──前置重试(回到等待)──> WAITING；前置重试后成功──> PENDING
WAITING/BLOCKED ──> CANCELLED (终态)
ASSIGNED/RUNNING ──回收且次数用尽──> FAILED
任意非终态 ──> CANCELLED (终态)
```

- 新增两个**非终态**：`WAITING`（仍有前置未成功，不可被领取）与
  `BLOCKED`（前置失败/取消导致不可执行，前置重试后可恢复）。
  `BLOCKED` 不是终态：它不允许领取/抢占，也不被回收逻辑当作在执行。
- 非法跳转（如 ASSIGNED→SUCCEEDED、PENDING→RUNNING）抛出
  `invalid_state_transition`。
- 状态上报必须携带当前租约凭证（`lease_id`），否则 `lease_not_held`；
  租约过期则 `lease_expired`。
- 不变量：任务处于 ASSIGNED/RUNNING 当且仅当存在对应租约，且持有机器人的
  `current_mission_id` 指向该任务。所有变更在单把可重入锁内原子完成。

## 依赖编排

- **声明方式**：`POST /missions` 增加 `dependencies: [mission_id, ...]`。
  允许引用**尚未下发**的任务标识（先声明、后补建）；依赖列表去重保序，
  空标识被忽略。任务只有在所有前置都 `SUCCEEDED` 后才进入 `PENDING`；
  前置处于 PENDING/WAITING/ASSIGNED/RUNNING/PREEMPTED 或尚未补建时保持
  `WAITING`，不会被自动调度派出，也不能被显式领取。
- **等待区分**：`GET /missions/{id}/dependencies` 返回
  `wait_kind`：`schedule`（无依赖或依赖已满足，等待调度）、
  `dependency`（等待前置，逐个给出前置状态，未补建显示 `MISSING`，
  并以 `unsatisfied` 列出卡点）、`blocked`（被拖垮，给出
  `blocked_by`/`blocked_reason`）、`none`（执行中/终态）。
- **阻断传播**：前置进入终态 `FAILED`（不再重试）或 `CANCELLED` 时，
  直接及间接依赖方级联进入 `BLOCKED`，`blocked_by` 沿阻断链归因到根因，
  `blocked_reason` 为 `dep_failed` / `dep_cancelled`。被阻断任务不允许领取，
  不能被抢占（`preempt_target_not_running`），也没有租约，因此
  `sweep()` 回收与抢占逻辑都不会把它误当成在执行的任务。
- **恢复**：前置随后 `retry` 回到 PENDING 时，曾被它阻断的依赖方按同一套
  规则恢复为 `WAITING`；前置重试并最终成功后恢复为 `PENDING`。整批解阻在
  单把锁内级联重算、随同一批事件一次落盘，外部只能看到 WAITING→PENDING
  之类的一致跳转，看不到半更新或自相矛盾的中间状态。
- **成环拒绝**：自依赖或间接绕回在**下发时**即以独立原因
  `dependency_cycle`（HTTP 409）拒绝，本次提交不落任何记录，其它任务不受
  连带影响。检测在“已有无环图 + 本次新边”上进行。
- **容量约束**：所有重新进入可领取范围的路径（补建前置、前置成功、重试
  成功）都受同一套 `max_backlog` 排队上限约束——`PENDING/PREEMPTED/
  WAITING/BLOCKED` 共用同一个积压池，等待与阻断中的任务同样占槽。因此一批
  任务同时解阻只是池内状态迁移，池总量不增加，不可能绕开上限；池子打满时
  新下发（含无依赖任务）按既有策略拒绝为 `backlog_full`（HTTP 429），不
  无界堆积。
- **幂等一致性**：依赖声明纳入内容指纹。同一 id 相同依赖重复下发仍幂等
  返回首次结果（`created=false`）；依赖不同按既有语义拒绝
  `mission_payload_conflict`，且拒绝不篡改原依赖。

### 与既有机制的关系

| 既有机制 | 关系 |
|---|---|
| 状态机 | WAITING/BLOCKED 为新增非终态，跳转见上表；BLOCKED 不可直接领取 |
| 租约 | WAITING/BLOCKED 从不持有租约；解阻到 PENDING 后才走既有授予流程 |
| 抢占 | 只能作用于 ASSIGNED/RUNNING；等待/阻断任务抢占被明确拒绝 |
| 超时回收 | sweep 只处理有租约者，绝不触碰无租约的 WAITING/BLOCKED |
| 容量上限 | 四类排队态共用 `max_backlog`，解阻不产生额外积压、不绕开上限 |
| 持久化恢复 | 依赖、等待/阻断/解阻状态全部随 mission 记录落盘；重启先对账再服务，重放一致 |

### 兼容范围（老记录与缺省语义）

- `dependencies`、`blocked_by`、`blocked_reason` 均为带默认值的新字段；
  没有这些字段的老记录解析时缺省为“无依赖、无阻断”，任务直接是 PENDING，
  行为与本轮改动前完全一致（见 `tests/test_evolution.py` 与
  `test_dependencies.py::test_legacy_record_without_dependencies_defaults_no_deps`）。
- 重启或按事件重放后：依赖关系、WAITING/BLOCKED 以及已解阻为 PENDING 的状态
  与故障前一致；恢复时仅重算 WAITING/BLOCKED，绝不改动 PENDING 等状态，
  因此不会凭空多出可领取任务。

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
- 积压控制：PENDING + PREEMPTED + WAITING + BLOCKED 数量达到 `max_backlog`
  时新任务被拒绝（`backlog_full`, HTTP 429），等待/阻断任务同样占槽，
  依赖解阻只是池内状态迁移，不无界占用资源。

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
- 依赖编排状态（dependencies / WAITING / BLOCKED / blocked_by / 已解阻的
  PENDING）全部包含在 mission 记录里，随快照与事件一并恢复；服务加载后会先
  对 WAITING/BLOCKED 做一次对账重算，任何在写入中途崩溃造成的偏差都在服务前
  收敛，且对账不会改动 PENDING 等状态，不凭空增加可领取任务。

## 数据格式演进

- 所有持久化模型 `extra="allow"`：含未知字段的记录解析不报错、字段原样
  保留并随重新持久化写回（不静默丢弃），但不参与调度判定。
- 新增字段一律带默认值，旧记录缺省语义确定（见
  `tests/test_evolution.py`）。

## HTTP API 摘要

| 方法/路径 | 说明 |
|---|---|
| `POST /robots/register` `/robots/heartbeat` `/robots/deregister` | 机器人生命周期 |
| `POST /missions` | 幂等下发（可带 `dependencies` 前置列表） |
| `POST /missions/acquire` | 领取（可指定 mission_id 或自动调度；仅 PENDING/PREEMPTED 可领） |
| `POST /missions/{id}/start` `/progress` `/complete` `/fail` `/renew` | 租约内操作 |
| `POST /missions/{id}/retry` `/cancel` `/preempt` | 重试/取消/抢占（会联动重算下游依赖态） |
| `GET /missions/{id}/dependencies` | 依赖等待视图：schedule/dependency/blocked + 各前置状态 |
| `POST /admin/sweep` `/admin/config` | 超时回收 / 配置 |
| `GET /missions` `/robots` `/missions/{id}/lease` | 查询 |

所有拒绝返回 `{"error": <reason>, "detail": <说明>}`，reason 取值见
`fleet_orchestrator/errors.py`。

## 验证方法

```bash
uv run pytest tests/ -q          # 全部 65 个测试
uv run pytest tests/ -q -s       # 打印每个用例的输入与判定依据 ([CASE] 日志)
uv run uvicorn main:app          # 启动服务，文档见 /docs
```

测试主题与文件对应：

| 主题 | 文件 |
|---|---|
| 依赖编排：等待/解阻、阻断传播与恢复、成环、先声明后补建、并发、容量、幂等、兼容与重放 | `tests/test_dependencies.py` |
| 心跳超时、租约过期、回收与重试 | `tests/test_lease_timeout.py` |
| 并发竞争（领取、完成 vs 回收、重复下发） | `tests/test_concurrency.py` |
| 抢占与优先级 | `tests/test_preemption.py` |
| 幂等下发与积压上限 | `tests/test_mission_submission.py` |
| 注册、能力与可用性 | `tests/test_robot_registration.py` |
| 非法跳转与非持有者上报 | `tests/test_state_machine.py` |
| 重启恢复与事件重放 | `tests/test_recovery.py` |
| 配置校验 | `tests/test_config.py` |
| 数据格式演进 | `tests/test_evolution.py` |
| HTTP 拒绝原因映射 | `tests/test_api.py` |
