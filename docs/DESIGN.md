# 机器人车队任务编排与调度 — 设计说明

## 1. 总览

服务围绕**事件溯源（Event Sourcing）**构建：所有状态变更先作为一条事件
原子追加到仅追加日志（WAL，JSONL），再按固定顺序投影到内存状态。
进程重启时重放日志即可还原任务、租约、机器人与调度决策。

模块职责：

| 模块 | 职责 |
|------|------|
| `app/models.py` | 任务 / 机器人 / 租约模型与状态枚举，`schema_version` 演进 |
| `app/config.py` | 调度与超时参数上限，构造即严格校验 |
| `app/storage.py` | JSONL 事件日志（fsync）、损坏检测、未知字段/事件容忍 |
| `app/events.py` | 事件总线：持久化 + 固定顺序扇出投影（命令路径==回放路径） |
| `app/registry.py` | 机器人注册 / 心跳 / 能力声明 / 排空 / 注销 |
| `app/tasks.py` | 幂等下发、内容指纹、有界优先级队列、抢占标记 |
| `app/leases.py` | 租约领取 / 开始 / 续约 / 进度 / 完成 / 抢占 / 回收，fencing token |
| `app/scheduler.py` | 拉模式调度、抢占编排、心跳超时回收、重试、决策日志 |
| `app/service.py` | 门面：命令串行化（RLock）、装配、故障恢复 |
| `app/api.py` | FastAPI HTTP 层，拒绝码 → HTTP 状态映射 |

并发模型：单进程内所有命令在 `FleetService._lock`（`RLock`）下串行执行，
把"心跳 / 完成 / 续约 / 回收"的竞争转化为 WAL 上确定的全序。

---

## 2. 任务状态机

```
            submit 内容一致(幂等返回原结果)
  ┌──────────────────────────────────────────┐
  │                                          ▼
PENDING/QUEUED ──claim(授予租约)──▶ LEASED ──start──▶ RUNNING
    ▲  ▲                              │  ▲             │
    │  │                              │  │             │
    │  └──────── reclaim(回收)◀───────┘  │             │
    │             (token失效,保留进度)     │renew        │progress
    │                                    │             ▼
    │             PREEMPTED ──reschedule─▶QUEUED      RUNNING
    │              (抢占,保留进度)                        │
    │                                                    │ complete
    │                                             ┌──────▼──────┐
    └─────────────────────────────────────────────┤  COMPLETED  │ 终态
                                                  └─────────────┘
  reclaim 且 attempt > max_retries ─▶ FAILED (终态)
```

合法跳转要点：
- 仅 `QUEUED/PREEMPTED` 可 `claim`；仅 `LEASED` 可 `start`；
  `LEASED/RUNNING` 可上报进度/完成。
- `complete` / `start` / `renew` / `report_progress` 必须同时匹配
  **当前租约的 robot_id 与 fencing_token**，非持有者一律拒绝。
- 终态（COMPLETED/FAILED）不可逆；对终态任务回收返回
  `TASK_INVALID_TRANSITION`。

机器人状态：`REGISTERED ⇄ DRAINING`，心跳超时 → `STALE`，补发心跳恢复
`REGISTERED`；`DEREGISTERED` 心跳被拒，重新注册开启新生命周期。
`DRAINING` 可继续完成手头任务但不领取新任务。

---

## 3. 租约（Lease）与 Fencing Token

- 每次授予 / 重授租约产生**全局单调递增**的 `fencing_token`，并写入
  `TASK_CLAIMED` 事件；重启后从日志取最大 token，绝不回退。
- 持有者每次操作（start/renew/progress/complete）都必须带回 token：
  - 非当前 robot → `TASK_NOT_LEASE_HOLDER`
  - 是当前 robot 但 token 旧 → `LEASE_VERSION_CONFLICT`（判定为陈旧持有者）
  - 租约状态非 ACTIVE → `LEASE_EXPIRED`
  - `now > expires_at_ms` → `LEASE_EXPIRED`
- 回收（`reclaim`）使旧 token 立即失效；旧持有者之后的任何写都被拒绝，
  从根本上杜绝"同一任务两个执行者"。
- 完成是终态幂等：同一持有者 + 同一 token 重复 complete 返回首次结果；
  token 不匹配仍拒绝（不会用过期写覆盖结果）。

同一时刻每个任务至多一个 ACTIVE 租约（`_active_by_task`），
每个机器人至多持有一个任务（`robot.current_task_id`）。

---

## 4. 调度规则

### 4.1 领取（拉模式 `claim_next`）
- 机器人主动拉取；队列按 **优先级降序 → 同级 FIFO**，被抢占续跑任务在
  **同级最前**（堆项 group=0）。
- 队头能力不匹配则跳过尝试后续候选（不阻塞队列）；全部不匹配返回
  `granted=false`（HTTP 200，语义为排队/降级），不抛错、不丢任务。
- STALE/DRAINING/DEREGISTERED/已持任务 的机器人分别以可区分原因拒绝。

### 4.2 抢占（`request_preempt`）
- 仅当：高优任务在排队、受害者在执行（LEASED/RUNNING）、
  受害者 `preemptible=True`、**高优优先级严格更大**、发起机器人
  REGISTERED/空闲/具备所需能力，抢占才成功。
- 成功：旧租约置 PREEMPTED（token 失效），受害者任务**保留进度**并经
  `TASK_RESCHEDULED` 事件回队；发起机器人随后可立即领取高优任务。
- 失败原因可区分：`TASK_NOT_PREEMPTIBLE`（不可剥夺）、
  `PREEMPT_NO_ELIGIBLE_VICTIM`（非执行态 / 优先级不够）、
  `ROBOT_CAPABILITY_MISMATCH`（发起者能力不足）、
  `ROBOT_UNAVAILABLE`（发起者不空闲/不可用）。

### 4.3 心跳超时与租约过期回收（`reap_stale_leases`）
回收判定依据（任一命中）：
- `holder_stale`：持有者机器人 `now - last_heartbeat > heartbeat_timeout_ms`；
- `lease_expired`：`now > lease.expires_at_ms`（即便心跳仍新鲜）；
- `task_timeout`：`now > acquired_ms + task.timeout_ms`。

回收后：
- 旧 token 失效，任务保留进度回到队列等待重试（`resumed=true`）；
- `attempt <= max_retries` → 重新排队，`attempt` 递增；
- 否则转 `FAILED` 终态。
- 每次判定写一条 `SCHEDULER_DECISION` 事件（动作 + 原因 + 上下文），
  重启后同样回放，可解释、可审计。

### 4.4 资源边界
- 队列长度硬上限 `max_queue_size`：下发满员 → `QUEUE_FULL`。
- 超时回收需要重试时，若排队已达上限 → `QUEUE_FULL` 且**先不回收**
  （保持任务仍被持有，避免半更新与丢失；调用方需扩容/降级）。
- 租约续约时长受 `max_lease_duration_ms` 限制；任务参数
  （priority / max_retries / timeout_ms）均有硬上限，越界即
  `INVALID_CONFIG`。

---

## 5. 幂等与确定语义

- **机器人注册**：同 ID + 同能力集合 → 幂等返回；同 ID + 不同能力 →
  `ROBOT_ALREADY_REGISTERED`，须显式 `update_capabilities`。
- **任务下发**：对 `payload / required_capabilities / priority /
  preemptible / max_retries / timeout_ms` 计算 SHA256 内容指纹。
  同 ID + 同指纹 → 返回首次结果（绝不重复执行，终态结果固化）；
  同 ID + 不同内容 → `TASK_DUPLICATE_CONFLICT`，错误消息逐一列出差异字段。

---

## 6. 数据格式演进

- 每条记录带 `schema_version`（当前 1）。反序列化旧记录时按版本补齐
  新字段的稳定缺省值（见 `models.py` 的 `DEFAULT_*`）。
- 含未知字段的记录：未知**顶层字段**记入事件 `unknown_keys`，
  模型未知字段进入 `unknown_fields`，原样保留、绝不静默丢弃，
  且不参与任何分配判定。
- 未知的未来**事件类型**：`event_type=None`、保留 `raw_type`，
  回放时跳过投影但保留在日志中，不报错、不影响分配。
- 损坏（非 JSON / 缺必需字段 / seq 不连续）→ `CORRUPT_RECORD` 显式失败，
  服务拒绝启动，绝不静默丢数据。

---

## 7. 持久化与恢复

- `JsonFileStorage`：每行一个 JSON 事件，`flush + fsync` 后才算成功；
  `a+` 追加不截断；打开即校验 seq 连续性。
- 恢复：`EventBus.replay_all()` 按 WAL 全序、固定投影顺序
  （registry → tasks → leases → scheduler）重建全部状态。
- 因此重启后：任务状态 / 进度 / 结果、租约与 fencing token、
  机器人 current_task_id 与心跳时间、队列内容（含被抢占回队任务）、
  调度决策日志，均与故障前一致；无幽灵任务、无重复分配、无半更新。
- 内存存储 `InMemoryStorage` 实现相同仅追加语义，供单测使用。

---

## 8. HTTP 接口与拒绝码

| 方法/路径 | 说明 |
|-----------|------|
| POST `/robots/register` | 注册（幂等） |
| POST `/robots/heartbeat` | 心跳 |
| PUT `/robots/capabilities` | 更新能力 |
| PUT `/robots/drain` | 排空/恢复 |
| POST `/tasks` | 下发任务（幂等） |
| POST `/tasks/claim` | 拉取任务（无合格任务返回 granted=false） |
| POST `/tasks/start` | 开始执行 |
| POST `/tasks/renew` | 续约（可带 duration_ms，受上限约束） |
| POST `/tasks/progress` | 上报进度（浅合并快照） |
| POST `/tasks/complete` | 完成（终态幂等） |
| POST `/tasks/preempt` | 发起抢占 |
| POST `/scheduler/reap` | 触发超时回收 |
| GET `/snapshot` | 一致快照（队列/任务/租约/机器人/决策） |

错误响应：`{"ok":false,"code":"<REJECT_CODE>","reason":"<判定依据>"}`。
状态码：404 不存在；400 非法配置/参数；409 语义冲突；503 暂不可用/STALE；
500 持久化损坏。

启动：`FLEET_WAL_PATH=/tmp/fleet.jsonl uv run uvicorn main:app`
（不设置则用内存存储）。

---

## 9. 验证方法

```bash
uv sync
uv run pytest                      # 全量测试（实时打印输入与判定依据）
uv run pytest -q                   # 安静模式
```

测试覆盖（共 50+ 用例）：
- `test_registration.py`：注册幂等、能力冲突、心跳恢复、注销、排空；
- `test_task_idempotency.py`：重复下发一致/各字段冲突、终态固化、非法跳转；
- `test_timeout_reclaim.py`：心跳超时、租约自身过期、回收续跑、重试耗尽；
- `test_concurrency.py`：20 线程并发单任务唯一赢家、完成/回收竞态、
  fencing 双执行者拦截；
- `test_preemption.py`：抢占续跑、不可抢占、优先级不严格更高、
  发起者能力/空闲校验；
- `test_recovery.py`：JSON WAL 重启一致性、无重复分配、被抢占任务不丢失、
  损坏拒绝、未知字段/未来事件容忍；
- `test_config_and_queue.py`：非法配置拒绝、任务参数上限、有界队列；
- `test_http_api.py`：端到端与拒绝码→HTTP 状态映射。

关键测试用例在日志中显式打印 `输入 ...` 与 `判定依据 code=... reason=...`，
便于复盘竞争与失败路径。
