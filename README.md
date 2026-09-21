# Fleet Mission Orchestrator（机器人车队任务编排与调度）

本地持久化模拟机器人与任务状态，保证任务的领取、执行、抢占、超时回收与重试
在竞争和故障下**可解释、可恢复**。

## 快速开始

```bash
uv sync

# 运行测试（实时打印输入与判定依据）
uv run pytest

# 启动服务（设置 WAL 文件即开启持久化，否则用内存存储）
FLEET_WAL_PATH=./fleet.jsonl uv run uvicorn main:app --reload
# 打开 http://127.0.0.1:8000/docs 查看交互式接口
```

## 30 秒体验

```bash
# 1) 注册一个具备 nav 能力的机器人
curl -s -XPOST localhost:8000/robots/register \
  -H 'Content-Type: application/json' \
  -d '{"robot_id":"r1","capabilities":["nav"]}'

# 2) 下发任务
curl -s -XPOST localhost:8000/tasks -H 'Content-Type: application/json' \
  -d '{"task_id":"t1","payload":{"x":1},"required_capabilities":["nav"]}'

# 3) 机器人拉取任务（拿 fencing_token）
curl -s -XPOST localhost:8000/tasks/claim -H 'Content-Type: application/json' \
  -d '{"robot_id":"r1"}'

# 4) 带 fencing_token 完成（非持有者/旧 token 会被拒绝）
curl -s -XPOST localhost:8000/tasks/complete -H 'Content-Type: application/json' \
  -d '{"task_id":"t1","robot_id":"r1","fencing_token":1,"result":{"ok":true}}'

# 5) 查看一致快照
curl -s localhost:8000/snapshot
```

## 文档

- 状态机、租约/fencing token、调度与抢占规则、格式演进、恢复机制：
  见 [`docs/DESIGN.md`](docs/DESIGN.md)。
- 失败路径均返回可区分拒绝码 `code` 与判定依据 `reason`
  （枚举见 `app/errors.py`）。
