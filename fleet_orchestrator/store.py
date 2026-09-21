"""本地持久化：原子快照 + 追加式事件日志。

恢复语义：
- 快照（state.json）通过 临时文件 + os.replace 原子写入，杜绝半更新状态。
- 事件日志（events.jsonl）记录每次状态变更后的完整记录，
  replay() 从空状态折叠全部事件，结果必须与快照一致（测试验证点）。
- 进程重启后 load() 优先读快照，快照缺失/损坏时回退到事件重放，
  不产生幽灵任务或重复分配（租约随状态一起恢复，过期判定基于时钟读数）。
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

STATE_FILE = "state.json"
EVENTS_FILE = "events.jsonl"

# 事件类型 -> (所属集合名, 主键名)
_EVENT_KINDS: dict[str, tuple[str, str]] = {
    "robot_upsert": ("robots", "robot_id"),
    "mission_upsert": ("missions", "mission_id"),
    "lease_upsert": ("leases", "mission_id"),
    "lease_delete": ("leases", "mission_id"),
}


def empty_state() -> dict[str, dict[str, Any]]:
    return {"robots": {}, "missions": {}, "leases": {}}


class Store:
    def __init__(self, directory: str | Path) -> None:
        self.dir = Path(directory)
        self.dir.mkdir(parents=True, exist_ok=True)

    # ---- 写入 ----

    def append_event(self, kind: str, record: dict[str, Any]) -> None:
        if kind not in _EVENT_KINDS:
            raise ValueError(f"unknown event kind: {kind}")
        line = json.dumps({"kind": kind, "record": record}, sort_keys=True)
        with open(self.dir / EVENTS_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
            f.flush()
            os.fsync(f.fileno())

    def save_snapshot(self, state: dict[str, dict[str, Any]]) -> None:
        tmp = self.dir / (STATE_FILE + ".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, sort_keys=True)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.dir / STATE_FILE)  # 原子替换

    # ---- 读取 / 恢复 ----

    def load_snapshot(self) -> dict[str, dict[str, Any]] | None:
        path = self.dir / STATE_FILE
        if not path.exists():
            return None
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError):
            return None  # 损坏快照回退到事件重放
        state = empty_state()
        for coll in state:
            for key, rec in (data.get(coll) or {}).items():
                state[coll][key] = rec
        return state

    def read_events(self) -> list[dict[str, Any]]:
        path = self.dir / EVENTS_FILE
        if not path.exists():
            return []
        events: list[dict[str, Any]] = []
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    events.append(json.loads(line))
                except json.JSONDecodeError:
                    # 尾部半行（崩溃时未写完）忽略，不阻断恢复
                    break
        return events

    def replay(self) -> dict[str, dict[str, Any]]:
        """从空状态折叠事件日志，重建最终状态。"""
        state = empty_state()
        for ev in self.read_events():
            kind = ev.get("kind")
            record = ev.get("record") or {}
            if kind not in _EVENT_KINDS:
                continue  # 未知事件类型（格式演进）跳过，不报错不静默改状态
            coll, key_name = _EVENT_KINDS[kind]
            key = record.get(key_name)
            if key is None:
                continue
            if kind.endswith("_delete"):
                state[coll].pop(key, None)
            else:
                state[coll][key] = record
        return state

    def load(self) -> dict[str, dict[str, Any]]:
        snap = self.load_snapshot()
        if snap is not None:
            return snap
        return self.replay()
