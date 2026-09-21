"""机器人车队任务编排与调度服务。

模块概览：
- errors:  可区分的拒绝原因（拒绝码）与领域异常
- models:  任务 / 机器人 / 租约的领域模型与状态枚举
- timeutil:可注入的时钟抽象（真实时钟 + 测试用假时钟）
- config:  调度与超时参数的可配置上限与校验
- storage: 本地持久化（WAL 事件日志 + 快照），支持回放恢复
- registry:机器人注册 / 心跳 / 能力声明
- tasks:   任务下发（幂等）、抢占与进度
- leases:  租约领取、续约、完成上报与状态机约束
- scheduler:调度、抢占、超时回收、重试
- service: 对外门面，聚合全部用例
- api:     FastAPI 路由层
"""

__all__ = ["errors", "models", "timeutil", "config", "storage",
           "registry", "tasks", "leases", "scheduler", "service", "api"]
