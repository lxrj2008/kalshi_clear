KalshiClear
===========

一个用于从 Kalshi 交易所拉取市场数据并同步到 SQL Server 的小型服务，包括：

- 定时全量/增量同步：系列（series）、事件（events）、市场（markets）、标签/筛选条件等。
- WebSocket 实时监听：订阅市场生命周期事件，增量更新数据库中的市场与事件信息。


项目结构概览
------------

- `config.py`：基于 `pydantic-settings` 的配置管理（从环境变量或 `.env` 读取）。
- `kalshi_client.py`：对官方 `kalshi-python` SDK 的封装，统一认证、日志和异常映射。
- `services/`：对 Kalshi HTTP API 的业务封装（Events / Markets / Series / Search 等）。
- `models/`：从 API payload 映射到数据库行的记录模型。
- `repositories/`：所有写入 SQL Server 的仓库，封装 `MERGE` / staging 表逻辑。
- `sync/jobs.py`：同步任务定义（tags/filters、series、events、markets），包含统一的分页 + 重试 + buffer 写入框架。
- `runtime/`：
  - `ws_runtime.py`：启动 WebSocket 监听线程。
  - `scheduler_runtime.py`：APScheduler 的构建和定时任务注册。
- `websocket_listener.py`：与 Kalshi WebSocket 的交互与消息处理。
- `main.py`：进程入口，只负责组装依赖、启动 WebSocket、跑首轮同步并启动定时任务。
- `cli.py`：交互式 CLI，可手动按 ticker 拉取单个 event/market/series 并写入数据库。


运行前准备
----------

### 1. 安装依赖

建议使用虚拟环境（略），在项目根目录执行：

```bash
pip install -r requirements.txt
```

### 2. 配置环境变量 / `.env`

项目通过 `KalshiSettings` 读取配置，支持下面这些关键变量（只列出常用的）：

- Kalshi API：
  - `KALSHI_HOST`（可选，默认官方 elections host）
  - `KALSHI_API_KEY_ID`
  - `KALSHI_PRIVATE_KEY_PATH`（指向 PEM 私钥文件）
- 日志：
  - `LOG_LEVEL`（默认 `INFO`）
  - `LOG_DIRECTORY`（默认 `logs`）
- SQL Server：
  - `SQLSERVER_HOST`
  - `SQLSERVER_PORT`
  - `SQLSERVER_DATABASE`
  - `SQLSERVER_SECONDARY_DATABASE`（存放参考/维度表）
  - `SQLSERVER_USERNAME`
  - `SQLSERVER_PASSWORD`
  - `SQLSERVER_DRIVER`（例如 `ODBC Driver 18 for SQL Server`）
- 邮件通知（可选，用于告警）：
  - `SMTP_HOST` / `SMTP_PORT`
  - `SMTP_FROM` / `SMTP_TO`
  - `SMTP_USERNAME` / `SMTP_PASSWORD`
  - `SMTP_USE_TLS`
- WebSocket 监听（可选，用于高频推送调优）：
  - `WS_WORKER_COUNT`（默认 `4`，范围 `1-32`，消息处理 worker 数）
  - `WS_QUEUE_MAXSIZE`（默认 `5000`，`0` 表示无界队列）
  - `WS_QUEUE_MONITOR_INTERVAL_SECONDS`（默认 `30`，范围 `5-3600`，队列监控日志间隔）

可以在项目根目录创建 `.env` 并填入上述变量；此文件已在 `.gitignore` 中忽略。


如何运行
--------

### 1. 启动主服务（定时同步 + WebSocket）

在项目根目录执行：

```bash
python main.py
```

行为说明：

- 启动 WebSocket 监听线程，订阅 `market_lifecycle_v2`（以及 event 生命周期），实时更新数据库。
- 如果 Kalshi 认证配置正确：
  - 立即执行一轮：
    - tags/filters（标签 & 体育/竞赛/范围 维度表）
    - series
    - events（带分页、缓冲和重试）
    - markets（带分页、缓冲和重试）
  - 启动 APScheduler，按 cron 周期运行对应 job：
    - tags/filters：每小时第 0 分
    - series：每小时第 5 分
    - events：每小时第 10 分
    - markets：每小时第 30 分（默认仅同步 open 状态，可带创建时间过滤）
- 按 `Ctrl+C` 时：
  - 通知 scheduler 停止并退出。
  - 通知 WebSocket 监听在下一轮循环中优雅退出。

### 2. 手动调试单条数据（`cli.py`）

执行：

```bash
python cli.py
```

根据提示选择：

- 1) 按 ticker 拉取单个 event，并写入数据库。
- 2) 按 ticker 拉取单个 market，并写入数据库。
- 3) 按 ticker 拉取单个 series，并写入数据库。


数据库相关说明
--------------

- 所有仓库都继承自 `BaseSQLRepository`，内部统一使用：
  - thread-local 的 `pyodbc` 连接工厂，减少频繁 `connect()` 开销且避免跨线程共用连接。
  - staging 表 + `MERGE` 的 upsert 流程（`*_TEMP` 表）。
- 部分写入只做 “不存在则插入”（`WHEN NOT MATCHED THEN INSERT`），如需更新已有行，可以：
  - 在对应仓库的 `MERGE` 语句中扩展 `WHEN MATCHED THEN UPDATE`。
  - 或通过额外的 `UPDATE` 方法（如 `MarketRepository.update_market_fields`）。


错误处理与告警
--------------

- Kalshi API 层：
  - HTTP / SDK 错误会映射为 `KalshiAPIError`，日志中包含 operation 名称与耗时。
- 数据库层：
  - 所有写入错误包装为 `DatabaseSaveError`，日志带具体异常。
- WebSocket / Scheduler：
  - 关键异常通过日志记录，并使用 `utils.notifications.send_throttled_email()` 发送节流告警邮件，避免抖动时邮件风暴。



