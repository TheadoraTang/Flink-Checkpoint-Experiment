### 1. 核心任务负载 (Workload Logic)

我们的Flink 作业是一个模拟的出租车营收统计流处理任务，旨在模拟一个“有状态”的流计算场景。

* **业务逻辑**：
    * **分组 (KeyBy)**：按照 `puLocationId`（上车地点 ID）进行分组。
    * **窗口 (Window)**：使用 **30秒** 的滚动处理时间窗口 (`TumblingProcessingTimeWindows`)。
    * **状态处理 (State)**：使用 `ValueState` 存储历史累计的“总营收 (`totalRevenue`)”和“总订单数 (`tripCount`)”。每当窗口触发时，将当前窗口的数据累加到历史状态中。
* **数据源 (Source)**：
    * **自定义源**：使用 `ContinuousTaxiSource`，它不是一次性读取文件，而是无限循环运行。
    * **生成机制**：它尝试读取 `nyc-taxi-2023-01.csv` 文件到内存池中。如果文件读取失败，它会生成 200 个模拟事件。然后它会无限循环地从这个池子中随机取数据发送，并对金额和距离做微小的随机扰动。

### 2. 实验运行模式 (Execution Mode)

通过脚本自动化执行，分为三组独立的对照实验，每组实验都包含“正常运行 -> 故障注入 -> 恢复”的过程。

* **自动化流程**：由 `run-corrected-experiment.sh` 编排。
* **三组对照实验**：
    1.  **实验 A**：Checkpoint 间隔 **30秒**。运行中途强制停止 `taskmanager1` 持续 **30秒**，然后恢复。
    2.  **实验 B**：Checkpoint 间隔 **60秒**。运行中途强制停止 `taskmanager2` 持续 **60秒**，然后恢复。
    3.  **实验 C**：Checkpoint 间隔 **120秒**。运行中途强制停止 `taskmanager3` 持续 **120秒**，然后恢复。
* **环境配置**：
    * 并行度设置为 3。
    * 状态后端为 `filesystem`。

### 3. 需要统计的数据 (Metrics Collection)

数据收集分为两个维度：Flink 内部指标和系统资源指标。

#### A. Flink 业务与容错指标 (由 `metrics-collector.py` 收集)
该脚本通过访问 Flink Rest API (`localhost:8081`) 每隔一定时间抓取一次数据：
1.  **检查点指标**：
    * `checkpoint_count`: 已完成的检查点数量。
    * `last_checkpoint_duration`: 最近一次检查点的耗时（毫秒）。
    * `last_checkpoint_size`: 最近一次检查点的大小（字节）。
2.  **故障恢复指标**：
    * `total_restarts`: 作业总重启次数（用于判断故障注入是否生效及恢复情况）。
    * `job_uptime`: 作业运行时长。
3.  **性能指标**：（这个抓取失败了）
    * `throughput_events_sec`: 吞吐量（尝试获取 `numRecordsInPerSecond`，获取失败则使用默认值）。
    * `latency`: 延迟（尝试获取 `latency`，获取失败使用默认值）。

#### B. 系统资源指标 (由 `system-monitor.sh` 收集)
该脚本每 30 秒运行一次：
1.  **容器级资源**：使用 `docker stats` 记录每个容器（JobManager, TaskManagers）的 CPU百分比、内存使用量、网络 I/O 和 磁盘块 I/O。
2.  **宿主机资源**：记录宿主机的全局 CPU、内存和磁盘使用率。
