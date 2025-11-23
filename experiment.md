### 1. 核心任务负载 (Workload Logic)

我们的Flink 作业是一个模拟的出租车营收统计流处理任务，旨在模拟一个“有状态”的流计算场景。

* **业务逻辑**：
    * **分组 (KeyBy)**：按照 `puLocationId`（上车地点 ID）进行分组。
    * **窗口 (Window)**：使用 **30秒** 的滚动处理时间窗口 (`TumblingProcessingTimeWindows`)。
    * **状态处理 (State)**：使用 `ValueState` 存储历史累计的“总营收 (`totalRevenue`)”和“总订单数 (`tripCount`)”。每当窗口触发时，将当前窗口的数据累加到历史状态中。
* **数据源 (Source)**：
    * **自定义源**：使用 `ContinuousTaxiSource`，它不是一次性读取文件，而是无限循环运行。
    * **生成机制**：它尝试读取 `nyc-taxi-2023-01.csv` 文件到内存池中。如果文件读取失败，它会生成 200 个模拟事件。然后它会无限循环地从这个池子中随机取数据发送，并对金额和距离做微小的随机扰动。
    * **速率控制**：代码中显式加入了 `Thread.sleep(200 + random.nextInt(300))`，这意味着单线程的吞吐量被限制在每秒约 2-5 条数据。

### 2. 实验运行模式 (Execution Mode)

通过脚本自动化执行，分为三组独立的对照实验，每组实验都包含“正常运行 -> 故障注入 -> 恢复”的过程。

* **自动化流程**：由 `run-corrected-experiment.sh` 编排。
* **三组对照实验**：
    1.  **实验 A**：Checkpoint 间隔 **30秒**。运行中途强制停止 `taskmanager1` 持续 **30秒**，然后恢复。
    2.  **实验 B**：Checkpoint 间隔 **60秒**。运行中途强制停止 `taskmanager2` 持续 **60秒**，然后恢复。
    3.  **实验 C**：Checkpoint 间隔 **120秒**。运行中途强制停止 `taskmanager3` 持续 **120秒**，然后恢复。
* **环境配置**：
    * 并行度设置为 3。
    * 状态后端为 `filesystem` (非 RocksDB)。
    * 重启策略为固定延迟重启（尝试 10 次，间隔 15 秒）。

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
3.  **性能指标**：
    * `throughput_events_sec`: 吞吐量（尝试获取 `numRecordsInPerSecond`，获取失败则使用默认值）。
    * `latency`: 延迟（尝试获取 `latency`，获取失败使用默认值）。

#### B. 系统资源指标 (由 `system-monitor.sh` 收集)
该脚本每 30 秒运行一次，直接在宿主机执行命令：
1.  **容器级资源**：使用 `docker stats` 记录每个容器（JobManager, TaskManagers）的 CPU百分比、内存使用量、网络 I/O 和 磁盘块 I/O。
2.  **宿主机资源**：记录宿主机的全局 CPU、内存和磁盘使用率。

---

### ⚠️ 潜在问题与优化建议 (重要)

基于对代码的分析，为了确保您的实验能得出“有效结论”，请注意以下两点：

1.  **负载压力过小 (Low Throughput)**：
    * **现状**：Source 中的 `Thread.sleep` 导致吞吐量极低（每秒仅几条数据）。
    * **后果**：由于数据量太小，State（状态）体积会非常小，Checkpoint 的耗时在 30s 和 120s 间隔下可能**几乎没有区别**（都是毫秒级）。这样您可能无法观察到“不同间隔对性能的影响”。
    * **建议**：移除或大幅减小 Source 中的 `Thread.sleep` 时间，或者增加 Source 的并行度，让系统处于高负载状态，这样 Checkpoint 带来的性能抖动才会明显。

2.  **状态增长有限 (Limited State Growth)**：
    * **现状**：代码是对 `puLocationId` 进行聚合。如果使用的是模拟数据，只有 10 个 Location ID；即使是 CSV 文件，Location ID 也是有限的（几百个）。
    * **后果**：State 的大小主要是 `Key的数量 * Value的大小`。Key 数量有限，状态大小很快就会达到上限不再增长。
    * **建议**：为了模拟“大状态”，可以在 Key 中加入时间戳或者随机后缀，或者在 ValueState 中存储一个不断增长的 List（虽然不推荐生产这样做，但为了实验增加状态大小是有效的）。