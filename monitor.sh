#!/bin/bash

# ==================== 配置区域 ====================
FLINK_HOME="/usr/local/flink"       # 【请修改】你的 Flink 安装目录 (此处保留，但启动脚本使用 ./)
FLINK_WEB_URL="http://localhost:8081" # Flink Web UI 地址
OUTPUT_FILE="metrics_monitor.csv"   # 监控数据输出文件
RECOVERY_LOG="recovery_result.log"  # 恢复时间记录文件

# ==================== 初始化检查 ====================
if ! command -v jq &> /dev/null; then
    echo "❌ 错误: 未检测到 jq。请先安装: yum install jq"
    exit 1
fi

# 清理旧数据并初始化 CSV Header
# CSV 格式: 时间戳, Job状态, Checkpoint耗时(ms), 总吞吐量(Records/s), 延迟(ms, 预留)
echo "Timestamp,JobState,CP_Duration,Total_Throughput,Latency" > "$OUTPUT_FILE"
echo "本次测试恢复时间记录:" > "$RECOVERY_LOG"

# 定义清理函数，确保脚本退出时停止后台监控
cleanup() {
    echo ""
    echo ">>> 正在停止后台监控进程..."
    if [ ! -z "$MONITOR_PID" ]; then
        kill "$MONITOR_PID" 2>/dev/null
    fi
    echo ">>> 测试脚本结束。"
}
trap cleanup EXIT

# ==================== 核心函数（保持不变） ====================

# 1. 获取当前正在运行的 Job ID
get_job_id() {
    curl -s "$FLINK_WEB_URL/jobs/overview" | jq -r '.jobs[] | select(.state=="RUNNING") | .jid' | head -n 1
}

# 2. 监控函数 (后台运行)
monitor_task() {
    local JOB_ID=$1
    while true; do
        TIMESTAMP=$(date "+%Y-%m-%d %H:%M:%S")

        # 获取 Job 详情 JSON
        JOB_DETAILS=$(curl -s "$FLINK_WEB_URL/jobs/$JOB_ID")

        # 1. 获取状态 (如果 API 失败，默认为 CONNECTING)
        STATE=$(echo "$JOB_DETAILS" | jq -r '.state // "CONNECTING"')

        # 2. 获取 Checkpoint 持续时间 (End to End Duration)
        CP_JSON=$(curl -s "$FLINK_WEB_URL/jobs/$JOB_ID/checkpoints")
        # 提取最新一次完成的 CP 耗时，如果没有则为 0
        CP_DURATION=$(echo "$CP_JSON" | jq -r '.latest.completed.end_to_end_duration // 0')

        # 3. 获取吞吐量 (这里保持原逻辑，请根据 Java 代码修改后的 Metrics Key 调整 jq 路径)
        THROUGHPUT=$(echo "$JOB_DETAILS" | jq -r '[.vertices[].metrics["write-records"] // 0] | add')
        if [ "$THROUGHPUT" == "null" ]; then THROUGHPUT=0; fi

        # 4. 延迟
        LATENCY=0

        # 写入文件
        echo "$TIMESTAMP,$STATE,$CP_DURATION,$THROUGHPUT,$LATENCY" >> "$OUTPUT_FILE"

        # 采样频率 1秒
        sleep 1
    done
}

# ==================== 主流程开始 ====================

echo "========================================================"
echo "   Flink 自动化故障恢复与监控测试脚本"
echo "========================================================"

# --- Step 1: 启动集群 ---
echo ">>> [Step 1] 正在启动 Master 和 Slaves..."
"./master-start.sh"
"./slaves-start.sh"

echo ">>> 集群启动命令已执行，等待 Web UI ($FLINK_WEB_URL) 就绪 (10s)..."
sleep 10

# --- Step 2: 人工介入 (保持不变) ---
echo "--------------------------------------------------------"
echo "🔴 待办事项:"
echo "1. 请打开浏览器访问 $FLINK_WEB_URL"
echo "2. 手动上传 Jar 包并提交 Job"
echo "3. 确保 Job 状态变为 RUNNING"
echo "--------------------------------------------------------"
read -p ">>> 确认 Job 已经在运行了吗？按 [Enter] 键开始监控并继续..."

# 获取 Job ID
TARGET_JOB_ID=$(get_job_id)

if [ -z "$TARGET_JOB_ID" ]; then
    echo "❌ 错误: 未能在 Web UI 找到状态为 RUNNING 的 Job。"
    exit 1
fi

echo "✅ 捕获到 Job ID: $TARGET_JOB_ID"

# --- Step 3: 开启监控 (保持不变) ---
echo ">>> [Step 3] 启动后台监控，数据写入 $OUTPUT_FILE ..."
monitor_task "$TARGET_JOB_ID" &
MONITOR_PID=$!

# --- Step 4: 稳定运行 60s (保持不变) ---
echo ">>> [Step 4] 系统将稳定运行 60 秒..."
for i in {60..1}; do
    printf "倒计时: %2d 秒 \r" "$i"
    sleep 1
done
echo ">>> 稳定运行结束。"

# --- Step 5: 模拟故障 (Stop Slaves) ---
echo ">>> [Step 5] 执行 slaves-stop.sh (模拟 TaskManager 宕机)..."
"./slaves-stop.sh"
echo ">>> 故障已触发！正在等待 Job 状态变为 RESTARTING..."

# 捕捉 RESTARTING 开始时间（从 Job Manager 检测到故障开始计时）
RESTARTING_START_TIME_MS=0
while true; do
    CURRENT_STATE=$(curl -s "$FLINK_WEB_URL/jobs/$TARGET_JOB_ID" | jq -r '.state')

    # 只要状态不是 RUNNING 且不是 CREATED，我们就认为它开始进入故障流程
    if [ "$CURRENT_STATE" == "RESTARTING" ] || [ "$CURRENT_STATE" == "FAILING" ]; then
        RESTARTING_START_TIME_MS=$(date +%s%3N)
        echo "✅ Job 状态已变为 $CURRENT_STATE。开始计时（总停机时间）。"
        break
    fi
    sleep 0.1
done




echo ">>> [Step 8] 正在轮询 Job 状态 (等待恢复到 RUNNING)..."

# 循环检查直到状态变回 RUNNING
RESTARTING_END_TIME_MS=0
while true; do
    # 获取当前状态
    CURRENT_STATE=$(curl -s "$FLINK_WEB_URL/jobs/$TARGET_JOB_ID" | jq -r '.state')

    if [ "$CURRENT_STATE" == "RUNNING" ]; then
        RESTARTING_END_TIME_MS=$(date +%s%3N)
        break
    fi

    # 防止 API 请求过于频繁，设置 0.1s 间隔
    sleep 0.1
done

# 计算耗时 (总故障停机时间：从 Flink 发现故障到 Job 恢复 RUNNING)
RECOVERY_TIME_MS=$((RESTARTING_END_TIME_MS - RESTARTING_START_TIME_MS))
RECOVERY_TIME_SEC=$(echo "scale=3; $RECOVERY_TIME_MS / 1000" | bc)

# 执行启动脚本
"./slaves-start.sh"

echo "✅ Job 已恢复运行！"
echo "--------------------------------------------------------"
echo "📊 故障恢复统计 (Total Downtime from Failure Detection to RUNNING):"
echo "Job ID: $TARGET_JOB_ID"
echo "总停机时间: $RECOVERY_TIME_MS ms ($RECOVERY_TIME_SEC 秒)"
echo "--------------------------------------------------------"

# 将结果写入日志
echo "$(date "+%Y-%m-%d %H:%M:%S") - Job: $TARGET_JOB_ID - TotalDowntime: ${RECOVERY_TIME_MS}ms" >> "$RECOVERY_LOG"

echo ">>> 测试流程全部完成。"
echo ">>> 监控数据已保存至: $OUTPUT_FILE"
echo ">>> 按任意键退出脚本 (退出将停止后台监控)..."
read -n 1