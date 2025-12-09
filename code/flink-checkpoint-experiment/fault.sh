#!/bin/bash

# =======================================================
# Flink Docker 容器循环定时故障注入脚本 (run_cyclic_docker.sh)
# =======================================================

# --- 配置 ---
# Flink REST API (用于验证连接，通常是 JobManager 容器的映射端口)
FLINK_HOST="localhost"
FLINK_REST_PORT=18081                     # ⚠️ 请确认您的 JobManager 映射端口
TM_CONTAINER_KEYWORD="taskmanager"       # TaskManager 容器名称中包含的关键字

PRE_RUN_DURATION_MINUTES=3               # 预运行时间 (每次注入前等待 Job 稳定)
STOP_DURATION_MINUTES=1                  # 模拟故障持续时间 (TM 停止时间)

# --- 循环间隔配置 (单位: 分钟) ---
INTERVALS=(10 10)
INJECTION_CSV="injection_timestamps_docker.csv"

PRE_RUN_DURATION_SECONDS=$((PRE_RUN_DURATION_MINUTES * 60))
STOP_DURATION_SECONDS=$((STOP_DURATION_MINUTES * 60))

# -------------------------------------------------------
# 初始化 CSV 文件
# -------------------------------------------------------
initialize_csv() {
    if [ ! -f $INJECTION_CSV ] || [ ! -s $INJECTION_CSV ]; then
        echo "unix_timestamp,formatted_timestamp,injection_interval_minutes,tm_containers_count" > $INJECTION_CSV
        echo "✅ 创建/初始化 ${INJECTION_CSV} 文件。"
    else
        echo "✅ ${INJECTION_CSV} 文件已存在，将追加数据。"
    fi
}

# -------------------------------------------------------
# 核心测试函数
# -------------------------------------------------------
run_test_cycle() {
    local INJECTION_INTERVAL_MINUTES=$1
    local INJECTION_INTERVAL_SECONDS=$((INJECTION_INTERVAL_MINUTES * 60))

    echo "========================================================"
    echo "🚀 [$(date +%H:%M:%S)] 测试周期开始 - 下一次注入间隔: ${INJECTION_INTERVAL_MINUTES} 分钟"

    # 1. 预运行和热身
    echo "1️⃣ 阶段：Job 预运行/热身 ${PRE_RUN_DURATION_MINUTES} 分钟..."
    sleep $PRE_RUN_DURATION_SECONDS

    # 2. 查找 TaskManager 容器
    TM_CONTAINERS=$(docker ps -q --filter "name=$TM_CONTAINER_KEYWORD" | tr '\n' ' ')
    TM_COUNT=$(echo $TM_CONTAINERS | wc -w)

    if [ -z "$TM_CONTAINERS" ]; then
        echo "❌ 错误: 未找到名称包含 '$TM_CONTAINER_KEYWORD' 的运行中 TaskManager 容器。"
        return 1
    fi
    echo "✅ 找到 ${TM_COUNT} 个运行中的 TaskManager 容器。"

    # 3. 模拟故障 (停止 TaskManagers 容器)
    echo "2️⃣ 阶段：模拟故障。停止 ${TM_COUNT} 个 TaskManager 容器..."

    # 记录注入开始时间
    INJECTION_START_UNIX=$(date +%s)
    INJECTION_START_FORMATTED=$(date +%Y-%m-%d\ %H:%M:%S)

    echo "⏱️ 注入开始时间: ${INJECTION_START_FORMATTED}"

    # 写入 CSV 文件
    echo "${INJECTION_START_UNIX},${INJECTION_START_FORMATTED},${INJECTION_INTERVAL_MINUTES},${TM_COUNT}" >> $INJECTION_CSV

    # 实际执行停止操作
    docker stop $TM_CONTAINERS

    # 4. 保持故障状态
    echo "3️⃣ 阶段：保持 TaskManagers 停止状态 ${STOP_DURATION_MINUTES} 分钟..."
    sleep $STOP_DURATION_SECONDS

    # 5. 触发恢复 (启动 TaskManagers 容器)
    echo "4️⃣ 阶段：触发恢复。启动 ${TM_COUNT} 个 TaskManager 容器..."

    # 实际执行启动操作
    docker start $TM_CONTAINERS

    echo "🎉 测试周期完成，进入等待期..."
    echo "========================================================"

    # 6. 等待下一个注入间隔
    echo "💤 正在等待 ${INJECTION_INTERVAL_MINUTES} 分钟 (直到 $(date -d "+$INJECTION_INTERVAL_SECONDS seconds" +%H:%M:%S))"
    sleep $INJECTION_INTERVAL_SECONDS

    return 0
}

# -------------------------------------------------------
# 主循环
# -------------------------------------------------------

initialize_csv
echo "========================================================"
echo "🎯 Flink Docker 持续故障注入模式启动。间隔循环: ${INTERVALS[@]} 分钟。"
echo "👉 请确保您的 Flink JobManager/TaskManagers 容器正在运行，且 Python 监控脚本也正在后台运行。"
echo "========================================================"

# 使用 trap 捕获中断信号
trap 'echo -e "\n🛑 捕获到 Ctrl+C，正在退出持续注入脚本..."; exit 0' INT

# 初始化循环索引
CURRENT_INDEX=0
INTERVALS_COUNT=${#INTERVALS[@]}

while true; do

    # 获取当前循环的间隔时间
    CURRENT_INTERVAL=${INTERVALS[$CURRENT_INDEX]}

    # 运行一次测试周期
    if ! run_test_cycle $CURRENT_INTERVAL; then
        echo "❌ 测试周期失败，退出脚本。"
        exit 1
    fi

    # 更新索引，实现循环
    CURRENT_INDEX=$(((CURRENT_INDEX + 1) % INTERVALS_COUNT))
done