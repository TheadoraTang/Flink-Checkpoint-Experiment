#!/bin/bash

CHECKPOINT_INTERVAL=$1
FAULT_TASKMANAGER=$2
FAULT_DURATION=$3
EXPERIMENT_NAME=$4

if [ -z "$CHECKPOINT_INTERVAL" ] || [ -z "$FAULT_TASKMANAGER" ] || [ -z "$FAULT_DURATION" ] || [ -z "$EXPERIMENT_NAME" ]; then
    echo "Usage: $0 <checkpoint_interval_ms> <fault_taskmanager> <fault_duration_sec> <experiment_name>"
    exit 1
fi

echo "Starting experiment: $EXPERIMENT_NAME"
echo "Checkpoint Interval: ${CHECKPOINT_INTERVAL}ms"
echo "Fault Target: $FAULT_TASKMANAGER"
echo "Fault Duration: ${FAULT_DURATION}s"

# 启动指标收集
echo "Starting metrics collection..."
python3 results/metrics-collector.py "${EXPERIMENT_NAME}_baseline" $((CHECKPOINT_INTERVAL/1000)) 0 &

# 提交作业（使用特定的检查点间隔）
JAR_FILE=$(find target -name "*.jar" | head -n 1)
JAR_NAME=$(basename "$JAR_FILE")

echo "Submitting job with checkpoint interval: ${CHECKPOINT_INTERVAL}ms"
docker cp "$JAR_FILE" flink-checkpoint-experiment_jobmanager_1:/opt/flink/
docker-compose exec -d jobmanager flink run -c com.experiment.TaxiRevenueJob /opt/flink/"$JAR_NAME" "$CHECKPOINT_INTERVAL"

# 等待作业启动和稳定
echo "Waiting for job to start and stabilize..."
sleep 30

# 检查作业状态
JOB_ID=$(curl -s http://localhost:8081/jobs/ | jq -r '.jobs[0].id' 2>/dev/null)
if [ "$JOB_ID" = "null" ] || [ -z "$JOB_ID" ]; then
    echo "ERROR: Job failed to start"
    exit 1
fi

echo "Job started successfully: $JOB_ID"
sleep 60  # 额外等待稳定

# 收集稳定状态指标
echo "Collecting stable state metrics..."
python3 results/metrics-collector.py "${EXPERIMENT_NAME}_stable" $((CHECKPOINT_INTERVAL/1000)) 0

# 注入故障
echo "Injecting fault: pausing $FAULT_TASKMANAGER for ${FAULT_DURATION}s..."
FAULT_START_TIME=$(date +%s)
docker-compose pause "$FAULT_TASKMANAGER"

# 故障期间监控
echo "Monitoring during fault..."
for i in $(seq 1 $FAULT_DURATION); do
    if [ $((i % 10)) -eq 0 ]; then  # 每10秒收集一次
        python3 results/metrics-collector.py "${EXPERIMENT_NAME}_fault_${i}s" $((CHECKPOINT_INTERVAL/1000)) $i
    fi
    sleep 1
done

# 恢复
echo "Restoring $FAULT_TASKMANAGER..."
docker-compose unpause "$FAULT_TASKMANAGER"
FAULT_END_TIME=$(date +%s)
ACTUAL_FAULT_DURATION=$((FAULT_END_TIME - FAULT_START_TIME))

echo "Actual fault duration: ${ACTUAL_FAULT_DURATION}s"

# 恢复期间监控
echo "Monitoring recovery for 120 seconds..."
for i in {1..12}; do
    sleep 10
    python3 results/metrics-collector.py "${EXPERIMENT_NAME}_recovery_${i}0s" $((CHECKPOINT_INTERVAL/1000)) $ACTUAL_FAULT_DURATION
done

# 最终状态
echo "Collecting final metrics..."
python3 results/metrics-collector.py "${EXPERIMENT_NAME}_final" $((CHECKPOINT_INTERVAL/1000)) $ACTUAL_FAULT_DURATION

# 停止作业
echo "Stopping job..."
curl -s -X PATCH http://localhost:8081/jobs/$JOB_ID > /dev/null
sleep 10

echo "Experiment $EXPERIMENT_NAME completed"
