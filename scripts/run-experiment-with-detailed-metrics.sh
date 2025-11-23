#!/bin/bash

echo "=== Experiment with Detailed Metrics Collection ==="

# 创建结果目录
mkdir -p results
rm -f results/experiment_metrics_detailed.csv
rm -f results/system_metrics.log

# 启动系统监控
echo "Starting system monitor..."
./scripts/system-monitor.sh &
SYSTEM_MONITOR_PID=$!

# 启动详细指标收集
echo "Starting detailed metrics collector..."
python3 results/metrics-collector.py > results/detailed_metrics.log 2>&1 &
METRICS_PID=$!

# 运行实验
./scripts/run-corrected-experiment.sh

# 停止监控
echo "Stopping monitors..."
kill $SYSTEM_MONITOR_PID 2>/dev/null
kill $METRICS_PID 2>/dev/null

# 生成报告
echo "Generating detailed report..."
python3 results/metrics-collector.py report

echo "=== Experiment Completed ==="
echo "Detailed metrics: results/experiment_metrics_detailed.csv"
echo "System metrics: results/system_metrics.log"
echo "Detailed logs: results/detailed_metrics.log"