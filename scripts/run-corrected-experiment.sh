#!/bin/bash

echo "=== Corrected Checkpoint Experiment ==="
echo "Running 3 separate experiments with different checkpoint intervals"

# 创建结果目录
mkdir -p results
rm -f results/experiment_metrics.csv

# 构建项目
echo "Building project..."
mvn clean package -DskipTests

# 部署基础设施
echo "Deploying Flink cluster..."
docker-compose down
docker-compose up -d
sleep 30

# 实验1: 30秒检查点 + 停止taskmanager1 (30秒)
echo ""
echo "=== EXPERIMENT 1: 30s Checkpoint + TaskManager1 Fault (30s) ==="
./scripts/run-checkpoint-experiment.sh 30000 taskmanager1 30 "exp_30s_checkpoint"

# 实验2: 60秒检查点 + 停止taskmanager2 (60秒)
echo ""
echo "=== EXPERIMENT 2: 60s Checkpoint + TaskManager2 Fault (60s) ==="
./scripts/run-checkpoint-experiment.sh 60000 taskmanager2 60 "exp_60s_checkpoint"

# 实验3: 120秒检查点 + 停止taskmanager3 (120秒)
echo ""
echo "=== EXPERIMENT 3: 120s Checkpoint + TaskManager3 Fault (120s) ==="
./scripts/run-checkpoint-experiment.sh 120000 taskmanager3 120 "exp_120s_checkpoint"

echo ""
echo "=== ALL EXPERIMENTS COMPLETED ==="
echo "Results: results/experiment_metrics.csv"