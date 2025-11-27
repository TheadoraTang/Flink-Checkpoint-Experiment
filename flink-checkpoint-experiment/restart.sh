#!/bin/bash
# restart_all_taskmanagers.sh
# 功能：中断所有 TaskManager，让 Swarm 自动重新拉起

SERVICE_NAME=flink_taskmanager

echo "Restarting all TaskManagers for service $SERVICE_NAME ..."

# 强制更新 service，Swarm 会重新拉起所有 TaskManager
docker service update --force $SERVICE_NAME

echo "All TaskManagers are being restarted. Check status with:"
echo "  docker service ps $SERVICE_NAME"
echo "  docker stack ps flink"

