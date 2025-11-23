#!/bin/bash

echo "Starting system monitoring..."

while true; do
    TIMESTAMP=$(date +%Y-%m-%d_%H-%M-%S)
    
    # 收集容器资源使用情况
    echo "=== System Metrics at $TIMESTAMP ===" >> results/system_metrics.log
    
    # Docker容器资源使用
    echo "Docker Container Stats:" >> results/system_metrics.log
    docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}\t{{.BlockIO}}" >> results/system_metrics.log
    
    # 系统级监控
    echo "System Overview:" >> results/system_metrics.log
    echo "CPU: $(top -bn1 | grep "Cpu(s)" | awk '{print $2}')" >> results/system_metrics.log
    echo "Memory: $(free -h | grep Mem | awk '{print $3"/"$2}')" >> results/system_metrics.log
    echo "Disk: $(df -h / | awk 'NR==2 {print $3"/"$2}')" >> results/system_metrics.log
    
    echo "----------------------------------------" >> results/system_metrics.log
    
    sleep 30
done