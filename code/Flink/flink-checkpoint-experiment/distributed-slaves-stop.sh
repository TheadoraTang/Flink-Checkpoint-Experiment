#!/bin/bash

# 定义 slave 列表
SLAVES=("slave-01" "slave-02" "slave-03" "slave-04")
REMOTE_BASE_DIR='~/flink-checkpoint-taskmanager/'

echo "Stopping slaves on all nodes..."

for slave in "${SLAVES[@]}"; do
    echo "Connecting to $slave and stopping slave process..."
    ssh "$slave" "cd $REMOTE_BASE_DIR && ./slaves-stop.sh"
    if [ $? -eq 0 ]; then
        echo "Successfully stopped on $slave"
    else
        echo "Failed to stop on $slave"
    fi
done

echo "All done."