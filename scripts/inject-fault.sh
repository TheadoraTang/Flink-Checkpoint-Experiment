#!/bin/bash

INTERVAL=$1
TASKMANAGER=$2

if [ -z "$INTERVAL" ] || [ -z "$TASKMANAGER" ]; then
    echo "Usage: $0 <interval> <taskmanager>"
    echo "Example: $0 30 taskmanager1"
    exit 1
fi

echo "Starting experiment with checkpoint interval: ${INTERVAL}s"
echo "Will inject fault into: $TASKMANAGER"

# Update checkpoint interval
sed -i "s/execution.checkpointing.interval: .*/execution.checkpointing.interval: ${INTERVAL}s/" config/flink-conf.yaml

# Restart cluster with new configuration
docker-compose down
docker-compose up -d

sleep 30

# Submit job
JAR_FILE=$(find target -name "*.jar" | head -n 1)
docker-compose exec jobmanager flink run -d -c com.experiment.TaxiRevenueJob /opt/flink/data/$JAR_FILE

echo "Job started. Waiting 2 minutes for stabilization..."
sleep 120

echo "Injecting fault into $TASKMANAGER at $(date)"
docker-compose pause $TASKMANAGER

echo "TaskManager paused. Waiting for recovery..."
sleep 60

echo "Restoring $TASKMANAGER..."
docker-compose unpause $TASKMANAGER

echo "Fault injection completed. Monitoring recovery..."