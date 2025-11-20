#!/bin/bash

echo "Starting metrics monitoring..."

# Collect metrics via Flink REST API
while true; do
    TIMESTAMP=$(date +%Y-%m-%d_%H-%M-%S)
    
    # Get job metrics
    curl -s http://localhost:8081/jobs/ | jq '.' > results/jobs_${TIMESTAMP}.json
    
    # Get checkpoint statistics
    JOB_ID=$(curl -s http://localhost:8081/jobs/ | jq -r '.jobs[0].id')
    if [ "$JOB_ID" != "null" ]; then
        curl -s http://localhost:8081/jobs/${JOB_ID}/checkpoints | jq '.' > results/checkpoints_${TIMESTAMP}.json
        curl -s http://localhost:8081/jobs/${JOB_ID}/metrics | jq '.' > results/metrics_${TIMESTAMP}.json
    fi
    
    sleep 30
done