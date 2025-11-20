#!/bin/bash

echo "Starting checkpoint interval experiment..."

# Experiment 1: 30s checkpoint interval
echo "=== Experiment 1: 30s Checkpoint Interval ==="
./scripts/inject-fault.sh 30 taskmanager1
sleep 300  # Wait 5 minutes for metrics collection

# Experiment 2: 60s checkpoint interval  
echo "=== Experiment 2: 60s Checkpoint Interval ==="
./scripts/inject-fault.sh 60 taskmanager2
sleep 300

# Experiment 3: 120s checkpoint interval
echo "=== Experiment 3: 120s Checkpoint Interval ==="
./scripts/inject-fault.sh 120 taskmanager3
sleep 300

echo "All experiments completed!"