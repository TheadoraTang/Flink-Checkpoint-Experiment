#!/usr/bin/env python3

import requests
import json
import time
import csv
from datetime import datetime

class FlinkMetricsCollector:
    def __init__(self, base_url="http://localhost:8081"):
        self.base_url = base_url
        self.metrics_file = "results/experiment_metrics.csv"
        self.setup_csv()
    
    def setup_csv(self):
        with open(self.metrics_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                'timestamp', 'checkpoint_interval', 'job_status',
                'last_checkpoint_duration', 'last_checkpoint_size',
                'throughput_events_per_sec', 'latency', 'task_failures',
                'total_restarts', 'uptime'
            ])
    
    def collect_metrics(self, checkpoint_interval):
        try:
            # Get job information
            jobs_resp = requests.get(f"{self.base_url}/jobs")
            jobs_data = jobs_resp.json()
            
            if not jobs_data['jobs']:
                return
            
            job_id = jobs_data['jobs'][0]['id']
            
            # Get job details
            job_resp = requests.get(f"{self.base_url}/jobs/{job_id}")
            job_data = job_resp.json()
            
            # Get checkpoint statistics
            checkpoint_resp = requests.get(f"{self.base_url}/jobs/{job_id}/checkpoints")
            checkpoint_data = checkpoint_resp.json()
            
            # Get metrics
            metrics_resp = requests.get(f"{self.base_url}/jobs/{job_id}/metrics")
            metrics_data = metrics_resp.resp()
            
            # Extract key metrics
            metrics = self.extract_metrics(job_data, checkpoint_data, metrics_data, checkpoint_interval)
            
            # Write to CSV
            with open(self.metrics_file, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(metrics)
                
        except Exception as e:
            print(f"Error collecting metrics: {e}")
    
    def extract_metrics(self, job_data, checkpoint_data, metrics_data, interval):
        timestamp = datetime.now().isoformat()
        
        # Extract checkpoint metrics
        last_checkpoint = checkpoint_data.get('latest', {}).get('completed', {})
        checkpoint_duration = last_checkpoint.get('duration', 0)
        checkpoint_size = last_checkpoint.get('size', 0)
        
        # Extract job metrics
        job_status = job_data.get('state', 'UNKNOWN')
        uptime = job_data.get('duration', 0)
        
        # Calculate throughput and latency (simplified)
        throughput = self.calculate_throughput(metrics_data)
        latency = self.calculate_latency(metrics_data)
        
        return [
            timestamp, interval, job_status,
            checkpoint_duration, checkpoint_size,
            throughput, latency, 0, 0, uptime
        ]
    
    def calculate_throughput(self, metrics_data):
        # Simplified throughput calculation
        # In practice, you'd query specific metrics
        return 0
    
    def calculate_latency(self, metrics_data):
        # Simplified latency calculation  
        # In practice, you'd query specific metrics
        return 0

if __name__ == "__main__":
    collector = FlinkMetricsCollector()
    
    # This would be called from the experiment script
    intervals = [30, 60, 120]
    for interval in intervals:
        print(f"Monitoring with checkpoint interval: {interval}s")
        for _ in range(60):  # Monitor for 5 minutes per interval
            collector.collect_metrics(interval)
            time.sleep(5)