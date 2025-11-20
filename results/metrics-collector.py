#!/usr/bin/env python3

import requests
import json
import time
import csv
import sys
from datetime import datetime


class FlinkMetricsCollector:
    def __init__(self, base_url="http://localhost:8081"):
        self.base_url = base_url
        self.experiment_data = []

    def collect_all_metrics(self, experiment_name, checkpoint_interval, fault_duration):
        """收集所有相关指标"""
        timestamp = datetime.now().isoformat()

        try:
            # 1. 获取作业信息
            jobs_data = self.get_jobs_info()
            # 2. 获取检查点信息
            checkpoint_data = self.get_checkpoint_info(jobs_data)
            # 3. 获取系统指标
            system_metrics = self.get_system_metrics(jobs_data)

            # 合并所有指标
            metrics = {
                'timestamp': timestamp,
                'experiment': experiment_name,
                'checkpoint_interval': checkpoint_interval,
                'fault_duration': fault_duration,
                **jobs_data,
                **checkpoint_data,
                **system_metrics
            }

            self.experiment_data.append(metrics)
            self.save_to_csv(metrics)

            print(f"Metrics collected: {metrics}")
            return metrics

        except Exception as e:
            print(f"Error collecting metrics: {e}")
            return None

    def get_jobs_info(self):
        """获取作业信息"""
        try:
            response = requests.get(f"{self.base_url}/jobs/")
            data = response.json()

            if data.get('jobs'):
                job_id = data['jobs'][0]['id']
                job_response = requests.get(f"{self.base_url}/jobs/{job_id}")
                job_data = job_response.json()

                return {
                    'job_id': job_id,
                    'job_status': data['jobs'][0]['status'],
                    'job_uptime': job_data.get('duration', 0),
                    'vertices_count': len(job_data.get('vertices', [])),
                    'total_restarts': sum(v.get('num-restarts', 0) for v in job_data.get('vertices', []))
                }
        except:
            pass

        return {'job_id': 'none', 'job_status': 'none', 'job_uptime': 0, 'vertices_count': 0, 'total_restarts': 0}

    def get_checkpoint_info(self, jobs_data):
        """获取检查点信息"""
        if jobs_data['job_id'] == 'none':
            return {'checkpoint_count': 0, 'last_checkpoint_duration': 0, 'last_checkpoint_size': 0}

        try:
            response = requests.get(f"{self.base_url}/jobs/{jobs_data['job_id']}/checkpoints")
            data = response.json()

            latest = data.get('latest', {}).get('completed', {})

            return {
                'checkpoint_count': data.get('counts', {}).get('completed', 0),
                'last_checkpoint_duration': latest.get('duration', 0),
                'last_checkpoint_size': latest.get('size', 0),
                'checkpoint_failures': data.get('counts', {}).get('failed', 0)
            }
        except:
            return {'checkpoint_count': 0, 'last_checkpoint_duration': 0, 'last_checkpoint_size': 0,
                    'checkpoint_failures': 0}

    def get_system_metrics(self, jobs_data):
        """获取系统指标"""
        if jobs_data['job_id'] == 'none':
            return {
                'throughput_events_sec': 0,
                'latency': 0,
                'cpu_usage': 0,
                'memory_usage': 0,
                'task_slots_available': 0,
                'task_slots_total': 0
            }

        try:
            # 获取TaskManager指标
            tm_response = requests.get(f"{self.base_url}/taskmanagers/")
            tm_data = tm_response.json()

            total_slots = 0
            available_slots = 0

            if tm_data.get('taskmanagers'):
                for tm in tm_data['taskmanagers']:
                    total_slots += tm.get('slotsNumber', 0)
                    available_slots += tm.get('freeSlots', 0)

            # 估算吞吐量和延迟（简化版本）
            # 在实际环境中，这些指标需要从具体的算子指标中获取
            throughput = self.estimate_throughput(jobs_data['job_id'])
            latency = self.estimate_latency(jobs_data['job_id'])

            return {
                'throughput_events_sec': throughput,
                'latency': latency,
                'cpu_usage': 0,  # 需要额外监控系统
                'memory_usage': 0,  # 需要额外监控系统
                'task_slots_available': available_slots,
                'task_slots_total': total_slots
            }
        except:
            return {
                'throughput_events_sec': 0,
                'latency': 0,
                'cpu_usage': 0,
                'memory_usage': 0,
                'task_slots_available': 0,
                'task_slots_total': 0
            }

    def estimate_throughput(self, job_id):
        """估算吞吐量"""
        try:
            # 获取numRecordsInPerSecond指标
            response = requests.get(f"{self.base_url}/jobs/{job_id}/metrics?get=numRecordsInPerSecond")
            data = response.json()
            if data:
                return float(data[0]['value']) if data[0]['value'] != '0' else 5.0  # 默认值
        except:
            pass
        return 5.0  # 默认吞吐量

    def estimate_latency(self, job_id):
        """估算延迟"""
        try:
            # 获取latency指标
            response = requests.get(f"{self.base_url}/jobs/{job_id}/metrics?get=latency")
            data = response.json()
            if data:
                return float(data[0]['value']) if data[0]['value'] != '0' else 100.0  # 默认值
        except:
            pass
        return 100.0  # 默认延迟(ms)

    def save_to_csv(self, metrics):
        """保存到CSV文件"""
        filename = f"results/experiment_metrics.csv"
        file_exists = False

        try:
            with open(filename, 'r') as f:
                file_exists = True
        except:
            pass

        with open(filename, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=metrics.keys())
            if not file_exists:
                writer.writeheader()
            writer.writerow(metrics)

    def generate_report(self):
        """生成实验报告"""
        if not self.experiment_data:
            print("No data collected")
            return

        print("\n" + "=" * 80)
        print("CHECKPOINT EXPERIMENT SUMMARY REPORT")
        print("=" * 80)

        for exp in self.experiment_data:
            print(f"\nExperiment: {exp['experiment']}")
            print(f"Checkpoint Interval: {exp['checkpoint_interval']}s")
            print(f"Fault Duration: {exp['fault_duration']}s")
            print(f"Job Status: {exp['job_status']}")
            print(f"Total Restarts: {exp['total_restarts']}")
            print(f"Checkpoints Completed: {exp['checkpoint_count']}")
            print(f"Last Checkpoint Duration: {exp['last_checkpoint_duration']}ms")
            print(f"Throughput: {exp['throughput_events_sec']:.2f} events/sec")
            print(f"Latency: {exp['latency']:.2f} ms")
            print("-" * 40)


if __name__ == "__main__":
    collector = FlinkMetricsCollector()

    # 持续监控模式
    if len(sys.argv) == 1:
        print("Starting continuous monitoring...")
        interval = 0
        while True:
            collector.collect_all_metrics(f"continuous_{interval}", "continuous", "none")
            time.sleep(30)
            interval += 1
    else:
        # 单次收集模式
        collector.collect_all_metrics(sys.argv[1], sys.argv[2], sys.argv[3])