#!/usr/bin/env python3

import requests
import json
import time
import csv
import sys
import psutil
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
            # 3. 获取Flink系统指标
            flink_metrics = self.get_flink_metrics(jobs_data)
            # 4. 获取系统资源指标
            system_metrics = self.get_system_metrics()
            
            # 合并所有指标
            metrics = {
                'timestamp': timestamp,
                'experiment': experiment_name,
                'checkpoint_interval': checkpoint_interval,
                'fault_duration': fault_duration,
                **jobs_data,
                **checkpoint_data,
                **flink_metrics,
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
            response = requests.get(f"{self.base_url}/jobs/", timeout=5)
            data = response.json()
            
            if data.get('jobs'):
                job_id = data['jobs'][0]['id']
                job_response = requests.get(f"{self.base_url}/jobs/{job_id}", timeout=5)
                job_data = job_response.json()
                
                return {
                    'job_id': job_id,
                    'job_status': data['jobs'][0]['status'],
                    'job_uptime': job_data.get('duration', 0),
                    'vertices_count': len(job_data.get('vertices', [])),
                    'total_restarts': sum(v.get('num-restarts', 0) for v in job_data.get('vertices', []))
                }
        except Exception as e:
            print(f"Error getting job info: {e}")
        
        return {'job_id': 'none', 'job_status': 'none', 'job_uptime': 0, 'vertices_count': 0, 'total_restarts': 0}
    
    def get_checkpoint_info(self, jobs_data):
        """获取检查点信息 - 修复版本"""
        if jobs_data['job_id'] == 'none':
            return {
                'checkpoint_count': 0, 
                'last_checkpoint_duration': 0, 
                'last_checkpoint_size': 0,
                'checkpoint_failures': 0,
                'checkpoint_alignment_buffered': 0
            }
        
        try:
            response = requests.get(f"{self.base_url}/jobs/{jobs_data['job_id']}/checkpoints", timeout=5)
            data = response.json()
            
            # 获取最新的已完成检查点
            latest_completed = data.get('latest', {}).get('completed', {})
            
            # 尝试从历史记录中获取更多检查点详情
            checkpoints_history = data.get('history', [])
            recent_checkpoints = [cp for cp in checkpoints_history if cp.get('status') == 'COMPLETED']
            
            if recent_checkpoints:
                latest_checkpoint = recent_checkpoints[0]  # 最新的检查点
                latest_duration = latest_checkpoint.get('duration', 0)
                latest_size = latest_checkpoint.get('size', 0)
            else:
                latest_duration = latest_completed.get('duration', 0)
                latest_size = latest_completed.get('size', 0)
            
            return {
                'checkpoint_count': data.get('counts', {}).get('completed', 0),
                'last_checkpoint_duration': latest_duration,
                'last_checkpoint_size': latest_size,
                'checkpoint_failures': data.get('counts', {}).get('failed', 0),
                'checkpoint_alignment_buffered': latest_completed.get('alignment_buffered', 0)
            }
        except Exception as e:
            print(f"Error getting checkpoint info: {e}")
            return {
                'checkpoint_count': 0, 
                'last_checkpoint_duration': 0, 
                'last_checkpoint_size': 0,
                'checkpoint_failures': 0,
                'checkpoint_alignment_buffered': 0
            }
    
    def get_flink_metrics(self, jobs_data):
        """获取Flink系统指标 - 修复版本"""
        if jobs_data['job_id'] == 'none':
            return {
                'throughput_events_sec': 0,
                'latency': 0,
                'num_records_in': 0,
                'num_records_out': 0,
                'busy_time_per_sec': 0,
                'back_pressure_time': 0
            }
        
        try:
            # 获取所有顶点信息
            vertices_response = requests.get(f"{self.base_url}/jobs/{jobs_data['job_id']}", timeout=5)
            vertices_data = vertices_response.json()
            
            total_throughput = 0
            total_latency = 0
            vertex_count = 0
            
            for vertex in vertices_data.get('vertices', []):
                vertex_id = vertex['id']
                
                # 获取吞吐量指标 - 使用更可靠的指标
                throughput_metrics = self.get_vertex_metrics(jobs_data['job_id'], vertex_id, [
                    'numRecordsInPerSecond',
                    'numRecordsOutPerSecond'
                ])
                
                # 获取延迟指标
                latency_metrics = self.get_vertex_metrics(jobs_data['job_id'], vertex_id, [
                    'latency'
                ])
                
                # 计算吞吐量 (使用输入记录率)
                records_in = float(throughput_metrics.get('numRecordsInPerSecond', 0))
                records_out = float(throughput_metrics.get('numRecordsOutPerSecond', 0))
                
                # 使用较大的值作为吞吐量估算
                vertex_throughput = max(records_in, records_out)
                total_throughput += vertex_throughput
                
                # 计算延迟
                vertex_latency = float(latency_metrics.get('latency', 0))
                if vertex_latency > 0:
                    total_latency += vertex_latency
                    vertex_count += 1
            
            # 计算平均延迟
            avg_latency = total_latency / vertex_count if vertex_count > 0 else 0
            
            # 获取背压信息
            back_pressure = self.get_back_pressure_metrics(jobs_data['job_id'])
            
            return {
                'throughput_events_sec': total_throughput,
                'latency': avg_latency,
                'num_records_in': total_throughput,  # 简化处理
                'num_records_out': total_throughput, # 简化处理
                'busy_time_per_sec': 0,  # 需要特定指标
                'back_pressure_time': back_pressure
            }
            
        except Exception as e:
            print(f"Error getting Flink metrics: {e}")
            return {
                'throughput_events_sec': 0,
                'latency': 0,
                'num_records_in': 0,
                'num_records_out': 0,
                'busy_time_per_sec': 0,
                'back_pressure_time': 0
            }
    
    def get_vertex_metrics(self, job_id, vertex_id, metric_names):
        """获取顶点特定指标"""
        metrics = {}
        for metric_name in metric_names:
            try:
                url = f"{self.base_url}/jobs/{job_id}/vertices/{vertex_id}/metrics?get={metric_name}"
                response = requests.get(url, timeout=5)
                data = response.json()
                if data and data[0]['value'] != '0':
                    metrics[metric_name] = float(data[0]['value'])
                else:
                    metrics[metric_name] = 0
            except:
                metrics[metric_name] = 0
        return metrics
    
    def get_back_pressure_metrics(self, job_id):
        """获取背压指标"""
        try:
            # 获取所有顶点
            response = requests.get(f"{self.base_url}/jobs/{job_id}", timeout=5)
            vertices_data = response.json()
            
            total_back_pressure = 0
            vertex_count = 0
            
            for vertex in vertices_data.get('vertices', []):
                vertex_id = vertex['id']
                # 获取背压状态 (简化版本)
                back_pressure_url = f"{self.base_url}/jobs/{job_id}/vertices/{vertex_id}/backpressure"
                bp_response = requests.get(back_pressure_url, timeout=5)
                bp_data = bp_response.json()
                
                if bp_data.get('status') == 'ok':
                    backpressure_level = bp_data.get('backpressure-level', 'low')
                    # 将背压级别转换为数值
                    if backpressure_level == 'high':
                        total_back_pressure += 100
                    elif backpressure_level == 'medium':
                        total_back_pressure += 50
                    else:
                        total_back_pressure += 10
                    vertex_count += 1
            
            return total_back_pressure / vertex_count if vertex_count > 0 else 0
            
        except:
            return 0
    
    def get_system_metrics(self):
        """获取系统资源指标"""
        try:
            # CPU使用率
            cpu_percent = psutil.cpu_percent(interval=1)
            
            # 内存使用率
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            
            # 磁盘I/O (简化)
            disk_io = psutil.disk_io_counters()
            disk_read = disk_io.read_bytes if disk_io else 0
            disk_write = disk_io.write_bytes if disk_io else 0
            
            # 网络I/O (简化)
            net_io = psutil.net_io_counters()
            net_sent = net_io.bytes_sent if net_io else 0
            net_recv = net_io.bytes_recv if net_io else 0
            
            return {
                'cpu_usage_percent': cpu_percent,
                'memory_usage_percent': memory_percent,
                'disk_read_bytes': disk_read,
                'disk_write_bytes': disk_write,
                'network_sent_bytes': net_sent,
                'network_recv_bytes': net_recv
            }
            
        except Exception as e:
            print(f"Error getting system metrics: {e}")
            return {
                'cpu_usage_percent': 0,
                'memory_usage_percent': 0,
                'disk_read_bytes': 0,
                'disk_write_bytes': 0,
                'network_sent_bytes': 0,
                'network_recv_bytes': 0
            }
    
    def save_to_csv(self, metrics):
        """保存到CSV文件"""
        filename = f"results/experiment_metrics_detailed.csv"
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
        
        print("\n" + "="*80)
        print("DETAILED CHECKPOINT EXPERIMENT SUMMARY REPORT")
        print("="*80)
        
        for exp in self.experiment_data:
            print(f"\nExperiment: {exp['experiment']}")
            print(f"Checkpoint Interval: {exp['checkpoint_interval']}s")
            print(f"Fault Duration: {exp['fault_duration']}s")
            print(f"Job Status: {exp['job_status']}")
            print(f"Throughput: {exp['throughput_events_sec']:.2f} events/sec")
            print(f"Latency: {exp['latency']:.2f} ms")
            print(f"Last Checkpoint Duration: {exp['last_checkpoint_duration']}ms")
            print(f"CPU Usage: {exp['cpu_usage_percent']:.1f}%")
            print(f"Memory Usage: {exp['memory_usage_percent']:.1f}%")
            print("-" * 40)

if __name__ == "__main__":
    collector = FlinkMetricsCollector()
    
    # 安装依赖检查
    try:
        import psutil
    except ImportError:
        print("Installing required dependencies...")
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "psutil"])
        import psutil
    
    # 持续监控模式
    if len(sys.argv) == 1:
        print("Starting continuous monitoring...")
        interval = 0
        while True:
            collector.collect_all_metrics(f"continuous_{interval}", "continuous", "none")
            time.sleep(30)
            interval += 1
    elif sys.argv[1] == "report":
        collector.generate_report()
    else:
        # 单次收集模式
        collector.collect_all_metrics(sys.argv[1], sys.argv[2], sys.argv[3])
