import requests
import json
import csv
import time
import os
import re 
from datetime import datetime, timedelta

# --- 配置 (已更新) ---
FLINK_HOST = "localhost"
FLINK_PORT = 18081
BASE_URL = f"http://{FLINK_HOST}:{FLINK_PORT}"
METRICS_CSV_FILE = "metrics.csv"
STATUS_CSV_FILE = "status.csv"
RESTART_CSV_FILE = "restart.csv"      
POLL_INTERVAL_SECONDS = 1  # 轮询间隔时间（秒）

# --- 指标配置：使用更精确的关键词组合进行模糊匹配 ---
LATENCY_KEYWORD_A = "fetch-latency-avg" 
THROUGHPUT_KEYWORDS = ["numRecordsOutPerSecond", "Source"] 

# --- 全局变量 ---
JOB_ID = None           
SOURCE_VERTEX_ID = None 
metric_id_cache = {} 
restarting_start_time = None
is_restarting = False
last_status = None

# -----------------------------------------------------------
# 🛠️ 自动发现函数 (略)
# -----------------------------------------------------------

def discover_job_and_source_ids():
    """自动发现处于 RUNNING 状态的 JOB_ID 和其 Source 算子的 ID。"""
    # ... (代码与上一个版本相同，为简洁此处省略)
    global JOB_ID, SOURCE_VERTEX_ID
    
    jobs_url = f"{BASE_URL}/jobs"
    try:
        print("🔍 尝试获取正在运行的 Flink Job ID...")
        response = requests.get(jobs_url, timeout=5)
        response.raise_for_status()
        jobs_data = response.json()
        
        running_job = next((
            job for job in jobs_data.get('jobs', []) if job.get('status') == 'RUNNING'
        ), None)
        
        if running_job:
            JOB_ID = running_job['id']
            print(f"✅ 成功发现 JOB ID: {JOB_ID} (状态: RUNNING)")
        else:
            print("❌ 未找到处于 'RUNNING' 状态的 Job。请启动一个 Job 或手动设置 JOB_ID。")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"❌ 无法连接到 Flink Job Manager 或获取 Job 列表: {e}")
        return False
        
    job_details_url = f"{BASE_URL}/jobs/{JOB_ID}"
    try:
        print(f"🔍 尝试获取 Job {JOB_ID} 的 Source Vertex ID...")
        response = requests.get(job_details_url, timeout=5)
        response.raise_for_status()
        job_details = response.json()
        
        source_vertex = next((
            vertex for vertex in job_details.get('vertices', []) 
            if 'Source' in vertex.get('name', '')
        ), None)
        
        if source_vertex:
            SOURCE_VERTEX_ID = source_vertex['id']
            print(f"✅ 成功发现 Source Vertex ID: {SOURCE_VERTEX_ID} (名称: {source_vertex['name']})")
            return True
        else:
            print("❌ 未能找到名称中包含 'Source' 的 Vertex ID。请检查 Job 图。")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"❌ 无法获取 Job {JOB_ID} 详情: {e}")
        return False
# -----------------------------------------------------------


# -----------------------------------------------------------
# 📊 Metrics CSV 函数 (略)
# -----------------------------------------------------------

def initialize_metrics_csv():
    """初始化 metrics.csv 文件并写入表头。"""
    fieldnames = ["timestamp", "Latency", "Throughput"]
    if not os.path.exists(METRICS_CSV_FILE) or os.stat(METRICS_CSV_FILE).st_size == 0:
        with open(METRICS_CSV_FILE, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
        print(f"✅ 创建/初始化 {METRICS_CSV_FILE} 文件。")

def write_metrics_to_csv(timestamp, latency, throughput):
    """将指标写入 metrics.csv 文件。"""
    fieldnames = ["timestamp", "Latency", "Throughput"]
    data = {
        "timestamp": timestamp,
        "Latency": latency,
        "Throughput": throughput
    }
    
    for key, value in data.items():
        if value is None:
            data[key] = "N/A"
            
    try:
        with open(METRICS_CSV_FILE, 'a', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writerow(data)
    except IOError as e:
        print(f"❌ 写入 CSV 文件失败: {e}")

# -----------------------------------------------------------
# 🆕 新增：状态和重启时间 CSV 函数
# -----------------------------------------------------------

def initialize_status_csv():
    """初始化 status.csv 文件并写入表头。"""
    fieldnames = ["record_timestamp", "job_status"]
    if not os.path.exists(STATUS_CSV_FILE) or os.stat(STATUS_CSV_FILE).st_size == 0:
        with open(STATUS_CSV_FILE, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
        print(f"✅ 创建/初始化 {STATUS_CSV_FILE} 文件。")

# 🌟 修改点 1: 修改表头以记录时长 (Duration)
def initialize_restart_csv():
    """初始化 restart.csv 文件并写入表头。"""
    # 更改表头名称，使其更具描述性
    fieldnames = ["record_timestamp", "duration_minus_60s_seconds"] 
    if not os.path.exists(RESTART_CSV_FILE) or os.stat(RESTART_CSV_FILE).st_size == 0:
        with open(RESTART_CSV_FILE, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
        print(f"✅ 创建/初始化 {RESTART_CSV_FILE} 文件。")

def write_status_to_csv(record_timestamp, job_status):
    """将 Job 状态写入 status.csv 文件。"""
    fieldnames = ["record_timestamp", "job_status"]
    data = {
        "record_timestamp": record_timestamp,
        "job_status": job_status if job_status is not None else "N/A (API Failed)"
    }
    try:
        with open(STATUS_CSV_FILE, 'a', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writerow(data)
    except IOError as e:
        print(f"❌ 写入 {STATUS_CSV_FILE} 文件失败: {e}")

# 🌟 修改点 2: 修改函数以计算并记录 (restart_duration - 60)
def write_duration_minus_60_to_csv(record_timestamp, restart_duration):
    """
    计算 'restart_duration - 60s' 并将结果写入 restart.csv 文件。
    restart_duration 是浮点数，单位为秒。
    """
    fieldnames = ["record_timestamp", "duration_minus_60s_seconds"]
    
    # 计算持续时间减去 60 秒
    calculated_duration = restart_duration - 60.0
    
    data = {
        # 记录 Job 状态变为 RUNNING 时的当前时间戳
        "record_timestamp": record_timestamp, 
        # 记录计算后的时长
        "duration_minus_60s_seconds": f"{calculated_duration:.4f}" 
    }
    try:
        with open(RESTART_CSV_FILE, 'a', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writerow(data)
        print(f"📝 Job 重启时长（-60s）已记录到 {RESTART_CSV_FILE}：{data['duration_minus_60s_seconds']} 秒")
    except IOError as e:
        print(f"❌ 写入 {RESTART_CSV_FILE} 文件失败: {e}")


# -----------------------------------------------------------
# 🎯 核心函数 (略)
# -----------------------------------------------------------
def get_job_metrics():
    # ... (代码与上一个版本相同，为简洁此处省略)
    global metric_id_cache
    
    metrics_config = {
        'Latency': {'keywords': [LATENCY_KEYWORD_A]},
        'Throughput': {'keywords': THROUGHPUT_KEYWORDS}
    }
    
    subtask_index = 0
    metrics_url_all = (
        f"{BASE_URL}/jobs/{JOB_ID}/vertices/{SOURCE_VERTEX_ID}/subtasks/{subtask_index}/metrics"
    )

    try:
        if len(metric_id_cache) < 2:
            response = requests.get(metrics_url_all, timeout=5)
            response.raise_for_status()
            all_metrics_data = response.json()
            metric_id_cache.clear()
            for key, config in metrics_config.items():
                match = next((
                    item['id'] for item in all_metrics_data 
                    if all(keyword in item['id'] for keyword in config['keywords'])
                ), None)
                if not match: return None
                metric_id_cache[key] = match

        if not metric_id_cache: return None

        metric_ids_for_query = list(metric_id_cache.values())
        metrics_url_specific = (
             f"{metrics_url_all}?get={','.join(metric_ids_for_query)}"
        )
        
        response = requests.get(metrics_url_specific, timeout=5)
        response.raise_for_status()
        metrics_data = response.json()
        
        metrics_map = {item['id']: item.get('value') for item in metrics_data}
        
        return {
            "Latency": metrics_map.get(metric_id_cache.get('Latency')),
            "Throughput": metrics_map.get(metric_id_cache.get('Throughput'))
        }
        
    except requests.exceptions.RequestException as e:
        return None

def get_job_status():
    if not JOB_ID: return None 
    try:
        response = requests.get(f"{BASE_URL}/jobs/{JOB_ID}", timeout=5)
        response.raise_for_status()
        data = response.json()
        return data.get('state')
    except requests.exceptions.RequestException as e:
        return None
# -----------------------------------------------------------


# -----------------------------------------------------------
# --- 主循环 (更新了重启时长记录逻辑) ---
# -----------------------------------------------------------

def main():
    global restarting_start_time, is_restarting, last_status, JOB_ID, SOURCE_VERTEX_ID
    
    if not discover_job_and_source_ids():
        print("\n⚠️ 无法启动监控：未成功发现所需的 Job 或 Source ID。")
        return

    # 初始化所有 CSV 文件
    initialize_metrics_csv()
    initialize_status_csv()   
    initialize_restart_csv()  
    
    print(f"\n🚀 开始监控 Flink Job (ID: {JOB_ID})，轮询间隔 {POLL_INTERVAL_SECONDS} 秒...")
    print(f"🎯 Source Vertex ID: {SOURCE_VERTEX_ID}")
    print(f"数据将分别写入 {METRICS_CSV_FILE}, {STATUS_CSV_FILE}, {RESTART_CSV_FILE}\n")

    try:
        while True:
            current_time = datetime.now()
            # 使用微秒精度的时间戳字符串，用于所有记录
            current_time_str = current_time.strftime("%Y-%m-%d %H:%M:%S.%f")
            
            current_status = get_job_status()
            
            # --- 状态检查和重启时间计算 ---
            
            if current_status:
                
                # 记录状态到 status.csv
                write_status_to_csv(current_time_str, current_status)
                
                if current_status == 'RESTARTING' and not is_restarting:
                    is_restarting = True
                    restarting_start_time = time.time()
                    print(f"🔥 [{current_time.strftime('%H:%M:%S')}] Job 状态变为 **RESTARTING**。开始计时...")
                
                elif current_status == 'RUNNING' and is_restarting:
                    is_restarting = False
                    restarting_end_time = time.time()
                    restart_duration = restarting_end_time - restarting_start_time
                    
                    # 🌟 修改点 3: 记录 restart_duration - 60s
                    write_duration_minus_60_to_csv(current_time_str, restart_duration)
                    
                    print(f"✅ [{current_time.strftime('%H:%M:%S')}] Job 状态变回 **RUNNING**。")
                    print(f"⏱️ Job 重启持续时间: **{restart_duration:.4f} 秒**\n")
                    restarting_start_time = None
                    
                if current_status != last_status:
                    print(f"ℹ️ [{current_time.strftime('%H:%M:%S')}] Job 状态: **{current_status}**")
                
                last_status = current_status
            else:
                # 记录状态为 N/A 到 status.csv
                write_status_to_csv(current_time_str, None)
                print(f"⚠️ [{current_time.strftime('%H:%M:%S')}] 无法获取 Job 状态。请检查 Job Manager 是否运行。")

            # --- 指标收集 ---

            metrics = get_job_metrics()
            
            latency = None
            throughput = None
            
            if metrics:
                latency = metrics.get('Latency')
                throughput = metrics.get('Throughput')
                
                if latency is not None and throughput is not None:
                     print(f"📊 [{current_time.strftime('%H:%M:%S')}] Latency: {latency}, Throughput: {throughput}")
            
            # 写入 metrics.csv (使用微秒精度时间戳)
            write_metrics_to_csv(
                timestamp=current_time_str,
                latency=latency,
                throughput=throughput
            )
            
            time.sleep(POLL_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print("\n gracefully 退出监控脚本。")
    except Exception as e:
        print(f"\n发生意外错误: {e}")

if __name__ == "__main__":
    main()