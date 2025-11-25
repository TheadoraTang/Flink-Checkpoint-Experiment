import subprocess
import time
import requests
import csv
import os
from flink_monitor import FlinkMonitor

# --- 配置 ---
CHECKPOINT_INTERVALS = [1000, 5000, 10000, 30000]
DOWNTIME_DURATIONS = [30, 60, 120]
FLINK_URL = "http://localhost:8081"
JAR_PATH = "/opt/job/flink-experiment-1.0.jar"
CLASS_NAME = "com.experiment.TaxiStreamJob"
TM_SERVICE_NAME = "taskmanager"
STABLE_DURATION = 60          # 稳定运行 60s
POST_RECOVERY_DURATION = 60   # 恢复后观察 60s
INIT_WAIT = 3                 # 等待 Job 初始化

CSV_FILE = 'experiment_results.csv'
CSV_HEADER = [
    'CheckpointInterval_ms', 'Downtime_s', 'Time_s',
    'Throughput_r_s', 'Latency_ms', 'CheckpointDuration_ms',
    'CompletedCheckpoints', 'RecoveryDuration_s',
    'Phase', 'JobState'
]

# -----------------
# 工具函数
# -----------------
def run_command(cmd, check_status=True):
    try:
        subprocess.run(cmd, shell=True, check=check_status)
        return True
    except subprocess.CalledProcessError as e:
        if "flink run" in cmd or "flink cancel" in cmd:
            print(f"命令执行失败 (非致命): {e}")
            return False
        raise

def get_job_id():
    try:
        resp = requests.get(f"{FLINK_URL}/jobs/overview", timeout=5)
        jobs = resp.json()['jobs']
        for job in jobs:
            if job['state'] == 'RUNNING':
                return job['jid']
    except Exception:
        pass
    return None

def write_header_if_not_exists():
    if not os.path.exists(CSV_FILE) or os.stat(CSV_FILE).st_size == 0:
        with open(CSV_FILE, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(CSV_HEADER)

# -----------------
# 实验函数
# -----------------
def run_experiment(cp_interval, down_time):
    print(f"\n=== 开始实验: Checkpoint间隔={cp_interval}ms, 宕机时间={down_time}s ===")

    # 1. 提交任务
    submit_success = run_command(
        f"docker compose exec jobmanager flink run -d -p 1 -c {CLASS_NAME} {JAR_PATH} {cp_interval} /opt/data/nyc-taxi-2023-01.csv",
        check_status=False
    )
    if not submit_success:
        print("作业提交失败，跳过本次实验。")
        return

    time.sleep(INIT_WAIT)
    job_id = get_job_id()
    if not job_id:
        print("错误: 未找到运行作业，跳过。")
        return

    monitor = FlinkMonitor(FLINK_URL, job_id)

    total_run_time = STABLE_DURATION + down_time + POST_RECOVERY_DURATION
    metrics_log = []

    fault_injected = False
    recovery_started = False

    # ⭐ Recovery TPS 监控变量
    recovery_start_time = 0.0
    recovery_duration = 0.0
    recovery_in_progress = False

    print(f"Step 2: 开始连续监控，总时长 {total_run_time}s...")

    for t in range(total_run_time + 1):
        current_time = t
        job_state = monitor.get_job_status()

        if current_time <= STABLE_DURATION:
            phase = "STABLE"
        elif current_time <= STABLE_DURATION + down_time:
            phase = "FAULT"
        else:
            phase = "POST_RECOVERY"

        # ------------------- 数据采集 -------------------
        if job_state not in ['RUNNING']:
            tp, lat, cp_dur, cp_count = 0.0, 0.0, 0, 0
        else:
            tp = monitor.get_throughput()
            lat = monitor.get_latency()
            cp_dur = monitor.get_checkpoint_duration()
            cp_count = monitor.get_completed_checkpoint_count()  # ✅ checkpoint count

        # ------------------- Recovery Time 逻辑 -------------------
        if tp == 0 and not recovery_in_progress:
            recovery_start_time = time.time()
            recovery_in_progress = True
            print(f"--- [时间 {current_time}s] ⚡ Recovery Started ---")

        if tp > 0 and recovery_in_progress:
            recovery_duration = time.time() - recovery_start_time
            recovery_in_progress = False
            print(f"--- [时间 {current_time}s] ✅ Recovery Finished, Duration={recovery_duration:.2f}s ---")

        # ------------------- 注入故障 -------------------
        if current_time == STABLE_DURATION and not fault_injected:
            print(f"--- [时间 {current_time}s] 注入故障 -> 强制停止 {TM_SERVICE_NAME} ---")
            run_command(f"docker compose kill {TM_SERVICE_NAME}")
            fault_injected = True

        # ------------------- 恢复 TaskManager -------------------
        if current_time == STABLE_DURATION + down_time and not recovery_started:
            print(f"--- [时间 {current_time}s] 恢复故障 -> 启动 {TM_SERVICE_NAME} ---")
            run_command(f"docker compose start {TM_SERVICE_NAME}")
            recovery_started = True

        # ------------------- 写入日志 -------------------
        log_entry = [
            cp_interval, down_time, current_time,
            tp, lat, cp_dur,
            cp_count,
            recovery_duration,
            phase,
            job_state
        ]
        metrics_log.append(log_entry)

        print(f"[Time {current_time}/{total_run_time}s] State: {job_state:<10} | TP={tp:.0f} r/s, Lat={lat:.0f} ms, CP_Dur={cp_dur}ms, CP_Success={cp_count}, Recovery={recovery_duration:.2f}s")
        time.sleep(1)

    # 7. 清理作业
    print("Step 7: 实验结束，取消任务...")
    run_command(f"docker compose exec jobmanager flink cancel {job_id}")
    time.sleep(5)

    # 8. 写入 CSV
    write_header_if_not_exists()
    with open(CSV_FILE, 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(metrics_log)

    print(f"=== 实验结束 ===\n数据已写入 {CSV_FILE}")

# -----------------
# main
# -----------------
if __name__ == "__main__":
    run_command("docker compose down")
    run_command("docker compose up -d")
    print("等待集群启动 (3s)...")
    time.sleep(3)

    write_header_if_not_exists()
    run_experiment(cp_interval=5000, down_time=60)

