import requests
import time

class FlinkMonitor:
    def __init__(self, flink_url, job_id):
        self.base_url = flink_url
        self.job_id = job_id
        self.vertices = self._get_vertices()
        
    def get_job_status(self):
        """获取作业状态 (RUNNING, RESTARTING, FAILING, etc.)"""
        try:
            url = f"{self.base_url}/jobs/{self.job_id}"
            resp = requests.get(url, timeout=2).json() 
            return resp.get('state', 'UNKNOWN')
        except Exception:
            return "CONNECTION_FAILED"

    def _get_vertices(self):
        """获取作业的所有算子ID，并区分 Source 和 Sink"""
        try:
            resp = requests.get(f"{self.base_url}/jobs/{self.job_id}")
            data = resp.json()
            vertices = {}
            for v in data.get('vertices', []):
                name = v['name']
                v_id = v['id']
                vertices[v_id] = name
                if "Source" in name:
                    self.source_id = v_id
                # 假设最后一个算子是 Sink 或者 Print
                self.sink_id = v_id
            return vertices
        except Exception as e:
            print(f"获取算子信息失败: {e}")
            return {}

    def get_checkpoint_duration(self):
        """获取最近一次成功 Checkpoint 的端到端耗时 (ms)"""
        try:
            url = f"{self.base_url}/jobs/{self.job_id}/checkpoints"
            resp = requests.get(url).json()
            completed = resp.get('latest', {}).get('completed')
            if completed:
                return completed['end_to_end_duration']
        except:
            pass
        return 0

    def get_completed_checkpoint_count(self):
        """返回成功完成的 checkpoint 数量"""
        try:
            url = f"{self.base_url}/jobs/{self.job_id}/checkpoints"
            resp = requests.get(url, timeout=2).json()
            return resp.get("summary", {}).get("numCompletedCheckpoints", 0)
        except:
            return 0

    def get_throughput(self):
        """获取 Source 节点的输出 TPS"""
        if not hasattr(self, 'source_id'): return 0
        try:
            url = f"{self.base_url}/jobs/{self.job_id}/vertices/{self.source_id}/metrics?get=numRecordsOutPerSecond"
            resp = requests.get(url).json()
            if resp and isinstance(resp, list):
                return float(resp[0]['value'])
        except:
            pass
        return 0.0

    def get_latency(self):
        """获取 Sink 节点的平均延迟"""
        if not hasattr(self, 'sink_id'): return 0
        try:
            url = f"{self.base_url}/jobs/{self.job_id}/vertices/{self.sink_id}/metrics"
            resp = requests.get(url).json()
            for m in resp:
                if 'latency' in m['id'] and 'mean' in m['id']:
                    val_resp = requests.get(f"{url}?get={m['id']}").json()
                    return float(val_resp[0]['value'])
        except:
            pass
        return 0.0

