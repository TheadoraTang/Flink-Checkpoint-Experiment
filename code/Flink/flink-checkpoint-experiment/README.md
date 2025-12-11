
# 批量部署免 sudo 配置

```
# 宿主机添加用户到 docker 组
sudo usermod -aG docker $USER

# 宿主机添加 9999 flink 组
sudo groupadd -g 9999 flink

# 宿主机添加用户到 flink 组
sudo usermod -aG flink $USER
```

# 部署步骤

1. 免密登录，在 `.ssh/config` 中配置别名 master, slave-01, slave-02, slave-03, slave-04,
2. 执行脚本 distributed-install.sh，会在用户根目录下生成 flink-checkpoint-taskmanager 文件夹

# 执行步骤

修改服务器信息 `<hostname_or_ip>`

补全 `flink-s3-fs-hadoop-1.19.3.jar` 和 `nyc-taxi-2023-01-fixed.csv`

注：`nyc-taxi-2023-01.csv` -> `sort_nyc_csv.py` -> `nyc-taxi-2023-01-fixed.csv`

1. 启动 `./master-start.sh`（包含 MinIO，kafka，JobManager）
2. 启动 `./distributed-slaves-start.sh`（启动TaskManager）
3. 上传作业到 Flink 平台
4. 启动 `python ./serv_nyc_csv_kafka.py`（Python 发包器，作为数据源）
5. 执行作业
6. 启动 `cd ./monitor/ && ./monitor.py`（用于采集指标）
7. 在特定服务器上启动 `./fault.sh`（用于中断模拟）
