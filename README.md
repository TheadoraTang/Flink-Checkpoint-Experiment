# Flink Checkpoint 实验

## 实验目的
研究 Flink 检查点间隔对系统性能与容错能力的影响。

## 实验环境
- Flink 1.19.3 + Java 17
- Docker + Docker Compose
- 3个 TaskManager

## 实验步骤
### 克隆项目
```bash
git clone <your-repo>
cd flink-checkpoint-experiment
```

### 将 nyc-taxi-2023-01.csv 放入 data/ 目录

### 安装依赖
```bash
sudo apt-get update
sudo apt-get install docker docker-compose python3-pip
```


### 创建虚拟环境
```bash
python3 -m venv flink-env
```


### 激活虚拟环境
```bash
source flink-env/bin/activate
```


### 安装依赖
```bash
pip install psutil requests
```


### 给脚本执行权限
```bash
chmod +x scripts/*.sh
```


### 运行优化后的实验
```bash
./scripts/run-experiment-with-detailed-metrics.sh
```

## 检查点间隔配置
- 实验1: 30秒
- 实验2: 60秒  
- 实验3: 120秒

## 数据源
使用 NYC Taxi 2023年1月数据，循环读取模拟持续数据流。