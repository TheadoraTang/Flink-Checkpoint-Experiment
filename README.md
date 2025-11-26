# Flink Checkpoint 实验

## 实验目的
研究 Flink 检查点间隔对系统性能与容错能力的影响。

## 实验环境
- Flink 1.19.3 + Java 17
- Docker + Docker Compose
- 4个 TaskManager


## 检查点间隔配置
- 实验1: 30秒
- 实验2: 60秒  
- 实验3: 120秒

## 数据源
使用 NYC Taxi 2023年1月数据，循环读取模拟持续数据流。