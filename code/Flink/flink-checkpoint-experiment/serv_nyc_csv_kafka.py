#!/usr/bin/env python3
import time
import sys
import os
from datetime import datetime, timedelta

# 新增 Kafka 相关导入
try:
    from kafka import KafkaProducer
    from kafka.admin import KafkaAdminClient, NewTopic  # 👈 新增这一行
except ImportError:
    print("❌ 请先安装 kafka-python: pip install kafka-python")
    sys.exit(1)

def parse_pickup_time(line, col_index=1):
    """从 CSV 行解析 pickup 时间（默认第2列）"""
    try:
        parts = line.strip().split(',')
        if col_index < len(parts):
            dt_str = parts[col_index].strip().strip('"')
            if '.' in dt_str:
                dt_str = dt_str.split('.')[0]
            return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
    except Exception:
        pass
    return None

def serve_csv_lines(csv_file_path, kafka_bootstrap="localhost:9092", topic="nyc-taxi-raw", timerate=60):
    """
    读取 CSV 文件并按时间倍率发送到 Kafka。
    
    :param csv_file_path: CSV 文件路径
    :param kafka_bootstrap: Kafka broker 地址（如 "192.168.1.100:9092"）
    :param topic: Kafka topic 名称
    :param timerate: 时间倍率（现实 1 秒 = 数据中 timerate 秒）
    """
    if not os.path.exists(csv_file_path):
        print(f"❌ 文件不存在: {csv_file_path}")
        sys.exit(1)

    # ===== 新增：自动创建 Kafka Topic（8 分区）=====
    partitions_num = 8
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=kafka_bootstrap,
            client_id='nyc-taxi-producer'
        )
        new_topic = NewTopic(name=topic, num_partitions=partitions_num, replication_factor=1)
        admin_client.create_topics([new_topic], validate_only=False)
        print(f"✅ 自动创建 Kafka Topic: {topic} ({partitions_num} 分区)")
    except Exception as e:
        # 如果 topic 已存在，会报错，但可以安全忽略
        if "TopicExistsError" not in str(e):
            print(f"⚠️ 创建 Topic 时出错（可能已存在）: {e}")
        else:
            print(f"ℹ️ Kafka Topic '{topic}' 已存在，跳过创建")
    # ============================================

    # 读取并预处理 CSV（跳过表头）
    with open(csv_file_path, "r", encoding="utf-8") as f:
        lines = []
        header = f.readline().strip()
        if header and not header.replace(",", "").replace('"', '').replace('.', '').isdigit():
            print("⚠️  跳过表头行:", header)
        else:
            f.seek(0)

        for line in f:
            line = line.strip()
            if line:
                lines.append(line)

    # 解析每行的时间戳（使用第2列，索引=1）
    timed_lines = []
    for line in lines:
        dt = parse_pickup_time(line, col_index=1)
        if dt is not None:
            timed_lines.append((dt, line))
        else:
            timed_lines.append((datetime.min, line))

    # 按时间排序（确保顺序）
    timed_lines.sort(key=lambda x: x[0])
    valid_events = [(dt, line) for dt, line in timed_lines if dt != datetime.min]

    if not valid_events:
        print("⚠️ 无有效时间戳的数据")
        return

    print(f"📊 准备发送 {len(valid_events)} 行数据（已按 pickup 时间排序）")
    print(f"🔌 连接到 Kafka: {kafka_bootstrap}, topic: {topic}")

    # 创建 Kafka Producer
    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap,
        value_serializer=lambda v: v.encode('utf-8'),
        acks=1,
        retries=3
    )

    start_time = valid_events[0][0]
    current_sim_time = start_time
    index = 0
    total = len(valid_events)

    print(f"⏱️  开始回放（时间倍率: {timerate}x），起始时间: {start_time}")

    try:
        while index < total:
            window_end = current_sim_time + timedelta(seconds=timerate)
            batch = []
            while index < total and valid_events[index][0] <= window_end:
                batch.append(valid_events[index][1])
                index += 1

            # 发送到 Kafka
            for line in batch:
                producer.send(topic, value=line)

            sent_count = len(batch)
            print(f"📤 发送 {sent_count} 行到 Kafka（模拟时间 {current_sim_time} → {window_end}）")

            time.sleep(1)
            current_sim_time = window_end

        producer.flush(timeout=10)
        print(f"✅ 全部 {total} 行已发送到 Kafka！")
    except Exception as e:
        print(f"❌ 发送失败: {e}")
    finally:
        producer.close()

if __name__ == "__main__":
    import argparse

    # python serv_nyc_csv_kafka.py ./docker/assets/datasets/nyc-taxi/nyc-taxi-2023-01-fixed.csv --timerate=7200

    parser = argparse.ArgumentParser(description="NYC Taxi CSV 发送器（发送到 Kafka）")
    parser.add_argument("csv_file", help="NYC 出租车 CSV 文件路径，例如 nyc-taxi-2023-01.csv")
    parser.add_argument("--kafka", default="localhost:9092", help="Kafka bootstrap servers (默认: localhost:9092)")
    parser.add_argument("--topic", default="nyc-taxi-raw", help="Kafka topic 名称 (默认: nyc-taxi-raw)")
    parser.add_argument("--timerate", type=int, default=60, help="时间倍率（现实 1 秒 = 数据中 N 秒，默认 60）")

    args = parser.parse_args()

    serve_csv_lines(
        csv_file_path=args.csv_file,
        kafka_bootstrap=args.kafka,
        topic=args.topic,
        timerate=args.timerate
    )