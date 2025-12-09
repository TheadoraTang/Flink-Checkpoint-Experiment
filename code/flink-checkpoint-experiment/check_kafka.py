from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'nyc-taxi-raw',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',  # 从头开始
    enable_auto_commit=False,
    consumer_timeout_ms=5000  # 5秒无消息就退出
)

print("正在从 Kafka 消费消息（最多显示10条）...")
count = 0
for msg in consumer:
    print(msg.value.decode('utf-8'))
    count += 1
    if count >= 10:
        break

print(f"\n✅ 共消费 {count} 条消息")
consumer.close()