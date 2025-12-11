package com.biyushen;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.time.Duration;
import java.util.Properties;

/**
 * NYC 出租车数据流处理作业（基于 Kafka 输入）。
 * 功能：
 *   - 从 Kafka topic "nyc-taxi-raw" 读取 CSV 行
 *   - 按 pickupDatetime 提取事件时间
 *   - 按 PULocationID 分组，1 小时滚动窗口聚合
 *   - 结果写入文件系统
 *
 * 优势：支持容错、Exactly-Once、分布式部署。
 */
public class NYCStreamingJob {

    public static void main(String[] args) throws Exception {
        // === 配置参数（硬编码，便于实验对比）===
        final int PARALLELISM = 8;                   // 并行度
        final long CHECKPOINT_INTERVAL = 30_000L;    // Checkpoint 间隔（30 秒）

        String jobName = String.format(
                "NYC Taxi - Large State Job (checkpoint_interval=%d)",
                CHECKPOINT_INTERVAL
        );

        // === 初始化执行环境 ===
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        env.enableCheckpointing(CHECKPOINT_INTERVAL); // Exactly-Once by default

        // === 配置 Kafka Consumer ===
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "<hostname_or_ip>:29092");
        kafkaProps.setProperty("group.id", "nyc-taxi-flink-group");
        kafkaProps.setProperty("auto.offset.reset", "earliest"); // 首次运行时从头开始
        kafkaProps.setProperty("enable.auto.commit", "false");  // Flink 自己管理 offset

        // 可选：确保从最早 offset 开始（首次运行时）
        // kafkaProps.setProperty("auto.offset.reset", "earliest");

        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>(
                "nyc-taxi-raw",           // topic 名称
                new org.apache.flink.api.common.serialization.SimpleStringSchema(),
                kafkaProps
        );

        // 关键：从 checkpoint 中恢复 offset（默认行为），实现容错
        kafkaSource.setStartFromGroupOffsets(); // 默认行为，可省略

        // === 数据源：从 Kafka 读取文本流 ===
        DataStream<String> textStream = env.addSource(kafkaSource).name("KafkaSource");

        // === 转换：字符串 → TaxiRecord + 事件时间分配 ===
        DataStream<TaxiRecord> stream = textStream
                .map(TaxiRecord::fromCsvLine)
                .returns(TaxiRecord.class)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TaxiRecord>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                                .withTimestampAssigner((event, timestamp) ->
                                        event.pickupDatetime.atZone(java.time.ZoneId.of("UTC")).toInstant().toEpochMilli()
                                )
                );

        // === 窗口聚合：按 PULocationID 分组，1 小时滚动窗口 ===
        DataStream<String> result = stream
                .keyBy(record -> record.puLocationID)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .process(new HourlyAggregator());

        // === 输出：写入文件系统（行格式）===
        StreamingFileSink<String> sink = StreamingFileSink
                .forRowFormat(
                        new Path("s3a://flink/output/nyc-taxi"),
                        new SimpleStringEncoder<String>("UTF-8")
                )
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withMaxPartSize(128 * 1024 * 1024)
                                .withRolloverInterval(Time.minutes(1).toMilliseconds())
                                .build()
                )
                .build();

        result.addSink(sink).name("FileSink");

        // === 启动作业 ===
        env.execute(jobName);
    }
}
