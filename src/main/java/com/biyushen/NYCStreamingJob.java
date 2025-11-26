package com.biyushen;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;

public class NYCStreamingJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        env.enableCheckpointing(30000);
        env.getCheckpointConfig().setCheckpointStorage("file:///tmp/flink-checkpoints");


        String csvPath = args.length > 0 ? args[0] : "data/nyc-taxi-data.csv";
        int timeMultiplier = args.length > 1 ? Integer.parseInt(args[1]) : 60;

        DataStream<TaxiRecord> taxiStream = env.addSource(new TaxiSourceFunction(csvPath, timeMultiplier))
                .filter(record ->
                        record.getPuLocationID() > 0
                                && record.getTotalAmount() >= 0
                                && record.getPickupDatetime() != null
                                && record.getPassengerCount() >= 1
                                && record.getTripDistance() >= 0
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TaxiRecord>forMonotonousTimestamps()
                                .withTimestampAssigner((record, timestamp) ->
                                        record.getPickupDatetime().toEpochSecond(java.time.ZoneOffset.UTC) * 1000
                                )
                );

        DataStream<String> aggregatedStream = taxiStream
                .keyBy(TaxiRecord::getPuLocationID)
                .window(TumblingEventTimeWindows.of(Time.minutes(3)))
                .apply(new HourlyAggregator());

        // 5. 修复文件输出：显式指定 <String> 泛型
        StreamingFileSink<String> fileSink = StreamingFileSink
                .<String>forRowFormat( // 关键修复：添加 <String> 显式泛型
                        new Path("output/nyc-taxi-aggregates"),
                        new SimpleStringEncoder<>("UTF-8")
                )
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofHours(1))
                                .withMaxPartSize(128 * 1024 * 1024)
                                .withInactivityInterval(Duration.ofMinutes(10))
                                .build()
                )
                // 建议：将路径从按小时 (HH) 细化到按分钟 (mm)，便于查找
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd/HH/mm"))
                .build();

        // 6. 绑定Sink并执行（不变）
        aggregatedStream.addSink(fileSink).name("Taxi-Aggregate-File-Sink");
        env.execute("NYC-Taxi-Infinite-Streaming-Aggregation");
    }
}