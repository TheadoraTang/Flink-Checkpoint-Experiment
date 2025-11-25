package com.experiment;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Flink Checkpoint 实验的主作业。
 * 该作业从 LoopingSource 读取数据，按 LocationID 聚合，并根据命令行参数配置 Checkpoint 间隔。
 */
public class TaxiStreamJob {
    
    // Flink 作业主入口
    public static void main(String[] args) throws Exception {
        
        // --- 1. 环境初始化 ---
        // 确保 StreamExecutionEnvironment 在所有配置之前被初始化
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从参数获取 Checkpoint 间隔 (毫秒)，默认 10秒
        long checkpointInterval = args.length > 0 ? Long.parseLong(args[0]) : 10000;
        String dataPath = args.length > 1 ? args[1] : "/opt/data/nyc-taxi-2023-01.csv";

        // --- 2. 关键配置：Checkpoint & 监控 ---
        env.enableCheckpointing(checkpointInterval);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500); // Checkpoint 之间至少间隔 500ms
        env.getCheckpointConfig().setCheckpointTimeout(60000); // 1分钟超时
        // 设置 Checkpoint 存储路径 (对应 Docker 挂载的 /opt/flink/checkpoints)
        env.getCheckpointConfig().setCheckpointStorage("file:///opt/flink/checkpoints");
        
        // 即使作业被 Cancel 也保留 Checkpoint (方便调试和恢复)
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 开启 Latency 监控，每 2000ms 发送一次 Latency Marker
        env.getConfig().setLatencyTrackingInterval(2000); 

        // --- 3. 数据源 ---
        // 使用自定义循环 Source 来模拟无限流
        DataStream<TaxiRide> stream = env.addSource(new LoopingSource(dataPath))
                                         // 指定 Event Time 语义，并允许 10 秒的乱序
                                         .assignTimestampsAndWatermarks(
                                                org.apache.flink.api.common.eventtime.WatermarkStrategy.<TaxiRide>forBoundedOutOfOrderness(
                                                    java.time.Duration.ofSeconds(10))
                                                    .withTimestampAssigner((event, timestamp) -> event.eventTime)
                                         );

        // --- 4. 业务逻辑 ---
        // 按 PULocationID 分组，5秒滚动窗口，计算 累计金额 和 订单数
        stream
            .keyBy(TaxiRide::getPuLocationId)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .aggregate(new Aggregator())
            .name("Window Aggregation") // 命名算子，方便 REST API 监控
            .print()
            .name("Sink Output");

        // --- 5. 执行作业 ---
        env.execute("Taxi Checkpoint Experiment [Interval: " + checkpointInterval + "ms]");
    }
    
    // 聚合函数：计算累计收入和行程数量
    public static class Aggregator implements AggregateFunction<TaxiRide, Tuple3<Integer, Double, Long>, Tuple3<Integer, Double, Long>> {
        
        // 累加器: (LocationID, 累计金额, 订单数量)
        @Override
        public Tuple3<Integer, Double, Long> createAccumulator() { 
            return Tuple3.of(0, 0.0, 0L); 
        }
        
        @Override
        public Tuple3<Integer, Double, Long> add(TaxiRide value, Tuple3<Integer, Double, Long> accumulator) {
            // 确保 LocationID 被设置在累加器中
            int locationId = (accumulator.f0 == 0) ? value.puLocationId : accumulator.f0;
            return Tuple3.of(locationId, accumulator.f1 + value.totalAmount, accumulator.f2 + 1);
        }
        
        @Override
        public Tuple3<Integer, Double, Long> getResult(Tuple3<Integer, Double, Long> accumulator) { 
            return accumulator; 
        }
        
        @Override
        public Tuple3<Integer, Double, Long> merge(Tuple3<Integer, Double, Long> a, Tuple3<Integer, Double, Long> b) {
            // Merge 函数在 Session Window 或 Hybrid Window 中使用，Tumbling Window 中一般用不到
            return Tuple3.of(a.f0, a.f1 + b.f1, a.f2 + b.f2);
        }
    }
}
