package com.experiment;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class TaxiRevenueJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        long checkpointInterval = 30000L;
        if (args.length > 0) {
            checkpointInterval = Long.parseLong(args[0]);
        }

        // 配置检查点
        env.enableCheckpointing(checkpointInterval);
        env.getCheckpointConfig().setCheckpointTimeout(120000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10000);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(5);

        // 重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
            10, org.apache.flink.api.common.time.Time.seconds(15)
        ));

        env.setParallelism(3);

        System.out.println("==========================================");
        System.out.println("=== TAXI REVENUE STREAMING JOB STARTED ===");
        System.out.println("Checkpoint Interval: " + checkpointInterval + "ms");
        System.out.println("Parallelism: " + env.getParallelism());
        System.out.println("==========================================");

        // 创建持续数据源
        DataStream<TaxiEvent> taxiEvents = env
            .addSource(new ContinuousTaxiSource())
            .name("continuous-taxi-source")
            .setParallelism(1);

        // 数据处理
        DataStream<String> results = taxiEvents
            .keyBy(event -> event.puLocationId)
            .window(TumblingProcessingTimeWindows.of(
                org.apache.flink.streaming.api.windowing.time.Time.seconds(30)))
            .process(new TaxiRevenueFunction())
            .name("revenue-aggregation");

        // 输出结果
        results.print().name("result-output");

        System.out.println("Job configuration complete. Starting execution...");

        try {
            env.execute("Taxi Revenue Streaming - Checkpoint Experiment: " + checkpointInterval + "ms");
        } catch (Exception e) {
            System.err.println("Job execution failed: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }

    // 持续数据源 - 确保作业不会结束
    public static class ContinuousTaxiSource implements SourceFunction<TaxiEvent> {
        private volatile boolean isRunning = true;
        private List<TaxiEvent> eventPool;
        private Random random = new Random();
        private long eventsEmitted = 0;
        private long startTime = System.currentTimeMillis();

        public ContinuousTaxiSource() {
            eventPool = createEventPool();
            System.out.println("ContinuousTaxiSource initialized with " + eventPool.size() + " events");
        }

        @Override
        public void run(SourceContext<TaxiEvent> ctx) throws Exception {
            System.out.println("=== STARTING CONTINUOUS DATA EMISSION ===");

            while (isRunning) {
                // 从事件池中随机选择事件
                TaxiEvent event = eventPool.get(random.nextInt(eventPool.size()));

                // 创建新的事件实例，避免状态共享问题
                TaxiEvent newEvent = new TaxiEvent(
                    java.time.LocalDateTime.now(),
                    event.puLocationId,
                    event.totalAmount + random.nextDouble() * 5, // 添加一些随机变化
                    event.tripDistance + random.nextDouble() * 2,
                    event.passengerCount
                );

                ctx.collect(newEvent);
                eventsEmitted++;

                // 控制发射速率：每秒2-5个事件
                Thread.sleep(200 + random.nextInt(300));

                // 定期输出状态
                if (eventsEmitted % 50 == 0) {
                    long runningTime = (System.currentTimeMillis() - startTime) / 1000;
                    System.out.printf("Status: %d events emitted, running for %d seconds%n",
                                    eventsEmitted, runningTime);
                }

                // 每运行5分钟输出一次详细状态
                if (eventsEmitted % 1000 == 0) {
                    System.out.println("=== JOB IS RUNNING CONTINUOUSLY ===");
                    System.out.println("This job is designed to run indefinitely for checkpoint testing");
                    System.out.println("Use Flink Web UI or REST API to monitor checkpoint progress");
                }
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
            System.out.println("=== DATA SOURCE CANCELLED ===");
            System.out.println("Total events emitted: " + eventsEmitted);
        }

        private List<TaxiEvent> createEventPool() {
            List<TaxiEvent> pool = new ArrayList<>();
            TaxiEventParser parser = new TaxiEventParser();

            // 尝试从文件加载数据
            String[] filePaths = {
                "/opt/flink/data/nyc-taxi-2023-01.csv",
                "data/nyc-taxi-2023-01.csv"
            };

            boolean dataLoaded = false;

            for (String filePath : filePaths) {
                try {
                    System.out.println("Attempting to load data from: " + filePath);
                    List<String> lines = Files.readAllLines(Paths.get(filePath));
                    System.out.println("Found " + lines.size() + " lines in file");

                    int validEvents = 0;
                    for (int i = 1; i < lines.size() && i < 500; i++) {
                        try {
                            TaxiEvent event = parser.map(lines.get(i));
                            if (event != null) {
                                pool.add(event);
                                validEvents++;
                            }
                        } catch (Exception e) {
                            // 忽略解析错误
                        }
                    }

                    if (validEvents > 0) {
                        System.out.println("Successfully loaded " + validEvents + " events from file");
                        dataLoaded = true;
                        break;
                    }
                } catch (Exception e) {
                    System.out.println("Failed to load " + filePath + ": " + e.getMessage());
                }
            }

            // 如果文件加载失败，创建模拟数据
            if (!dataLoaded || pool.isEmpty()) {
                System.out.println("Creating simulated taxi data...");
                String[] locations = {"100", "101", "102", "103", "104", "105", "106", "107", "108", "109"};

                for (int i = 0; i < 200; i++) {
                    String location = locations[i % locations.length];
                    double amount = 8.0 + random.nextDouble() * 40.0;
                    double distance = 0.5 + random.nextDouble() * 15.0;
                    int passengers = 1 + random.nextInt(4);

                    pool.add(new TaxiEvent(
                        java.time.LocalDateTime.now().minusMinutes(random.nextInt(1000)),
                        location, amount, distance, passengers
                    ));
                }
                System.out.println("Created " + pool.size() + " simulated events");
            }

            return pool;
        }
    }
}