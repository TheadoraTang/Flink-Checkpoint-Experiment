package com.experiment;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

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
        
        // 创建数据源 - 使用原始数据集循环读取
        DataStream<TaxiEvent> taxiEvents = env
            .addSource(new FileCyclingSource())
            .name("file-cycling-source")
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
    
    // 文件循环读取数据源 - 完全可序列化
    public static class FileCyclingSource implements SourceFunction<TaxiEvent> {
        private volatile boolean isRunning = true;
        private long eventsEmitted = 0;
        private long startTime = System.currentTimeMillis();
        
        // 只包含可序列化的字段
        private final List<String> filePaths;
        
        public FileCyclingSource() {
            // 初始化文件路径列表
            this.filePaths = new ArrayList<>();
            filePaths.add("/opt/flink/data/nyc-taxi-2023-01.csv");
            filePaths.add("data/nyc-taxi-2023-01.csv");
            System.out.println("FileCyclingSource initialized");
        }
        
        @Override
        public void run(SourceContext<TaxiEvent> ctx) throws Exception {
            System.out.println("=== STARTING FILE CYCLING DATA SOURCE ===");
            
            TaxiEventParser parser = new TaxiEventParser();
            List<TaxiEvent> eventPool = new ArrayList<>();
            
            // 加载数据到内存
            boolean dataLoaded = false;
            for (String filePath : filePaths) {
                try {
                    System.out.println("Loading data from: " + filePath);
                    List<String> lines = Files.readAllLines(Paths.get(filePath));
                    System.out.println("Found " + lines.size() + " lines in file");
                    
                    int validEvents = 0;
                    for (int i = 1; i < lines.size() && i < 1000; i++) {
                        try {
                            TaxiEvent event = parser.map(lines.get(i));
                            if (event != null) {
                                eventPool.add(event);
                                validEvents++;
                            }
                        } catch (Exception e) {
                            // 忽略解析错误
                        }
                    }
                    
                    if (validEvents > 0) {
                        System.out.println("Successfully loaded " + validEvents + " events");
                        dataLoaded = true;
                        break;
                    }
                } catch (Exception e) {
                    System.out.println("Failed to load " + filePath + ": " + e.getMessage());
                }
            }
            
            if (!dataLoaded || eventPool.isEmpty()) {
                System.out.println("No data loaded, creating sample data");
                // 创建简单的样本数据
                for (int i = 0; i < 100; i++) {
                    eventPool.add(new TaxiEvent(
                        LocalDateTime.now(),
                        String.valueOf(100 + (i % 10)),
                        10.0 + (i % 30),
                        1.0 + (i % 10),
                        1 + (i % 4)
                    ));
                }
            }
            
            System.out.println("Starting to cycle through " + eventPool.size() + " events");
            
            int currentIndex = 0;
            
            while (isRunning) {
                // 循环发送事件
                TaxiEvent event = eventPool.get(currentIndex);
                ctx.collect(event);
                eventsEmitted++;
                
                currentIndex = (currentIndex + 1) % eventPool.size();
                
                // 控制发射速率
                Thread.sleep(200); // 每秒5个事件
                
                // 定期输出状态
                if (eventsEmitted % 50 == 0) {
                    long runningTime = (System.currentTimeMillis() - startTime) / 1000;
                    System.out.printf("FileCycling: %d events emitted, running for %d seconds, index: %d/%d%n", 
                                    eventsEmitted, runningTime, currentIndex, eventPool.size());
                }
                
                // 每运行一段时间输出详细状态
                if (eventsEmitted % 500 == 0) {
                    System.out.println("=== CONTINUOUS DATA STREAM RUNNING ===");
                    System.out.println("Cycling through real dataset for checkpoint testing");
                }
            }
        }
        
        @Override
        public void cancel() {
            isRunning = false;
            System.out.println("=== FILE CYCLING SOURCE CANCELLED ===");
            System.out.println("Total events emitted: " + eventsEmitted);
        }
    }
}
