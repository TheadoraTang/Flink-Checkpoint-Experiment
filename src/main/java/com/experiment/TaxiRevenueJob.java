package com.experiment;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class TaxiRevenueJob {
    
    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Checkpoint configuration - this will be overridden by flink-conf.yaml
        env.enableCheckpointing(30000); // 30 seconds default
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        
        // Restart strategy
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
            3, Time.seconds(10)
        ));
        
        // Read from CSV file
        DataStream<String> input = env.readTextFile("file:///opt/flink/data/nyc-taxi-2023-01.csv");
        
        // Parse CSV lines
        DataStream<TaxiEvent> taxiEvents = input
            .map(new TaxiEventParser())
            .filter(event -> event != null);
        
        // Assign watermarks and process
        DataStream<TaxiEvent> timedEvents = taxiEvents
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<TaxiEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((event, timestamp) -> 
                    event.tpepPickupDatetime.atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli()
                )
            );
        
        // Key by location and apply tumbling window
        DataStream<String> results = timedEvents
            .keyBy(event -> event.puLocationId)
            .window(TumblingEventTimeWindows.of(Time.minutes(10)))
            .process(new TaxiRevenueFunction());
        
        // Print results for monitoring
        results.print().setParallelism(1);
        
        // Execute job
        env.execute("Taxi Revenue Analysis with Checkpoint Experiment");
    }
}