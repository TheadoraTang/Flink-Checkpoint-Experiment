package com.biyushen;

import org.apache.flink.configuration.Configuration; // 新增
import org.apache.flink.metrics.Meter;             // 新增
import org.apache.flink.metrics.MeterView;         // 新增
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction; // 修改导入
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

// 修改：改为继承 RichWindowFunction
public class HourlyAggregator extends RichWindowFunction<TaxiRecord, String, Integer, TimeWindow> {

    private transient Meter outputMeter; // 新增 Metric

    private static final DateTimeFormatter windowFormatter = DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss")
            .withZone(ZoneId.of("UTC"));

    // 新增：初始化 Metrics
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.outputMeter = getRuntimeContext()
                .getMetricGroup()
                .meter("window_throughput", new MeterView(5));
    }

    @Override
    public void apply(
            Integer puLocationID,
            TimeWindow window,
            Iterable<TaxiRecord> records,
            Collector<String> out) {

        long tripCount = 0;
        double totalIncome = 0.0;

        for (TaxiRecord record : records) {
            tripCount++;
            totalIncome += record.getTotalAmount();
        }

        String windowStart = windowFormatter.format(Instant.ofEpochMilli(window.getStart()));
        String windowEnd = windowFormatter.format(Instant.ofEpochMilli(window.getEnd()));
        String result = String.format(
                "窗口时段：%s ~ %s, 上车地点ID：%d, 行程总数：%d, 总收益：%.2f 美元",
                windowStart, windowEnd, puLocationID, tripCount, totalIncome
        );

        out.collect(result);

        // 新增：记录输出
        if (outputMeter != null) {
            outputMeter.markEvent();
        }
    }
}