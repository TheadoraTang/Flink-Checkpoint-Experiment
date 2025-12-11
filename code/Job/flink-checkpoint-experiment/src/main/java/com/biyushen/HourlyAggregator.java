package com.biyushen;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * 按 PULocationID 分组的 1 小时滚动窗口聚合器。
 * 计算每个区域每小时的：
 * - 行程数量（TripCount）
 * - 总收入（TotalIncome = Σ totalAmount）
 */
public class HourlyAggregator extends ProcessWindowFunction<TaxiRecord, String, Integer, TimeWindow> {

    /**
     * 对窗口内所有 TaxiRecord 进行聚合。
     *
     * @param puLocationID 窗口分组键（上车区域 ID）
     * @param context      窗口上下文（可获取窗口边界等信息）
     * @param elements     窗口内所有元素的迭代器
     * @param out          输出收集器
     */
    @Override
    public void process(Integer puLocationID,
                        Context context,
                        Iterable<TaxiRecord> elements,
                        Collector<String> out) throws Exception {
        long count = 0;
        double totalIncome = 0.0;

        for (TaxiRecord record : elements) {
            count++;
            totalIncome += record.totalAmount;
        }

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

        // 格式化输出：便于后续分析
        String result = String.format(
                "PULocationID=%d, WindowStart=%s, WindowEnd=%s, TripCount=%d, TotalIncome=%.2f",
                puLocationID,
                formatter.format(Instant.ofEpochMilli(context.window().getStart())),
                formatter.format(Instant.ofEpochMilli(context.window().getEnd())),
                count,
                totalIncome
        );

        out.collect(result);
    }
}