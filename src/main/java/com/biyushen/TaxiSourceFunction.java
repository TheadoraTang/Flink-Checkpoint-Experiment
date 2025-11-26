package com.biyushen;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * 自定义Flink数据源：循环读取CSV文件，模拟无限流，解析时过滤空值/无效数据
 * 修改为继承 RichSourceFunction 以支持 Metrics 监控
 */
public class TaxiSourceFunction extends RichSourceFunction<TaxiRecord> { // 继承 RichSourceFunction
    private final String csvPath; //
    private final int timeMultiplier; //
    private volatile boolean isRunning = true; //

    // 新增：吞吐量监控指标
    private transient Meter throughputMeter; //

    // 关键修改：使用 ThreadLocal 包装 DateTimeFormatter（避免序列化时触发权限问题）
    private static final ThreadLocal<DateTimeFormatter> DT_FORMATTER = ThreadLocal.withInitial(
            () -> DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    ); //
    private long lastEventTimestamp = -1; //

    // 构造函数：接收CSV路径和时间倍率
    public TaxiSourceFunction(String csvPath, int timeMultiplier) {
        this.csvPath = csvPath; //
        this.timeMultiplier = timeMultiplier; //
    }

    // 新增：初始化 Metrics
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.throughputMeter = getRuntimeContext()
                .getMetricGroup()
                .meter("source_throughput", new MeterView(5)); //
    }

    @Override
    public void run(SourceContext<TaxiRecord> ctx) throws Exception {
        // 无限循环读取CSV（实现无限流）
        while (isRunning) { //
            try (BufferedReader reader = new BufferedReader(new FileReader(csvPath))) { //
                String line; //
                boolean isFirstLine = true; // 跳过CSV表头

                while (isRunning && (line = reader.readLine()) != null) { //
                    if (isFirstLine || line.trim().isEmpty()) { //
                        isFirstLine = false; //
                        continue; //
                    }

                    TaxiRecord record = parseCsvLine(line); //
                    if (record == null) { //
                        continue; //
                    }

                    controlEmissionSpeed(record); //

                    long currentTimestamp = record.getPickupDatetime().toEpochSecond(java.time.ZoneOffset.UTC) * 1000; //
                    ctx.collectWithTimestamp(record, currentTimestamp); //
                    ctx.emitWatermark(new Watermark(currentTimestamp - 1000)); //

                    if (throughputMeter != null) {
                        throughputMeter.markEvent(); //
                    }

                    lastEventTimestamp = currentTimestamp; //
                }
            } catch (IOException e) {
                // CSV读取失败（如文件被删除、权限不足），5秒后重试
                System.err.println("CSV文件读取异常，5秒后重试：" + e.getMessage()); //
                Thread.sleep(5000); //
            }
        }
    }

    private TaxiRecord parseCsvLine(String line) {
        String[] fields = line.split(","); //

        // 校验字段数量（至少19个，避免数组越界）
        if (fields.length < 19) { //
            System.err.println("跳过字段数量不足的行：" + line); //
            return null; //
        }

        try {
            // 安全解析每个字段（空值/格式错误返回默认无效值）
            int vendorID = parseSafeInt(fields[0].trim()); //
            LocalDateTime pickupDatetime = parseSafeDateTime(fields[1].trim()); //
            LocalDateTime dropoffDatetime = parseSafeDateTime(fields[2].trim()); //
            int puLocationID = parseSafeInt(fields[3].trim()); //
            double tripDistance = parseSafeDouble(fields[4].trim()); //
            int passengerCount = parseSafeInt(fields[5].trim()); //
            String rateCodeID = fields[6].trim().isEmpty() ? "0" : fields[6].trim(); //
            int storeAndFwdFlag = parseSafeInt(fields[7].trim()); //
            int doLocationID = parseSafeInt(fields[8].trim()); //
            int paymentType = parseSafeInt(fields[9].trim()); //
            double fareAmount = parseSafeDouble(fields[10].trim()); //
            double extra = parseSafeDouble(fields[11].trim()); //
            double mtaTax = parseSafeDouble(fields[12].trim()); //
            double tipAmount = parseSafeDouble(fields[13].trim()); //
            double tollsAmount = parseSafeDouble(fields[14].trim()); //
            double improvementSurcharge = parseSafeDouble(fields[15].trim()); //
            double totalAmount = parseSafeDouble(fields[16].trim()); //
            double congestionSurcharge = parseSafeDouble(fields[17].trim()); //
            double airportFee = parseSafeDouble(fields[18].trim()); //

            if (pickupDatetime == null || dropoffDatetime == null
                    || puLocationID <= 0 || passengerCount < 1
                    || totalAmount < 0 || tripDistance < 0) { //
                System.err.println("跳过无效数据行：" + line); //
                return null; //
            }

            return new TaxiRecord(
                    vendorID, pickupDatetime, dropoffDatetime,
                    puLocationID, tripDistance, passengerCount, rateCodeID,
                    storeAndFwdFlag, doLocationID, paymentType, fareAmount,
                    extra, mtaTax, tipAmount, tollsAmount,
                    improvementSurcharge, totalAmount, congestionSurcharge,
                    airportFee
            ); //
        } catch (Exception e) {
            System.err.println("解析CSV行失败，跳过：" + line + "，异常：" + e.getMessage()); //
            return null; //
        }
    }

    private int parseSafeInt(String value) {
        if (value.isEmpty() || "null".equalsIgnoreCase(value)) { //
            return -1; //
        }
        try {
            return Integer.parseInt(value); //
        } catch (NumberFormatException e) {
            return -1; //
        }
    }


    private double parseSafeDouble(String value) {
        if (value.isEmpty() || "null".equalsIgnoreCase(value)) { //
            return -1.0; //
        }
        try {
            return Double.parseDouble(value); //
        } catch (NumberFormatException e) {
            return -1.0; //
        }
    }


    private LocalDateTime parseSafeDateTime(String value) {
        if (value.isEmpty() || "null".equalsIgnoreCase(value)) { //
            return null; //
        }
        try {
            // 关键修改：从 ThreadLocal 中获取 formatter，而非成员变量
            return LocalDateTime.parse(value, DT_FORMATTER.get()); //
        } catch (DateTimeParseException e) {
            return null; //
        }
    }


    private void controlEmissionSpeed(TaxiRecord record) throws InterruptedException {
        long currentTimestamp = record.getPickupDatetime().toEpochSecond(java.time.ZoneOffset.UTC) * 1000; //

        // 第一条数据无需休眠
        if (lastEventTimestamp == -1) { //
            return; //
        }

        // 计算两条数据的事件时间差（秒）
        long eventTimeDiff = (currentTimestamp - lastEventTimestamp) / 1000; //
        // 按倍率计算实际休眠时间（加速流处理）
        long sleepTime = eventTimeDiff * 1000 / timeMultiplier; //

        // 休眠控制发射速度（避免数据瞬间发送完毕）
        if (sleepTime > 0) { //
            Thread.sleep(sleepTime); //
        }
    }

    @Override
    public void cancel() {
        isRunning = false; //
    }
}