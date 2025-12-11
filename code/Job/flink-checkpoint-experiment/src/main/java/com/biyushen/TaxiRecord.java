package com.biyushen;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * NYC 出租车行程记录的 POJO 类。
 * 对应 NYC TLC Trip Record 数据集字段（2023 年格式）。
 * 必须包含无参构造函数以支持 Flink 的序列化/反序列化。
 */
public class TaxiRecord {

    // 字段顺序与 NYC CSV 一致（共 19 个字段）
    public int vendorID;
    public LocalDateTime pickupDatetime;
    public LocalDateTime dropoffDatetime;
    public int passengerCount;
    public double tripDistance;
    public int ratecodeID;
    public String storeAndFwdFlag;
    public int puLocationID;   // 上车区域 ID（用于 keyBy 分组）
    public int doLocationID;   // 下车区域 ID
    public int paymentType;
    public double fareAmount;
    public double extra;
    public double mtaTax;
    public double tipAmount;
    public double tollsAmount;
    public double improvementSurcharge;
    public double totalAmount; // 总费用（聚合目标）
    public double congestionSurcharge;
    public double airportFee;

    /**
     * 无参构造函数（Flink 反射要求，不可省略）
     */
    public TaxiRecord() {}

    /**
     * 全参构造函数（用于 fromCsvLine 内部创建对象）
     */
    public TaxiRecord(int vendorID, LocalDateTime pickupDatetime, LocalDateTime dropoffDatetime,
                      int passengerCount, double tripDistance, int ratecodeID, String storeAndFwdFlag,
                      int puLocationID, int doLocationID, int paymentType, double fareAmount,
                      double extra, double mtaTax, double tipAmount, double tollsAmount,
                      double improvementSurcharge, double totalAmount, double congestionSurcharge,
                      double airportFee) {
        this.vendorID = vendorID;
        this.pickupDatetime = pickupDatetime;
        this.dropoffDatetime = dropoffDatetime;
        this.passengerCount = passengerCount;
        this.tripDistance = tripDistance;
        this.ratecodeID = ratecodeID;
        this.storeAndFwdFlag = storeAndFwdFlag;
        this.puLocationID = puLocationID;
        this.doLocationID = doLocationID;
        this.paymentType = paymentType;
        this.fareAmount = fareAmount;
        this.extra = extra;
        this.mtaTax = mtaTax;
        this.tipAmount = tipAmount;
        this.tollsAmount = tollsAmount;
        this.improvementSurcharge = improvementSurcharge;
        this.totalAmount = totalAmount;
        this.congestionSurcharge = congestionSurcharge;
        this.airportFee = airportFee;
    }

    /**
     * 从单行 CSV 字符串解析为 TaxiRecord 对象。
     * 支持空值、"null" 字符串、带毫秒的时间戳（如 "2023-01-01 00:00:00.000"）。
     *
     * @param line 一行原始 CSV（不含换行符）
     * @return 解析后的 TaxiRecord 实例
     * @throws RuntimeException 解析失败时抛出（含原始行内容）
     */
    public static TaxiRecord fromCsvLine(String line) {
        if (line == null || line.trim().isEmpty()) {
            throw new IllegalArgumentException("Empty input line");
        }

        String[] fields = line.split(",", -1);
        if (fields.length < 19) {
            throw new IllegalArgumentException("CSV line has fewer than 19 fields: " + line);
        }

        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

            int vendorID = parseIntOrZero(fields[0]);
            LocalDateTime pickupDatetime = parseDateTime(fields[1], formatter);
            LocalDateTime dropoffDatetime = parseDateTime(fields[2], formatter);
            int passengerCount = parseIntOrZero(fields[3]);
            double tripDistance = parseDoubleOrZero(fields[4]);
            int ratecodeID = parseIntOrZero(fields[5]);
            String storeAndFwdFlag = fields[6].trim();
            int puLocationID = parseIntOrZero(fields[7]);
            int doLocationID = parseIntOrZero(fields[8]);
            int paymentType = parseIntOrZero(fields[9]);
            double fareAmount = parseDoubleOrZero(fields[10]);
            double extra = parseDoubleOrZero(fields[11]);
            double mtaTax = parseDoubleOrZero(fields[12]);
            double tipAmount = parseDoubleOrZero(fields[13]);
            double tollsAmount = parseDoubleOrZero(fields[14]);
            double improvementSurcharge = parseDoubleOrZero(fields[15]);
            double totalAmount = parseDoubleOrZero(fields[16]);
            double congestionSurcharge = parseDoubleOrZero(fields[17]);
            double airportFee = parseDoubleOrZero(fields[18]);

            return new TaxiRecord(
                    vendorID, pickupDatetime, dropoffDatetime,
                    passengerCount, tripDistance, ratecodeID, storeAndFwdFlag,
                    puLocationID, doLocationID, paymentType, fareAmount,
                    extra, mtaTax, tipAmount, tollsAmount,
                    improvementSurcharge, totalAmount, congestionSurcharge, airportFee
            );

        } catch (Exception e) {
            throw new RuntimeException("Failed to parse CSV line: " + line, e);
        }
    }

    // --- 辅助解析方法（私有，仅内部使用） ---

    private static int parseIntOrZero(String s) {
        if (s == null || s.trim().isEmpty() || "null".equalsIgnoreCase(s.trim())) {
            return 0;
        }
        try {
            return (int) Math.round(Double.parseDouble(s.trim()));
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private static double parseDoubleOrZero(String s) {
        if (s == null || s.trim().isEmpty() || "null".equalsIgnoreCase(s.trim())) {
            return 0.0;
        }
        try {
            return Double.parseDouble(s.trim());
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }

    private static LocalDateTime parseDateTime(String s, DateTimeFormatter formatter) {
        if (s == null || s.trim().isEmpty() || "null".equalsIgnoreCase(s.trim())) {
            return LocalDateTime.of(1970, 1, 1, 0, 0, 0); // 默认时间（避免 NPE）
        }
        try {
            return LocalDateTime.parse(s.trim(), formatter);
        } catch (DateTimeParseException e) {
            // 尝试截断毫秒部分（如 "2023-01-01 00:00:00.000" → "2023-01-01 00:00:00"）
            if (s.contains(".")) {
                String baseTime = s.substring(0, s.indexOf('.')).trim();
                return LocalDateTime.parse(baseTime, formatter);
            }
            throw e; // 无法解析则抛出异常
        }
    }
}