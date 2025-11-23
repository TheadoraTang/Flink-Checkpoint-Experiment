package com.experiment;

import org.apache.flink.api.common.functions.MapFunction;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class TaxiEventParser implements MapFunction<String, TaxiEvent> {
    
    // 支持多种时间格式
    private static final DateTimeFormatter formatter1 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter formatter2 = DateTimeFormatter.ofPattern("yyyy/M/d H:m");
    
    @Override
    public TaxiEvent map(String line) throws Exception {
        if (line == null || line.trim().isEmpty() || line.startsWith("VendorID")) {
            return null;
        }
        
        // 使用逗号分割字段
        String[] fields = line.split(",", -1);
        
        // 检查字段数量
        if (fields.length < 17) {
            System.err.println("Invalid line - not enough fields (" + fields.length + ")");
            return null;
        }
        
        try {
            // 根据你的数据格式解析字段
            // VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,RatecodeID,store_and_fwd_flag,PULocationID,DOLocationID,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,congestion_surcharge,airport_fee
            String pickupTimeStr = fields[1].trim();
            String passengerCountStr = fields[3].trim();
            String tripDistanceStr = fields[4].trim();
            String puLocationId = fields[7].trim();
            String totalAmountStr = fields[16].trim();
            
            if (pickupTimeStr.isEmpty() || puLocationId.isEmpty() || totalAmountStr.isEmpty()) {
                return null;
            }
            
            // 解析时间
            LocalDateTime pickupTime = parseDateTime(pickupTimeStr);
            if (pickupTime == null) {
                return null;
            }
            
            // 解析数值
            double totalAmount = parseDoubleSafely(totalAmountStr);
            double tripDistance = parseDoubleSafely(tripDistanceStr);
            int passengerCount = parseIntSafely(passengerCountStr);
            
            // 验证数据
            if (totalAmount <= 0 || tripDistance < 0 || passengerCount <= 0) {
                return null;
            }
            
            return new TaxiEvent(pickupTime, puLocationId, totalAmount, tripDistance, passengerCount);
            
        } catch (Exception e) {
            return null;
        }
    }
    
    private LocalDateTime parseDateTime(String datetimeStr) {
        try {
            return LocalDateTime.parse(datetimeStr, formatter1);
        } catch (Exception e1) {
            try {
                return LocalDateTime.parse(datetimeStr, formatter2);
            } catch (Exception e2) {
                return null;
            }
        }
    }
    
    private double parseDoubleSafely(String value) {
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }
    
    private int parseIntSafely(String value) {
        try {
            if (value.contains(".")) {
                return (int) Double.parseDouble(value);
            }
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return 1;
        }
    }
}
