package com.experiment;

import org.apache.flink.api.common.functions.MapFunction;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class TaxiEventParser implements MapFunction<String, TaxiEvent> {

    private static final DateTimeFormatter formatter =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Override
    public TaxiEvent map(String line) throws Exception {
        if (line == null || line.trim().isEmpty() || line.startsWith("VendorID")) {
            return null;
        }

        String[] fields = line.split(",", -1);

        if (fields.length < 17) {
            System.err.println("Invalid line - not enough fields (" + fields.length + ")");
            return null;
        }

        try {
            String pickupTimeStr = fields[1].trim();
            String passengerCountStr = fields[3].trim();
            String tripDistanceStr = fields[4].trim();
            String puLocationId = fields[7].trim();
            String totalAmountStr = fields[16].trim();

            if (pickupTimeStr.isEmpty() || puLocationId.isEmpty() || totalAmountStr.isEmpty()) {
                return null;
            }

            LocalDateTime pickupTime = LocalDateTime.parse(pickupTimeStr, formatter);
            double totalAmount = parseDoubleSafely(totalAmountStr);
            double tripDistance = parseDoubleSafely(tripDistanceStr);
            int passengerCount = parseIntSafely(passengerCountStr);

            if (totalAmount <= 0 || tripDistance < 0 || passengerCount <= 0) {
                return null;
            }

            return new TaxiEvent(pickupTime, puLocationId, totalAmount, tripDistance, passengerCount);

        } catch (Exception e) {
            System.err.println("Error parsing line: " + e.getMessage());
            return null;
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