package com.experiment;

import org.apache.flink.api.common.functions.MapFunction;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class TaxiEventParser implements MapFunction<String, TaxiEvent> {
    private static final DateTimeFormatter formatter = 
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    
    @Override
    public TaxiEvent map(String line) throws Exception {
        if (line == null || line.startsWith("VendorID")) {
            return null; // Skip header
        }
        
        String[] fields = line.split(",");
        
        try {
            LocalDateTime pickupTime = LocalDateTime.parse(fields[1], formatter);
            String puLocationId = fields[7]; // PULocationID
            double totalAmount = Double.parseDouble(fields[16]); // Total_amount
            double tripDistance = Double.parseDouble(fields[4]); // Trip_distance
            int passengerCount = Integer.parseInt(fields[3]); // Passenger_count
            
            return new TaxiEvent(pickupTime, puLocationId, totalAmount, tripDistance, passengerCount);
        } catch (Exception e) {
            System.err.println("Error parsing line: " + line);
            return null;
        }
    }
}