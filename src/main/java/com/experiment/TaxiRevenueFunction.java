package com.experiment;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class TaxiRevenueFunction extends ProcessWindowFunction<TaxiEvent, String, String, TimeWindow> {
    
    private transient ValueState<Double> totalRevenueState;
    private transient ValueState<Integer> tripCountState;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Double> revenueDescriptor = 
            new ValueStateDescriptor<>("totalRevenue", Double.class);
        totalRevenueState = getRuntimeContext().getState(revenueDescriptor);
        
        ValueStateDescriptor<Integer> countDescriptor = 
            new ValueStateDescriptor<>("tripCount", Integer.class);
        tripCountState = getRuntimeContext().getState(countDescriptor);
    }
    
    @Override
    public void process(String locationId, Context context, 
                       Iterable<TaxiEvent> events, Collector<String> out) throws Exception {
        
        // Initialize or get current state
        Double currentRevenue = totalRevenueState.value();
        Integer currentCount = tripCountState.value();
        
        if (currentRevenue == null) currentRevenue = 0.0;
        if (currentCount == null) currentCount = 0;
        
        // Process events in this window
        double windowRevenue = 0.0;
        int windowTrips = 0;
        
        for (TaxiEvent event : events) {
            windowRevenue += event.totalAmount;
            windowTrips++;
        }
        
        // Update state
        double newRevenue = currentRevenue + windowRevenue;
        int newCount = currentCount + windowTrips;
        
        totalRevenueState.update(newRevenue);
        tripCountState.update(newCount);
        
        // Output results
        String result = String.format("Location: %s | Total Revenue: $%.2f | Total Trips: %d | Window: %s", 
                                    locationId, newRevenue, newCount, context.window());
        out.collect(result);
        
        System.out.println("Processed: " + result);
    }
}