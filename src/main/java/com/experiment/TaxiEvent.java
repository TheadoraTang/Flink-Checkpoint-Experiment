package com.experiment;

import java.time.LocalDateTime;

public class TaxiEvent {
    public LocalDateTime tpepPickupDatetime;
    public String puLocationId;
    public double totalAmount;
    public double tripDistance;
    public int passengerCount;
    
    public TaxiEvent() {}
    
    public TaxiEvent(LocalDateTime tpepPickupDatetime, String puLocationId, 
                    double totalAmount, double tripDistance, int passengerCount) {
        this.tpepPickupDatetime = tpepPickupDatetime;
        this.puLocationId = puLocationId;
        this.totalAmount = totalAmount;
        this.tripDistance = tripDistance;
        this.passengerCount = passengerCount;
    }
    
    @Override
    public String toString() {
        return String.format("TaxiEvent{time=%s, location=%s, amount=%.2f, distance=%.1f, passengers=%d}",
                           tpepPickupDatetime, puLocationId, totalAmount, tripDistance, passengerCount);
    }
}