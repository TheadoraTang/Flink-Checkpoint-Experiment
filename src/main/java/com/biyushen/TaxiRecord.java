package com.biyushen;

import java.time.LocalDateTime;

/**
 * 出租车行程数据模型（匹配19个字段的构造函数，含所有必要getter）
 */
public class TaxiRecord {
    // 纽约市出租车CSV标准字段（19个）
    private int vendorID;
    private LocalDateTime pickupDatetime;
    private LocalDateTime dropoffDatetime;
    private int puLocationID;
    private double tripDistance;
    private int passengerCount;
    private String rateCodeID;
    private int storeAndFwdFlag;
    private int doLocationID;
    private int paymentType;
    private double fareAmount;
    private double extra;
    private double mtaTax;
    private double tipAmount;
    private double tollsAmount;
    private double improvementSurcharge;
    private double totalAmount;
    private double congestionSurcharge;
    private double airportFee;

    // 完整构造函数（19个参数，与CSV字段顺序对应）
    public TaxiRecord(int vendorID, LocalDateTime pickupDatetime, LocalDateTime dropoffDatetime,
                      int puLocationID, double tripDistance, int passengerCount, String rateCodeID,
                      int storeAndFwdFlag, int doLocationID, int paymentType, double fareAmount,
                      double extra, double mtaTax, double tipAmount, double tollsAmount,
                      double improvementSurcharge, double totalAmount, double congestionSurcharge,
                      double airportFee) {
        this.vendorID = vendorID;
        this.pickupDatetime = pickupDatetime;
        this.dropoffDatetime = dropoffDatetime;
        this.puLocationID = puLocationID;
        this.tripDistance = tripDistance;
        this.passengerCount = passengerCount;
        this.rateCodeID = rateCodeID;
        this.storeAndFwdFlag = storeAndFwdFlag;
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

    public int getVendorID() {
        return vendorID;
    }

    public LocalDateTime getPickupDatetime() {
        return pickupDatetime;
    }

    public LocalDateTime getDropoffDatetime() {
        return dropoffDatetime;
    }

    public int getPuLocationID() {
        return puLocationID;
    }

    public double getTripDistance() {
        return tripDistance;
    }

    public int getPassengerCount() {
        return passengerCount;
    }

    public double getTotalAmount() {
        return totalAmount;
    }

    // 其他字段getter（按需保留，不影响核心逻辑）
    public int getDoLocationID() {
        return doLocationID;
    }

    public double getFareAmount() {
        return fareAmount;
    }

    public double getTipAmount() {
        return tipAmount;
    }
}