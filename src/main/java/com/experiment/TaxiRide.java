package com.experiment;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TaxiRide {
    public long eventTime;      // 解析后的时间戳
    public int puLocationId;
    public double totalAmount;
}