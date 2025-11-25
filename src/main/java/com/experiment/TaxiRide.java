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

    // 新增：解析CSV行的静态方法
    public static TaxiRide fromString(String line) {
        String[] fields = line.split(",");
        // 假设CSV格式为：... puLocationId, totalAmount, ...（根据实际数据调整索引）
        // 这里以第7列为puLocationId，第16列为totalAmount为例（需根据实际CSV调整）
        int puLoc = Integer.parseInt(fields[7]);
        double amount = Double.parseDouble(fields[16]);
        return new TaxiRide(0, puLoc, amount); // eventTime暂时设为0，后续会被覆盖
    }
}