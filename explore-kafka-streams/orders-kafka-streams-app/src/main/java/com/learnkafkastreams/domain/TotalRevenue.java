package com.learnkafkastreams.domain;

import java.math.BigDecimal;

public record TotalRevenue(
    String locationId, Integer runnuingOrderCount, BigDecimal runningRevenue) {

  public TotalRevenue() {
    this("null", 0, BigDecimal.valueOf(0.0));
  }

  public TotalRevenue updateRunningRevenue(String key, Order order) {
    Integer newRunningOrderCount = this.runnuingOrderCount + 1;
    BigDecimal newRunningRevenue = this.runningRevenue.add(order.finalAmount());
    TotalRevenue newTotalRevenue = new TotalRevenue(key, newRunningOrderCount, newRunningRevenue);
    return newTotalRevenue;
  }
}
