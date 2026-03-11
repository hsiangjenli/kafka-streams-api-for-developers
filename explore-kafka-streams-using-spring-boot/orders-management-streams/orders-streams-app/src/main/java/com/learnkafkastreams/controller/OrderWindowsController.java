package com.learnkafkastreams.controller;

import com.learnkafkastreams.domain.OrdersCountPerStoreByWindowsDTO;
import com.learnkafkastreams.domain.OrdersRevenuePerStoreByWindowsDTO;
import com.learnkafkastreams.service.OrderWindowsService;
import java.time.LocalDateTime;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1/orders/")
public class OrderWindowsController {

  @Autowired private OrderWindowsService orderWindowsService;

  @GetMapping("/windows/count/{order_type}")
  public ResponseEntity<List<OrdersCountPerStoreByWindowsDTO>> getOrderCount(
      @PathVariable("order_type") String orderType) {
    return ResponseEntity.ok(orderWindowsService.getOrderCountWindowsByType(orderType));
  }

  @GetMapping("/windows/count")
  public ResponseEntity<List<OrdersCountPerStoreByWindowsDTO>> getAllOrderCountWindows(
      @RequestParam(value = "from_time", required = false)
          @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
          LocalDateTime fromTime,
      @RequestParam(value = "to_time", required = false)
          @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
          LocalDateTime toTime) {

    if (fromTime != null && toTime != null) {
      return ResponseEntity.ok(orderWindowsService.getAllOrderCountWindows(fromTime, toTime));
    }

    return ResponseEntity.ok(orderWindowsService.getAllOrderCountWindows());
  }

  @GetMapping("/windows/revenue/{order_type}")
  public ResponseEntity<List<OrdersRevenuePerStoreByWindowsDTO>> getOrderRevenue(
      @PathVariable("order_type") String orderType) {
    return ResponseEntity.ok(orderWindowsService.getOrderRevenueWindowsByType(orderType));
  }
}
