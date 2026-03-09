package com.learnkafkastreams.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import com.learnkafkastreams.domain.OrdersCountPerStoreByWindowsDTO;
import com.learnkafkastreams.service.OrderWindowsService;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;


@RestController
@RequestMapping("/v1/orders/")
public class OrderWindowsController {

  @Autowired
  private OrderWindowsService orderWindowsService;

  @GetMapping("/windows/count/{order_type}")
  public ResponseEntity<List<OrdersCountPerStoreByWindowsDTO>> getOrderCount(
      @PathVariable("order_type") String orderType) {
    return ResponseEntity.ok(orderWindowsService.getOrderCountWindowsByType(orderType));
  }

  @GetMapping("/windows/count")
  public ResponseEntity<List<OrdersCountPerStoreByWindowsDTO>> getAllOrderCountWindows() {
    return ResponseEntity.ok(orderWindowsService.getAllOrderCountWindows());
  }

}
