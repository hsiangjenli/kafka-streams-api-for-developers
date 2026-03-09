package com.learnkafkastreams.controller;

import com.learnkafkastreams.domain.OrderCountPerStoreDTO;
import com.learnkafkastreams.service.OrderService;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1/orders")
public class OrderController {

  @Autowired
  private OrderService orderService;

  @GetMapping("/count/{order_type}")
  public List<OrderCountPerStoreDTO> getOrdersCount(@PathVariable("order_type") String orderType) {
    return orderService.getOrdersCount(orderType);
  }
}
