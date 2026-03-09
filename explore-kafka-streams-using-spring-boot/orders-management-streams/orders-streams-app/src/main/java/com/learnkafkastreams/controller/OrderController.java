package com.learnkafkastreams.controller;

import com.learnkafkastreams.domain.OrderCountPerStoreDTO;
import com.learnkafkastreams.service.OrderService;
import io.micrometer.common.util.StringUtils;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;


@RestController
@RequestMapping("/v1/orders")
public class OrderController {

  @Autowired
  private OrderService orderService;

  @GetMapping("/count/{order_type}")
  public ResponseEntity<?> getOrdersCount(@PathVariable("order_type") String orderType,
      @PathVariable(required = false) String location_id) {

    if (StringUtils.isNotEmpty(location_id)) {
      return ResponseEntity.ok(orderService.getOrdersCountByLocationId(orderType, location_id));
    }

    return ResponseEntity.ok(orderService.getOrdersCount(orderType));
  }



}
