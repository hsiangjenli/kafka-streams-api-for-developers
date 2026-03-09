package com.learnkafkastreams.service;

import static com.learnkafkastreams.topology.OrdersTopology.GENERAL_ORDERS;
import static com.learnkafkastreams.topology.OrdersTopology.GENERAL_ORDERS_COUNT;
import static com.learnkafkastreams.topology.OrdersTopology.RESTAURANT_ORDERS;
import static com.learnkafkastreams.topology.OrdersTopology.RESTAURANT_ORDERS_COUNT;

import com.learnkafkastreams.domain.OrderCountPerStoreDTO;
import java.util.List;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class OrderService {

  @Autowired
  private OrderStoreService orderStoreService;

  public List<OrderCountPerStoreDTO> getOrdersCount(String orderType) {
    var orderCountStore = getOrderStore(orderType);
    var orders = orderCountStore.all();
    var spliterator = Spliterators.spliteratorUnknownSize(orders, 0);
    return StreamSupport.stream(spliterator, false)
        .map(keyValue -> new OrderCountPerStoreDTO(keyValue.key, keyValue.value))
        .collect(Collectors.toList());
  }

  public ReadOnlyKeyValueStore<String, Long> getOrderStore(String orderType) {
    return switch (orderType) {
      case GENERAL_ORDERS -> orderStoreService.orderCountStore(GENERAL_ORDERS_COUNT);
      case RESTAURANT_ORDERS -> orderStoreService.orderCountStore(RESTAURANT_ORDERS_COUNT);
      default -> throw new IllegalStateException();
    };
  }

  public OrderCountPerStoreDTO getOrdersCountByLocationId(String orderType, String location_id) {
    var orderCountStore = getOrderStore(orderType);
    var orderCount = orderCountStore.get(location_id);
    if (orderCount != null) {
      return new OrderCountPerStoreDTO(location_id, orderCount);
    }

    return null;
  }

}
