package com.learnkafkastreams.service;

import static com.learnkafkastreams.topology.OrdersTopology.GENERAL_ORDERS;
import static com.learnkafkastreams.topology.OrdersTopology.GENERAL_ORDERS_COUNT;
import static com.learnkafkastreams.topology.OrdersTopology.GENERAL_ORDERS_REVENUE;
import static com.learnkafkastreams.topology.OrdersTopology.RESTAURANT_ORDERS;
import static com.learnkafkastreams.topology.OrdersTopology.RESTAURANT_ORDERS_COUNT;
import static com.learnkafkastreams.topology.OrdersTopology.RESTAURANT_ORDERS_REVENUE;
import com.learnkafkastreams.domain.AllOrdersCountPerStoreDTO;
import com.learnkafkastreams.domain.OrderCountPerStoreDTO;
import com.learnkafkastreams.domain.OrderRevenueDTO;
import com.learnkafkastreams.domain.OrderType;
import com.learnkafkastreams.domain.TotalRevenue;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Spliterators;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import lombok.val;
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

  public List<AllOrdersCountPerStoreDTO> getAllOrderCount() {
    BiFunction<OrderCountPerStoreDTO, OrderType, AllOrdersCountPerStoreDTO> mapper =
        (orderCountPerStoreDTO, orderType) -> new AllOrdersCountPerStoreDTO(
            orderCountPerStoreDTO.locationId(), orderCountPerStoreDTO.orderCount(), orderType);

    var generalOrdersCount = getOrdersCount(GENERAL_ORDERS).stream()
        .map(orderCountPerStoreDTO -> mapper.apply(orderCountPerStoreDTO, OrderType.GENERAL))
        .toList();

    var restaurantOrderCounts = getOrdersCount(RESTAURANT_ORDERS).stream()
        .map(orderCountPerStoreDTO -> mapper.apply(orderCountPerStoreDTO, OrderType.RESTAURANT))
        .toList();

    return Stream.of(generalOrdersCount, restaurantOrderCounts).flatMap(Collection::stream)
        .collect(Collectors.toList());
  }

  public List<OrderRevenueDTO> getRevenueByOrderType(String orderType) {

    getRevenueStore(orderType);

    var revenueStore = getRevenueStore(orderType);
    var revenues = revenueStore.all();
    var spliterator = Spliterators.spliteratorUnknownSize(revenues, 0);
    return StreamSupport.stream(spliterator, false)
        .map(keyValue -> new OrderRevenueDTO(keyValue.key, mapOrderType(orderType), keyValue.value))
        .collect(Collectors.toList());

  }

  public static OrderType mapOrderType(String orderType) {
    return switch (orderType) {
      case GENERAL_ORDERS -> OrderType.GENERAL;
      case RESTAURANT_ORDERS -> OrderType.RESTAURANT;
      default -> throw new IllegalStateException();
    };
  }

  private ReadOnlyKeyValueStore<String, TotalRevenue> getRevenueStore(String orderType) {
    return switch (orderType) {
      case GENERAL_ORDERS -> orderStoreService.orderRevenueStore(GENERAL_ORDERS_REVENUE);
      case RESTAURANT_ORDERS -> orderStoreService.orderRevenueStore(RESTAURANT_ORDERS_REVENUE);
      default -> throw new IllegalStateException();
    };
  }

}
