package com.learnkafkastreams.service;

import static com.learnkafkastreams.topology.OrdersTopology.GENERAL_ORDERS;
import static com.learnkafkastreams.topology.OrdersTopology.GENERAL_ORDERS_COUNT_WINDOWS;
import static com.learnkafkastreams.topology.OrdersTopology.RESTAURANT_ORDERS;
import static com.learnkafkastreams.topology.OrdersTopology.RESTAURANT_ORDERS_COUNT_WINDOWS;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collection;
import java.util.List;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.learnkafkastreams.domain.OrdersCountPerStoreByWindowsDTO;
import static com.learnkafkastreams.service.OrderService.mapOrderType;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class OrderWindowsService {

  @Autowired
  private OrderStoreService orderStoreService;

  public List<OrdersCountPerStoreByWindowsDTO> getOrderCountWindowsByType(String orderType) {
    var countWindowsStore = getCountWindowsStore(orderType);
    var orderTypeEnum = mapOrderType(orderType);
    var countWindowsIterator = countWindowsStore.all();
    var spliterator = Spliterators.spliteratorUnknownSize(countWindowsIterator, 0);
    return StreamSupport.stream(spliterator, false)
        .map(keyValue -> new OrdersCountPerStoreByWindowsDTO(keyValue.key.key(), keyValue.value,
            orderTypeEnum,
            LocalDateTime.ofInstant(keyValue.key.window().startTime(), ZoneId.of("GMT")),
            LocalDateTime.ofInstant(keyValue.key.window().endTime(), ZoneId.of("GMT"))

        )).collect(Collectors.toList());
  }

  private ReadOnlyWindowStore<String, Long> getCountWindowsStore(String orderType) {

    return switch (orderType) {
      case GENERAL_ORDERS -> orderStoreService.orderWindowsCountStore(GENERAL_ORDERS_COUNT_WINDOWS);
      case RESTAURANT_ORDERS -> orderStoreService
          .orderWindowsCountStore(RESTAURANT_ORDERS_COUNT_WINDOWS);
      default -> throw new IllegalStateException();
    };
  }

  public List<OrdersCountPerStoreByWindowsDTO> getAllOrderCountWindows() {

    var generalOrderCountWindows = getOrderCountWindowsByType(GENERAL_ORDERS);
    var restaurantOrderCountWindows = getOrderCountWindowsByType(RESTAURANT_ORDERS);
    return Stream.of(generalOrderCountWindows, restaurantOrderCountWindows)
        .flatMap(Collection::stream).collect(Collectors.toList());
  }

}
