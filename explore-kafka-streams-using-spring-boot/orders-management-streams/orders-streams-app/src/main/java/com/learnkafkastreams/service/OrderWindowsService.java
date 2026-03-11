package com.learnkafkastreams.service;

import static com.learnkafkastreams.service.OrderService.mapOrderType;
import static com.learnkafkastreams.topology.OrdersTopology.GENERAL_ORDERS;
import static com.learnkafkastreams.topology.OrdersTopology.GENERAL_ORDERS_COUNT_WINDOWS;
import static com.learnkafkastreams.topology.OrdersTopology.GENERAL_ORDERS_REVENUE_WINDOWS;
import static com.learnkafkastreams.topology.OrdersTopology.RESTAURANT_ORDERS;
import static com.learnkafkastreams.topology.OrdersTopology.RESTAURANT_ORDERS_COUNT_WINDOWS;
import static com.learnkafkastreams.topology.OrdersTopology.RESTAURANT_ORDERS_REVENUE_WINDOWS;

import com.learnkafkastreams.domain.OrderType;
import com.learnkafkastreams.domain.OrdersCountPerStoreByWindowsDTO;
import com.learnkafkastreams.domain.OrdersRevenuePerStoreByWindowsDTO;
import com.learnkafkastreams.domain.TotalRevenue;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class OrderWindowsService {

    @Autowired
    private OrderStoreService orderStoreService;

    public List<OrdersCountPerStoreByWindowsDTO> getOrderCountWindowsByType(String orderType) {
        var countWindowsStore = getCountWindowsStore(orderType);
        var orderTypeEnum = mapOrderType(orderType);
        var countWindowsIterator = countWindowsStore.all();

        return mapToOrderCountPerStoreByWindowsDTO(orderTypeEnum, countWindowsIterator);
    }

    private List<OrdersCountPerStoreByWindowsDTO> mapToOrderCountPerStoreByWindowsDTO(
            OrderType orderTypeEnum,
            KeyValueIterator<Windowed<String>, Long> countWindowsIterator) {
        var spliterator = Spliterators.spliteratorUnknownSize(countWindowsIterator, 0);
        return StreamSupport.stream(spliterator, false)
                .map(keyValue -> new OrdersCountPerStoreByWindowsDTO(keyValue.key.key(),
                        keyValue.value, orderTypeEnum,
                        LocalDateTime.ofInstant(keyValue.key.window().startTime(), ZoneOffset.UTC),
                        LocalDateTime.ofInstant(keyValue.key.window().endTime(), ZoneOffset.UTC)))
                .collect(Collectors.toList());
    }

    private ReadOnlyWindowStore<String, Long> getCountWindowsStore(String orderType) {

        return switch (orderType) {
            case GENERAL_ORDERS -> orderStoreService
                    .orderWindowsCountStore(GENERAL_ORDERS_COUNT_WINDOWS);
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

    public List<OrdersCountPerStoreByWindowsDTO> getAllOrderCountWindows(LocalDateTime fromTime,
            LocalDateTime toTime) {
        var fromTimeInstant = fromTime.toInstant(ZoneOffset.UTC);
        var toTimeInstant = toTime.toInstant(ZoneOffset.UTC);

        var generalOrderCountWindows =
                getCountWindowsStore(GENERAL_ORDERS).fetchAll(fromTimeInstant, toTimeInstant);
        var restaurantOrderCountWindows =
                getCountWindowsStore(RESTAURANT_ORDERS).fetchAll(fromTimeInstant, toTimeInstant);

        var generalOrderCountWindowsDTO =
                mapToOrderCountPerStoreByWindowsDTO(OrderType.GENERAL, generalOrderCountWindows);
        var restaurantOrderCountWindowsDTO = mapToOrderCountPerStoreByWindowsDTO(
                OrderType.RESTAURANT, restaurantOrderCountWindows);

        return Stream.of(generalOrderCountWindowsDTO, restaurantOrderCountWindowsDTO)
                .flatMap(Collection::stream).collect(Collectors.toList());
    }

    public List<OrdersRevenuePerStoreByWindowsDTO> getOrderRevenueWindowsByType(String orderType) {
        var revenueWindowsStore = getRevenueWindowsStore(orderType);
        var orderTypeEnum = mapOrderType(orderType);
        var revenueWindowsIterator = revenueWindowsStore.all();

        var spliterator = Spliterators.spliteratorUnknownSize(revenueWindowsIterator, 0);

        return StreamSupport.stream(spliterator, false)
                .map(keyValue -> new OrdersRevenuePerStoreByWindowsDTO(keyValue.key.key(),
                        keyValue.value, orderTypeEnum,
                        LocalDateTime.ofInstant(keyValue.key.window().startTime(), ZoneOffset.UTC),
                        LocalDateTime.ofInstant(keyValue.key.window().endTime(), ZoneOffset.UTC)))
                .collect(Collectors.toList());
    }

    public ReadOnlyWindowStore<String, TotalRevenue> getRevenueWindowsStore(String orderType) {
        return switch (orderType) {
            case GENERAL_ORDERS -> orderStoreService
                    .orderWindowsRevenueStore(GENERAL_ORDERS_REVENUE_WINDOWS);
            case RESTAURANT_ORDERS -> orderStoreService
                    .orderWindowsRevenueStore(RESTAURANT_ORDERS_REVENUE_WINDOWS);
            default -> throw new IllegalStateException();
        };
    }
}
