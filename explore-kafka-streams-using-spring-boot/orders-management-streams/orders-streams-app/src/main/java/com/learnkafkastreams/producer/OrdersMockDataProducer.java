package com.learnkafkastreams.producer;

import static com.learnkafkastreams.producer.ProducerUtil.publishMessageSync;
import static java.lang.Thread.sleep;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.learnkafkastreams.domain.Order;
import com.learnkafkastreams.domain.OrderLineItem;
import com.learnkafkastreams.domain.OrderType;
import com.learnkafkastreams.topology.OrdersTopology;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OrdersMockDataProducer {

  public static void main(String[] args) throws InterruptedException {
    ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule())
        .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

    publishOrders(objectMapper, buildOrders());
    // publishBulkOrders(objectMapper);

    /**
     * To test grace period. 1. Run the {@link #publishOrders(ObjectMapper, List)} function during
     * the start of the minute. 2. Wait until the next minute and run the
     * {@link #publishOrders(ObjectMapper, List)} and then the
     * {@link #publishOrdersToTestGrace(ObjectMapper, List)} function before the 15th second. - This
     * should allow the aggregation to be added to the window before
     */
    // publishOrdersForGracePeriod(objectMapper, buildOrders());

    // Future and Old Records
    // publishFutureRecords(objectMapper);
    // publishExpiredRecords(objectMapper);

  }

  private static void publishFutureRecords(ObjectMapper objectMapper) {
    var localDateTime = LocalDateTime.now().plusDays(1);

    var newOrders =
        buildOrders()
            .stream().map(order -> new Order(order.orderId(), order.locationId(),
                order.finalAmount(), order.orderType(), order.orderLineItems(), localDateTime))
            .toList();
    publishOrders(objectMapper, newOrders);
  }

  private static void publishExpiredRecords(ObjectMapper objectMapper) {

    var localDateTime = LocalDateTime.now().minusDays(1);

    var newOrders =
        buildOrders()
            .stream().map(order -> new Order(order.orderId(), order.locationId(),
                order.finalAmount(), order.orderType(), order.orderLineItems(), localDateTime))
            .toList();
    publishOrders(objectMapper, newOrders);
  }

  private static void publishOrdersForGracePeriod(ObjectMapper objectMapper, List<Order> orders)
      throws InterruptedException {

    var now = LocalDateTime.now().withNano(0);
    var windowStart = now.withSecond(0).withNano(0); // window 開始時間
    var onTimeEventTime = windowStart.plusSeconds(5); // 事件時間在 window 內，準時送
    var lateEventTime = windowStart.plusSeconds(10); // 事件時間在同一 window 內，晚一點送

    var onTimeOrders = stampOrdersForType(orders, OrderType.GENERAL, onTimeEventTime);
    var lateOrders = stampOrdersForType(orders, OrderType.GENERAL, lateEventTime);
    var advanceStreamTimeOrders =
        stampOrdersForType(orders, OrderType.GENERAL, windowStart.plusSeconds(80));
    var tooLateOrders = stampOrdersForType(orders, OrderType.GENERAL, lateEventTime);

    log.info("Grace test windowStart={}, onTimeEventTime={}, lateEventTime={}", windowStart,
        onTimeEventTime, lateEventTime);

    log.info("Grace test step=on_time"); // 準時送出第 5 秒的事件
    publishOrders(objectMapper, onTimeOrders);

    // Publish late events within grace (windowSize=60s, grace=15s).
    waitUntil(windowStart.plusSeconds(65)); // windowStart 過了 65 秒後，送出第 10 秒的事件
    log.info("Grace test step=late_within_grace");
    publishOrders(objectMapper, lateOrders);

    // Publish late events after grace to confirm they are dropped.
    waitUntil(windowStart.plusSeconds(80));
    log.info("Grace test step=advance_stream_time");
    publishOrders(objectMapper, advanceStreamTimeOrders); // windowStart 過了 80 秒後，送出第 80 秒的事件
    log.info("Grace test step=late_after_grace");
    publishOrders(objectMapper, tooLateOrders); // windowStart 過了 80 秒後，送出第 10 秒的事件
  }

  private static List<Order> stampOrdersForType(List<Order> orders, OrderType orderType,
      LocalDateTime eventTime) {
    return orders.stream().filter(order -> order.orderType().equals(orderType))
        .map(order -> new Order(order.orderId(), order.locationId(), order.finalAmount(),
            order.orderType(), order.orderLineItems(), eventTime))
        .toList();
  }

  private static void waitUntil(LocalDateTime targetTime) throws InterruptedException {
    while (LocalDateTime.now().isBefore(targetTime)) {
      sleep(200);
    }
  }

  private static void publishRecordsWithDelay(List<Order> newOrders, LocalDateTime localDateTime,
      ObjectMapper objectMapper) {

    publishOrders(objectMapper, newOrders);
  }

  private static void publishRecordsWithDelay(List<Order> newOrders, LocalDateTime localDateTime,
      ObjectMapper objectMapper, int timeToPublish) {

    var flag = true;
    while (flag) {
      var dateTime = LocalDateTime.now();
      if (dateTime.toLocalTime().getMinute() == localDateTime.getMinute()
          && dateTime.toLocalTime().getSecond() == timeToPublish) {
        System.out.printf("Publishing the record with delay ");
        publishOrders(objectMapper, newOrders);
        flag = false;
      } else {
        System.out.println(
            " Current Time is  and the record will be published at the 16th second: " + dateTime);
        System.out.println("Record Date Time : " + localDateTime);
      }
    }
  }

  private static List<Order> buildOrdersForGracePeriod() {

    var orderItems = List.of(new OrderLineItem("Bananas", 2, new BigDecimal("2.00")),
        new OrderLineItem("Iphone Charger", 1, new BigDecimal("25.00")));

    var orderItemsRestaurant = List.of(new OrderLineItem("Pizza", 2, new BigDecimal("12.00")),
        new OrderLineItem("Coffee", 1, new BigDecimal("3.00")));

    var order1 = new Order(12345, "store_999", new BigDecimal("27.00"), OrderType.RESTAURANT,
        orderItems, LocalDateTime.parse("2023-01-06T18:50:21"));

    var order2 = new Order(54321, "store_999", new BigDecimal("15.00"), OrderType.RESTAURANT,
        orderItemsRestaurant, LocalDateTime.parse("2023-01-06T18:50:21"));

    var order3 = new Order(54321, "store_999", new BigDecimal("15.00"), OrderType.RESTAURANT,
        orderItemsRestaurant, LocalDateTime.parse("2023-01-06T18:50:22"));

    return List.of(order1, order2, order3);
  }

  private static List<Order> buildOrders() {
    var orderItems = List.of(new OrderLineItem("Bananas", 2, new BigDecimal("2.00")),
        new OrderLineItem("Iphone Charger", 1, new BigDecimal("25.00")));

    var orderItemsRestaurant = List.of(new OrderLineItem("Pizza", 2, new BigDecimal("12.00")),
        new OrderLineItem("Coffee", 1, new BigDecimal("3.00")));

    var order1 = new Order(12345, "store_1234", new BigDecimal("27.00"), OrderType.GENERAL,
        orderItems, LocalDateTime.now()
    // LocalDateTime.now(ZoneId.of("UTC"))
    );

    var order2 = new Order(54321, "store_1234", new BigDecimal("15.00"), OrderType.RESTAURANT,
        orderItemsRestaurant, LocalDateTime.now()
    // LocalDateTime.now(ZoneId.of("UTC"))
    );

    var order3 = new Order(12345, "store_4567", new BigDecimal("27.00"), OrderType.GENERAL,
        orderItems, LocalDateTime.now()
    // LocalDateTime.parse("2023-02-25T05:02:01")
    // LocalDateTime.now(ZoneId.of("UTC"))
    );

    var order4 = new Order(12345, "store_4567", new BigDecimal("27.00"), OrderType.RESTAURANT,
        orderItems, LocalDateTime.now()
    // LocalDateTime.parse("2023-02-25T05:02:01")
    // LocalDateTime.now(ZoneId.of("UTC"))
    );

    return List.of(order1, order2, order3, order4);
  }

  private static List<Order> buildOrdersToTestGrace() {
    var orderItems = List.of(new OrderLineItem("Bananas", 2, new BigDecimal("2.00")),
        new OrderLineItem("Iphone Charger", 1, new BigDecimal("25.00")));

    var orderItemsRestaurant = List.of(new OrderLineItem("Pizza", 2, new BigDecimal("12.00")),
        new OrderLineItem("Coffee", 1, new BigDecimal("3.00")));

    // 使用 5 分钟前的时间戳，超出窗口保留期（60秒 + 15秒 grace period）
    var oldTimestamp = LocalDateTime.now().minusMinutes(5);

    var order1 = new Order(12345, "store_1234", new BigDecimal("27.00"), OrderType.GENERAL,
        orderItems, oldTimestamp
    // LocalDateTime.now(ZoneId.of("UTC"))
    );

    var order2 = new Order(54321, "store_1234", new BigDecimal("100.00"), OrderType.RESTAURANT,
        orderItemsRestaurant, oldTimestamp
    // LocalDateTime.now(ZoneId.of("UTC"))
    );

    var order3 =
        new Order(12345, "store_4567", new BigDecimal("27.00"), OrderType.GENERAL, orderItems,
            // LocalDateTime.now()
            oldTimestamp
        // LocalDateTime.now(ZoneId.of("UTC"))
        );

    var order4 =
        new Order(12345, "store_4567", new BigDecimal("100.00"), OrderType.RESTAURANT, orderItems,
            // LocalDateTime.now()
            oldTimestamp
        // LocalDateTime.now(ZoneId.of("UTC"))
        );

    return List.of(order1, order2, order3, order4);
  }

  private static void publishBulkOrders(ObjectMapper objectMapper) throws InterruptedException {

    int count = 0;
    while (count < 100) {
      var orders = buildOrders();
      publishOrders(objectMapper, orders);
      sleep(1000);
      count++;
    }
  }

  private static void publishOrdersToTestGrace(ObjectMapper objectMapper, List<Order> orders) {

    orders.forEach(order -> {
      try {
        var ordersJSON = objectMapper.writeValueAsString(order);
        var recordMetaData =
            publishMessageSync(OrdersTopology.ORDERS, order.orderId() + "", ordersJSON);
        log.info("Published the order message : {} ", recordMetaData);
      } catch (JsonProcessingException e) {
        log.error("JsonProcessingException : {} ", e.getMessage(), e);
        throw new RuntimeException(e);
      } catch (Exception e) {
        log.error("Exception : {} ", e.getMessage(), e);
        throw new RuntimeException(e);
      }
    });
  }

  private static void publishOrders(ObjectMapper objectMapper, List<Order> orders) {

    orders.forEach(order -> {
      try {
        var ordersJSON = objectMapper.writeValueAsString(order);
        var recordMetaData =
            publishMessageSync(OrdersTopology.ORDERS, order.orderId() + "", ordersJSON);
        log.info("Published the order message : {} ", recordMetaData);
      } catch (JsonProcessingException e) {
        log.error("JsonProcessingException : {} ", e.getMessage(), e);
        throw new RuntimeException(e);
      } catch (Exception e) {
        log.error("Exception : {} ", e.getMessage(), e);
        throw new RuntimeException(e);
      }
    });
  }
}
