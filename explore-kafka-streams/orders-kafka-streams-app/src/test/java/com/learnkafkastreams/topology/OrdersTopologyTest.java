package com.learnkafkastreams.topology;

import static com.learnkafkastreams.topology.OrdersTopology.GENERAL_ORDERS_COUNT;
import static com.learnkafkastreams.topology.OrdersTopology.GENERAL_ORDERS_REVENUE;
import static com.learnkafkastreams.topology.OrdersTopology.GENERAL_ORDERS_REVENUE_WINDOWS;
import static com.learnkafkastreams.topology.OrdersTopology.ORDERS;
import static com.learnkafkastreams.topology.OrdersTopology.RESTAURANT_ORDERS_COUNT;
import static com.learnkafkastreams.topology.OrdersTopology.RESTAURANT_ORDERS_REVENUE;
import static com.learnkafkastreams.topology.OrdersTopology.RESTAURANT_ORDERS_REVENUE_WINDOWS;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.learnkafkastreams.domain.Order;
import com.learnkafkastreams.domain.OrderLineItem;
import com.learnkafkastreams.domain.OrderType;
import com.learnkafkastreams.domain.TotalRevenue;
import com.learnkafkastreams.serde.SerdesFactory;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OrdersTopologyTest {

  TopologyTestDriver topologyTestDriver = null;

  TestInputTopic<String, Order> orderInputTopic = null;

  static String INPUT_TOPIC = ORDERS;

  @BeforeEach
  void setUp() {
    topologyTestDriver = new TopologyTestDriver(OrdersTopology.buildTopology());
    orderInputTopic = topologyTestDriver.createInputTopic(ORDERS, Serdes.String().serializer(),
        SerdesFactory.orderSerde().serializer());
  }

  @AfterEach
  void tearDown() {
    topologyTestDriver.close();
  }

  @Test
  void orderCount() {
    pipeOrdersWithTimestamp();

    ReadOnlyKeyValueStore<String, Long> generalOrderCountStore =
        topologyTestDriver.getKeyValueStore(GENERAL_ORDERS_COUNT);
    ReadOnlyKeyValueStore<String, Long> restaurantOrderCountStore =
        topologyTestDriver.getKeyValueStore(RESTAURANT_ORDERS_COUNT);

    var generalOrderCount = generalOrderCountStore.get("store_1234");
    assertEquals(1, generalOrderCount);
    var restaurantOrderCount = restaurantOrderCountStore.get("store_1234");
    assertEquals(1, restaurantOrderCount);
  }

  @Test
  void orderRevenue() {
    pipeOrdersWithTimestamp();

    ReadOnlyKeyValueStore<String, TotalRevenue> generalOrderRevenueStore =
        topologyTestDriver.getKeyValueStore(GENERAL_ORDERS_REVENUE);
    ReadOnlyKeyValueStore<String, TotalRevenue> restaurantOrderRevenueStore =
        topologyTestDriver.getKeyValueStore(RESTAURANT_ORDERS_REVENUE);

    var generalOrderData = generalOrderRevenueStore.get("store_1234");
    assertEquals(1, generalOrderData.runnuingOrderCount());
    assertEquals(new BigDecimal("27.00"), generalOrderData.runningRevenue());

    var restaurantOrderData = restaurantOrderRevenueStore.get("store_1234");
    assertEquals(1, restaurantOrderData.runnuingOrderCount());
    assertEquals(new BigDecimal("15.00"), restaurantOrderData.runningRevenue());
  }

  @Test
  void orderRevenue_multipleOrdersPerStore() {
    orderInputTopic.pipeKeyValueList(orders());
    orderInputTopic.pipeKeyValueList(orders());

    ReadOnlyKeyValueStore<String, TotalRevenue> generalOrderRevenueStore =
        topologyTestDriver.getKeyValueStore(GENERAL_ORDERS_REVENUE);
    ReadOnlyKeyValueStore<String, TotalRevenue> restaurantOrderRevenueStore =
        topologyTestDriver.getKeyValueStore(RESTAURANT_ORDERS_REVENUE);

    var generalOrderData = generalOrderRevenueStore.get("store_1234");
    assertEquals(2, generalOrderData.runnuingOrderCount());
    assertEquals(new BigDecimal("54.00"), generalOrderData.runningRevenue());

    var restaurantOrderData = restaurantOrderRevenueStore.get("store_1234");
    assertEquals(2, restaurantOrderData.runnuingOrderCount());
    assertEquals(new BigDecimal("30.00"), restaurantOrderData.runningRevenue());
  }

  @Test
  void orderRevenue_byWindows() {

    orderInputTopic.pipeKeyValueList(orders());
    orderInputTopic.pipeKeyValueList(orders());

    WindowStore<String, TotalRevenue> generalOrderRevenueWindowsStore =
        topologyTestDriver.getWindowStore(GENERAL_ORDERS_REVENUE_WINDOWS);
    WindowStore<String, TotalRevenue> restaurantOrderRevenueWindowsStore =
        topologyTestDriver.getWindowStore(RESTAURANT_ORDERS_REVENUE_WINDOWS);

    generalOrderRevenueWindowsStore.all().forEachRemaining(windowedTotalRevenue -> {
      var startTime = windowedTotalRevenue.key.window().startTime();
      var endTime = windowedTotalRevenue.key.window().endTime();

      System.out.println("[Output] : " + startTime);
      System.out.println("[Output] : " + endTime);

      var exceptedStartTime = LocalDateTime.parse("2023-02-21T21:25:00");
      var exceptedEndTime = LocalDateTime.parse("2023-02-21T21:25:15");

      var totalRevenue = windowedTotalRevenue.value;

      assertEquals(2, totalRevenue.runnuingOrderCount());
      assertEquals(new BigDecimal("54.00"), totalRevenue.runningRevenue());

      assert LocalDateTime.ofInstant(startTime, ZoneId.of(ZoneId.SHORT_IDS.get("CST")))
          .equals(exceptedStartTime);
      assert LocalDateTime.ofInstant(endTime, ZoneId.of(ZoneId.SHORT_IDS.get("CST")))
          .equals(exceptedEndTime);
    });

    restaurantOrderRevenueWindowsStore.all().forEachRemaining(windowedTotalRevenue -> {
      var startTime = windowedTotalRevenue.key.window().startTime();
      var endTime = windowedTotalRevenue.key.window().endTime();

      var exceptedStartTime = LocalDateTime.parse("2023-02-21T21:25:00");
      var exceptedEndTime = LocalDateTime.parse("2023-02-21T21:25:15");

      var totalRevenue = windowedTotalRevenue.value;
      assertEquals(2, totalRevenue.runnuingOrderCount());
      assertEquals(new BigDecimal("30.00"), totalRevenue.runningRevenue());

      assert LocalDateTime.ofInstant(startTime, ZoneId.of(ZoneId.SHORT_IDS.get("CST")))
          .equals(exceptedStartTime);
      assert LocalDateTime.ofInstant(endTime, ZoneId.of(ZoneId.SHORT_IDS.get("CST")))
          .equals(exceptedEndTime);
    });
  }

  static List<KeyValue<String, Order>> orders() {

    var orderItems = List.of(new OrderLineItem("Bananas", 2, new BigDecimal("2.00")),
        new OrderLineItem("Iphone Charger", 1, new BigDecimal("25.00")));

    var orderItemsRestaurant = List.of(new OrderLineItem("Pizza", 2, new BigDecimal("12.00")),
        new OrderLineItem("Coffee", 1, new BigDecimal("3.00")));

    var order1 = new Order(12345, "store_1234", new BigDecimal("27.00"), OrderType.GENERAL,
        orderItems, LocalDateTime.parse("2023-02-21T21:25:01")
    // LocalDateTime.now()
    // LocalDateTime.now(ZoneId.of("UTC"))
    );

    var order2 = new Order(54321, "store_1234", new BigDecimal("15.00"), OrderType.RESTAURANT,
        orderItemsRestaurant, LocalDateTime.parse("2023-02-21T21:25:01")
    // LocalDateTime.now()
    // LocalDateTime.now(ZoneId.of("UTC"))
    );
    var keyValue1 = KeyValue.pair(order1.orderId().toString(), order1);

    var keyValue2 = KeyValue.pair(order2.orderId().toString(), order2);

    return List.of(keyValue1, keyValue2);
  }

  private void pipeOrdersWithTimestamp() {
    var start = LocalDateTime.parse("2023-02-21T21:25:01").atZone(ZoneId.of("UTC")).toInstant();
    var advanceBy = java.time.Duration.ofSeconds(0);

    orderInputTopic.pipeKeyValueList(orders(), start, advanceBy);
  }

}
