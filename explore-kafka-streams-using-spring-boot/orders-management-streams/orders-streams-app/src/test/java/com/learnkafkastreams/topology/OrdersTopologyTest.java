package com.learnkafkastreams.topology;

import static com.learnkafkastreams.topology.OrdersTopology.ORDERS;

import com.learnkafkastreams.domain.Order;
import com.learnkafkastreams.domain.OrderLineItem;
import com.learnkafkastreams.domain.OrderType;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.kafka.support.serializer.JsonSerde;

class OrdersTopologyTest {

  TopologyTestDriver topologyTestDriver = null;
  TestInputTopic<String, Order> ordersInputTopic = null;

  OrdersTopology ordersTopology = new OrdersTopology();
  StreamsBuilder streamsBuilder = null;

  static String INPUT_TOPIC = ORDERS;

  @BeforeEach
  void setUp() {
    streamsBuilder = new StreamsBuilder();
    ordersTopology.process(streamsBuilder);
    topologyTestDriver = new TopologyTestDriver(streamsBuilder.build());

    ordersInputTopic =
        topologyTestDriver.createInputTopic(
            ORDERS, Serdes.String().serializer(), new JsonSerde<>(Order.class).serializer());
  }

  @AfterEach
  void tearDown() {
    topologyTestDriver.close();
  }

  static List<KeyValue<String, Order>> orders() {

    var orderItems =
        List.of(
            new OrderLineItem("Bananas", 2, new BigDecimal("2.00")),
            new OrderLineItem("Iphone Charger", 1, new BigDecimal("25.00")));

    var orderItemsRestaurant =
        List.of(
            new OrderLineItem("Pizza", 2, new BigDecimal("12.00")),
            new OrderLineItem("Coffee", 1, new BigDecimal("3.00")));

    var order1 =
        new Order(
            12345,
            "store_1234",
            new BigDecimal("27.00"),
            OrderType.GENERAL,
            orderItems,
            LocalDateTime.now()
            // LocalDateTime.now(ZoneId.of("UTC"))
            );

    var order2 =
        new Order(
            54321,
            "store_1234",
            new BigDecimal("15.00"),
            OrderType.RESTAURANT,
            orderItemsRestaurant,
            LocalDateTime.now()
            // LocalDateTime.now(ZoneId.of("UTC"))
            );
    var keyValue1 = KeyValue.pair(order1.orderId().toString(), order1);

    var keyValue2 = KeyValue.pair(order2.orderId().toString(), order2);

    return List.of(keyValue1, keyValue2);
  }
}
