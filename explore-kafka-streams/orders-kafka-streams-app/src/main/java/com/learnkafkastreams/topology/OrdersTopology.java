package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.Order;
import com.learnkafkastreams.domain.OrderType;
import com.learnkafkastreams.domain.Revenue;
import com.learnkafkastreams.domain.Store;
import com.learnkafkastreams.domain.TotalRevenue;
import com.learnkafkastreams.domain.TotalRevenueWithAddress;
import com.learnkafkastreams.serde.SerdesFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.state.KeyValueStore;

@Slf4j
public class OrdersTopology {

  public static final String ORDERS = "orders";
  public static final String STORES = "stores";
  public static final String GENERAL_ORDERS = "general_orders";
  public static final String GENERAL_ORDERS_COUNT = "general_orders_count";
  public static final String GENERAL_ORDERS_REVENUE = "general_orders_revenue";
  public static final String RESTAURANT_ORDERS = "restaurant_orders";
  public static final String RESTAURANT_ORDERS_COUNT = "restaurant_order_count";
  public static final String RESTAURANT_ORDERS_REVENUE = "restaurant_orders_revenue";

  public static Topology buildTopology() {

    Predicate<String, Order> generalPredicate =
        (key, order) -> order.orderType().equals(OrderType.GENERAL);
    Predicate<String, Order> restaurnatPredicate =
        (key, order) -> order.orderType().equals(OrderType.RESTAURANT);

    ValueMapper<Order, Revenue> revenueMapper =
        order -> new Revenue(order.locationId(), order.finalAmount());

    StreamsBuilder streamsBuilder = new StreamsBuilder();
    KStream<String, Order> orderStream =
        streamsBuilder.stream(ORDERS, Consumed.with(Serdes.String(), SerdesFactory.orderSerde()));

    orderStream.print(Printed.<String, Order>toSysOut().withLabel("orders"));
    KTable<String, Store> storesTable =
        streamsBuilder.table(STORES, Consumed.with(Serdes.String(), SerdesFactory.storeSerde()));

    orderStream
        .split(Named.as("General-Restaurant-Stream"))
        .branch(
            generalPredicate,
            Branched.withConsumer(
                generalOrderStream -> {
                  //   generalOrderStream
                  //       .mapValues((readyOnlyKey, value) -> revenueMapper.apply(value))
                  //       .to(
                  //           GENERAL_ORDERS,
                  //           Produced.with(Serdes.String(), SerdesFactory.revenueSerde()));
                  generalOrderStream.print(
                      Printed.<String, Order>toSysOut().withLabel("generalStream"));
                  aggregateOrdersByCount(generalOrderStream, GENERAL_ORDERS_COUNT);
                  aggregateOrderByRevenue(generalOrderStream, GENERAL_ORDERS_REVENUE, storesTable);
                }))
        .branch(
            restaurnatPredicate,
            Branched.withConsumer(
                restaurantOrderStream -> {
                  //   restaurantOrderStream
                  //       .mapValues((readyOnlyKey, value) -> revenueMapper.apply(value))
                  //       .to(
                  //           RESTAURANT_ORDERS,
                  //           Produced.with(Serdes.String(), SerdesFactory.revenueSerde()));
                  restaurantOrderStream.print(
                      Printed.<String, Order>toSysOut().withLabel("restaurnatStream"));
                  aggregateOrdersByCount(restaurantOrderStream, RESTAURANT_ORDERS_COUNT);
                  aggregateOrderByRevenue(
                      restaurantOrderStream, RESTAURANT_ORDERS_REVENUE, storesTable);
                }));

    return streamsBuilder.build();
  }

  public static void aggregateOrdersByCount(
      KStream<String, Order> generalOrderStream, String storeName) {
    KTable<String, Long> orderCountPerStore =
        generalOrderStream
            .map((key, value) -> KeyValue.pair(value.locationId(), value))
            .groupByKey(Grouped.with(Serdes.String(), SerdesFactory.orderSerde()))
            .count(Named.as(storeName), Materialized.as(storeName));

    orderCountPerStore.toStream().print(Printed.<String, Long>toSysOut().withLabel(storeName));
  }

  public static void aggregateOrderByRevenue(
      KStream<String, Order> generalOrderStream,
      String storeName,
      KTable<String, Store> storesTable) {

    Initializer<TotalRevenue> totalRevenueInitializer = TotalRevenue::new;
    Aggregator<String, Order, TotalRevenue> aggregator =
        (key, value, aggregate) -> aggregate.updateRunningRevenue(key, value);
    KTable<String, TotalRevenue> revenueTable =
        generalOrderStream
            .map((key, value) -> KeyValue.pair(value.locationId(), value))
            .groupByKey(Grouped.with(Serdes.String(), SerdesFactory.orderSerde()))
            .aggregate(
                totalRevenueInitializer,
                aggregator,
                Materialized.<String, TotalRevenue, KeyValueStore<Bytes, byte[]>>as(storeName)
                    .withKeySerde(Serdes.String())
                    .withValueSerde(SerdesFactory.totalRevenueSerde()));
    revenueTable.toStream().print(Printed.<String, TotalRevenue>toSysOut().withLabel(storeName));
    ValueJoiner<TotalRevenue, Store, TotalRevenueWithAddress> valueJoiner =
        TotalRevenueWithAddress::new;
    KTable<String, TotalRevenueWithAddress> revenueWithStoreTable =
        revenueTable.join(storesTable, valueJoiner);
    revenueWithStoreTable
        .toStream()
        .print(
            Printed.<String, TotalRevenueWithAddress>toSysOut().withLabel(storeName + "-bystore"));
  }
}
