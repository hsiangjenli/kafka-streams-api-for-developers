package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.Order;
import com.learnkafkastreams.domain.OrderType;
import com.learnkafkastreams.serde.SerdesFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

@Slf4j
public class OrdersTopology {

  public static final String ORDERS = "orders";
  public static final String STORES = "stores";
  public static final String GENERAL_ORDERs = "general_orders";
  public static final String RESTAURANT_OEDERS = "restaurant_orders";

  public static Topology buildTopology() {

    Predicate<String, Order> generalPredicate =
        (key, order) -> order.orderType().equals(OrderType.GENERAL);
    Predicate<String, Order> restaurnatPreidcate =
        (key, order) -> order.orderType().equals(OrderType.RESTAURANT);

    StreamsBuilder streamsBuilder = new StreamsBuilder();
    KStream<String, Order> orderStream =
        streamsBuilder.stream(ORDERS, Consumed.with(Serdes.String(), SerdesFactory.orderSerde()));

    orderStream.print(Printed.<String, Order>toSysOut().withLabel("orders"));

    orderStream
        .split(Named.as("General-Restaurant-Stream"))
        .branch(
            generalPredicate,
            Branched.withConsumer(
                generalOrderStream -> {
                  generalOrderStream.to(
                      GENERAL_ORDERs, Produced.with(Serdes.String(), SerdesFactory.orderSerde()));
                  generalOrderStream.print(
                      Printed.<String, Order>toSysOut().withLabel("generalStream"));
                }))
        .branch(
            generalPredicate,
            Branched.withConsumer(
                restaurantOrderStream -> {
                  restaurantOrderStream.to(
                      RESTAURANT_OEDERS,
                      Produced.with(Serdes.String(), SerdesFactory.orderSerde()));
                  restaurantOrderStream.print(
                      Printed.<String, Order>toSysOut().withLabel("restaurnatStream"));
                }));

    return streamsBuilder.build();
  }
}
