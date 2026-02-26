package com.learnkafkastreams.serde;

import com.learnkafkastreams.domain.Order;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class SerdesFactory {

  public static Serde<Order> orderSerde() {

    JsonSerializer<Order> jsonSerializer = new JsonSerializer<>();
    JsonDeserializer<Order> jsonDeserializer = new JsonDeserializer<>(Order.class);

    return Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
  }
}
