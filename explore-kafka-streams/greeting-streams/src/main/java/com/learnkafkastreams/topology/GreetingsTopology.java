package com.learnkafkastreams.topology;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

public class GreetingsTopology {

  public static String GREETINGS = "greetings";

  public static String GREETINGS_UPPERCASE = "greetings_uppercase";

  public static Topology buildTopology() {
    StreamsBuilder streamsBuilder = new StreamsBuilder();

    KStream<String, String> greetingStream =
        streamsBuilder.stream(GREETINGS, Consumed.with(Serdes.String(), Serdes.String()));
    greetingStream.print(Printed.<String, String>toSysOut().withLabel("greetingStream"));

    KStream<String, String> modifiedStream =
        greetingStream.flatMap(
            (key, value) -> {
              List<String> newValue = Arrays.asList(value.split(""));
              List<KeyValue<String, String>> newKeyValue =
                  newValue.stream()
                      .map(t -> KeyValue.pair(key.toUpperCase(), t))
                      .collect(Collectors.toList());
              return newKeyValue;
            });
    modifiedStream.print(Printed.<String, String>toSysOut().withLabel("modifiedStream"));

    modifiedStream.to(GREETINGS_UPPERCASE, Produced.with(Serdes.String(), Serdes.String()));

    return streamsBuilder.build();
  }
}
