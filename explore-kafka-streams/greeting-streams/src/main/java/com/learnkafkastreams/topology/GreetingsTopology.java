package com.learnkafkastreams.topology;

import org.apache.kafka.common.serialization.Serdes;
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
        greetingStream
        .filter((key, value) -> value.length() > 5) // filter, filterNot
        .mapValues((readOnlyKey, value) -> value.toUpperCase());
    modifiedStream.print(Printed.<String, String>toSysOut().withLabel("modifiedStream"));

    modifiedStream.to(GREETINGS_UPPERCASE, Produced.with(Serdes.String(), Serdes.String()));

    return streamsBuilder.build();
  }
}
