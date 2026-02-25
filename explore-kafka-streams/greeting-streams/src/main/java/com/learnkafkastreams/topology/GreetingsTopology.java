package com.learnkafkastreams.topology;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;

@Slf4j
public class GreetingsTopology {

  public static String GREETINGS = "greetings";

  public static String GREETINGS_UPPERCASE = "greetings_uppercase";

  public static String GREETINGS_SPANISH = "greetings_spanish";

  public static Topology buildTopology() {
    StreamsBuilder streamsBuilder = new StreamsBuilder();

    KStream<String, String> greetingStream =
        streamsBuilder.stream(GREETINGS, Consumed.with(Serdes.String(), Serdes.String()));

    KStream<String, String> greetingSpanishStream =
        streamsBuilder.stream(GREETINGS_SPANISH, Consumed.with(Serdes.String(), Serdes.String()));

    // merge 兩個 stream
    KStream<String, String> mergedStream = greetingStream.merge(greetingSpanishStream);
    mergedStream.print(Printed.<String, String>toSysOut().withLabel("mergedStream"));

    KStream<String, String> modifiedStream =
        mergedStream.mapValues((readOnlyKey, value) -> value.toUpperCase());
    modifiedStream.print(Printed.<String, String>toSysOut().withLabel("modifiedStream"));

    modifiedStream.to(GREETINGS_UPPERCASE);

    return streamsBuilder.build();
  }
}
