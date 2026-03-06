package com.learnkafkastreams.greeting_streams_springboot.topology;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class GreetingStreamsTopology {

  public static String GREETING = "greetings";
  public static String GREETING_OUTPUT = "greetings-output";

  @Autowired
  public void process(StreamsBuilder streamsBuilder) {

    var greetinStream =
        streamsBuilder.stream(GREETING, Consumed.with(Serdes.String(), Serdes.String()));
    greetinStream.print(Printed.<String, String>toSysOut().withLabel(GREETING));

    var modifiedStream = greetinStream.mapValues((readOnluKey, value) -> value.toUpperCase());
    modifiedStream.print(Printed.<String, String>toSysOut().withLabel(GREETING + "-modified"));

    modifiedStream.to(GREETING_OUTPUT, Produced.with(Serdes.String(), Serdes.String()));
  }

}
