package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.Greeting;
import com.learnkafkastreams.serdes.SerdesFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

@Slf4j
public class GreetingsTopology {

  public static String GREETINGS = "greetings";

  public static String GREETINGS_UPPERCASE = "greetings_uppercase";

  public static String GREETINGS_SPANISH = "greetings_spanish";

  public static Topology buildTopology() {
    StreamsBuilder streamsBuilder = new StreamsBuilder();

    // KStream<String, String> mergedStream = getStringGreetingKStream(streamsBuilder);
    KStream<String, Greeting> mergedStream = getCustomGreetingKStream(streamsBuilder);
    KStream<String, Greeting> modifiedStream =
        mergedStream.mapValues(
            (readOnlyKey, value) ->
                new Greeting(value.getMessage().toUpperCase(), value.getTimeStamp()));

    modifiedStream.print(Printed.<String, Greeting>toSysOut().withLabel("modifiedStream"));
    modifiedStream.to(
        GREETINGS_UPPERCASE, Produced.with(Serdes.String(), SerdesFactory.greetingSerdes()));

    return streamsBuilder.build();
  }

  public static KStream<String, String> getStringGreetingKStream(StreamsBuilder streamsBuilder) {

    KStream<String, String> greetingStream = streamsBuilder.stream(GREETINGS);
    KStream<String, String> greetingSpanishStream = streamsBuilder.stream(GREETINGS_SPANISH);

    // merge 兩個 stream
    KStream<String, String> mergedStream = greetingStream.merge(greetingSpanishStream);
    mergedStream.print(Printed.<String, String>toSysOut().withLabel("mergedStream"));
    return mergedStream;
  }

  public static KStream<String, Greeting> getCustomGreetingKStream(StreamsBuilder streamsBuilder) {

    KStream<String, Greeting> greetingStream =
        streamsBuilder.stream(
            GREETINGS, Consumed.with(Serdes.String(), SerdesFactory.greetingSerdes()));
    KStream<String, Greeting> greetingSpanishStream =
        streamsBuilder.stream(
            GREETINGS_SPANISH, Consumed.with(Serdes.String(), SerdesFactory.greetingSerdes()));

    // merge 兩個 stream
    KStream<String, Greeting> mergedStream = greetingStream.merge(greetingSpanishStream);
    mergedStream.print(Printed.<String, Greeting>toSysOut().withLabel("mergedStream"));
    return mergedStream;
  }
}
