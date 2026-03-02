package com.learnkafkastreams.topology;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

@Slf4j
public class ExploreAggregateOperatorsTopology {

  public static String AGGREGATE = "aggregate";

  public static Topology build() {
    StreamsBuilder streamsBuilder = new StreamsBuilder();

    KStream<String, String> inputStream =
        streamsBuilder.stream(AGGREGATE, Consumed.with(Serdes.String(), Serdes.String()));
    inputStream.print(Printed.<String, String>toSysOut().withLabel(AGGREGATE));
    KGroupedStream<String, String> groupedStream =
        inputStream.groupByKey(Grouped.with(Serdes.String(), Serdes.String()));
    exploreCount(groupedStream);

    return streamsBuilder.build();
  }

  public static void exploreCount(KGroupedStream<String, String> groupedStream) {
    KTable<String, Long> countByAlphabet = groupedStream.count(Named.as("count-per-alphabet"));
    countByAlphabet
        .toStream()
        .print(Printed.<String, Long>toSysOut().withLabel("words-count-per-alphabet"));
  }
}
