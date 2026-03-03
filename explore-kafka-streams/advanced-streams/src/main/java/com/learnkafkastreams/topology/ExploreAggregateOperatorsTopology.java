package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.AlphabetWordAggregate;
import com.learnkafkastreams.serdes.SerdesFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

@Slf4j
public class ExploreAggregateOperatorsTopology {

  public static String AGGREGATE = "aggregate";

  public static Topology build() {
    StreamsBuilder streamsBuilder = new StreamsBuilder();

    KStream<String, String> inputStream =
        streamsBuilder.stream(AGGREGATE, Consumed.with(Serdes.String(), Serdes.String()));
    inputStream.print(Printed.<String, String>toSysOut().withLabel(AGGREGATE));

    // 計算每個 key 底下的數量
    // [words-count-per-alphabet]: A, 9
    // [words-count-per-alphabet]: B, 6
    KGroupedStream<String, String> groupedStream =
        inputStream.groupByKey(Grouped.with(Serdes.String(), Serdes.String()));

    // 計算每個字出現的次數
    // [words-count-per-alphabet]: Apple, 2
    // [words-count-per-alphabet]: Alligator, 2
    // [words-count-per-alphabet]: Ambulance, 2
    // [words-count-per-alphabet]: Bus, 2
    // [words-count-per-alphabet]: Baby, 2
    // KGroupedStream<String, String> groupedStream =
    //     inputStream.groupBy((key, value) -> value, Grouped.with(Serdes.String(),
    // Serdes.String()));

    exploreCount(groupedStream);
    // exploreReduce(groupedStream);
    // exploreAggregate(groupedStream);

    return streamsBuilder.build();
  }

  public static void exploreCount(KGroupedStream<String, String> groupedStream) {
    KTable<String, Long> countByAlphabet =
        groupedStream.count(Named.as("count-per-alphabet"), Materialized.as("count-per-alphabet"));
    countByAlphabet
        .toStream()
        .print(Printed.<String, Long>toSysOut().withLabel("words-count-per-alphabet"));
  }

  public static void exploreReduce(KGroupedStream<String, String> groupedStream) {
    KTable<String, String> reducedStream =
        groupedStream.reduce(
            (value1, value2) -> {
              log.info("value 1 : {}, value 2 : {}", value1, value2);
              return value1.toUpperCase() + "-" + value2.toUpperCase();
            },
            Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("reduced-words")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String()));

    reducedStream.toStream().print(Printed.<String, String>toSysOut().withLabel("reduced-words"));
  }

  public static void exploreAggregate(KGroupedStream<String, String> groupedStream) {

    Initializer<AlphabetWordAggregate> alphabetWordAggregateInitializer =
        AlphabetWordAggregate::new;
    Aggregator<String, String, AlphabetWordAggregate> aggregator =
        (key, value, aggregate) -> aggregate.updateNewEvents(key, value);

    KTable<String, AlphabetWordAggregate> aggregatedStream =
        groupedStream.aggregate(
            alphabetWordAggregateInitializer,
            aggregator,
            Materialized.<String, AlphabetWordAggregate, KeyValueStore<Bytes, byte[]>>as(
                    "aggregated-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(SerdesFactory.alphabetWordAggregate()));
    aggregatedStream
        .toStream()
        .print(Printed.<String, AlphabetWordAggregate>toSysOut().withLabel("aggregated-words"));
  }
}
