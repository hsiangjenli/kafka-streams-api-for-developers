package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.Alphabet;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

@Slf4j
public class ExploreJoinsOperatorsTopology {

  public static String ALPHABETS = "alphabets"; // A => First letter in the english alphabet
  public static String ALPHABETS_ABBREVATIONS = "alphabets_abbreviations"; // A=> Apple

  public static Topology build() {
    StreamsBuilder streamsBuilder = new StreamsBuilder();

    joinStreamWithKTable(streamsBuilder);

    return streamsBuilder.build();
  }

  private static void joinStreamWithKTable(StreamsBuilder streamsBuilder) {

    KStream<String, String> alphabetAbbreviation =
        streamsBuilder.stream(
            ALPHABETS_ABBREVATIONS, Consumed.with(Serdes.String(), Serdes.String()));
    alphabetAbbreviation.print(
        Printed.<String, String>toSysOut().withLabel(ALPHABETS_ABBREVATIONS));

    KTable<String, String> alphabetKTable =
        streamsBuilder.table(
            ALPHABETS,
            Consumed.with(Serdes.String(), Serdes.String()),
            Materialized.as("alphabets-store"));
    alphabetKTable.toStream().print(Printed.<String, String>toSysOut().withLabel(ALPHABETS));

    ValueJoiner<String, String, Alphabet> valueJoiner = Alphabet::new;

    KStream<String, Alphabet> joinedKStream =
        alphabetAbbreviation.join(alphabetKTable, valueJoiner);
    joinedKStream.print(
        Printed.<String, Alphabet>toSysOut().withLabel("alphabet-with-abbrevation"));
  }
}
