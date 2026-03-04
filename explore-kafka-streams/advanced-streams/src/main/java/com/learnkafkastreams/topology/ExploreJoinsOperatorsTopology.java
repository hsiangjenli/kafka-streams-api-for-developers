package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.Alphabet;
import java.time.Duration;
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

    // joinStreamWithKTable(streamsBuilder);
    // joinStreamWithGlobalKTable(streamsBuilder);
    // joinKtableWithKTable(streamsBuilder);
    joinStreamWithStream(streamsBuilder);
    // leftJoinStreamWithStream(streamsBuilder);
    // outerJoinStreamWithStream(streamsBuilder);

    return streamsBuilder.build();
  }

  private static void outerJoinStreamWithStream(StreamsBuilder streamsBuilder) {
    KStream<String, String> alphabetAbbreviation =
        streamsBuilder.stream(
            ALPHABETS_ABBREVATIONS, Consumed.with(Serdes.String(), Serdes.String()));
    alphabetAbbreviation.print(
        Printed.<String, String>toSysOut().withLabel(ALPHABETS_ABBREVATIONS));

    KStream<String, String> alphabetStream =
        streamsBuilder.stream(ALPHABETS, Consumed.with(Serdes.String(), Serdes.String()));
    alphabetStream.print(Printed.<String, String>toSysOut().withLabel(ALPHABETS));

    ValueJoiner<String, String, Alphabet> valueJoiner = Alphabet::new;

    JoinWindows fiveSecondJoinWindows =
        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(5));

    StreamJoined<String, String, String> joinedParams =
        StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String());

    KStream<String, Alphabet> joinedStream =
        alphabetAbbreviation.outerJoin(
            alphabetStream, valueJoiner, fiveSecondJoinWindows, joinedParams);
    joinedStream.print(
        Printed.<String, Alphabet>toSysOut()
            .withLabel("alphabet_alphabet_abbreviations-kstream-outer-join"));
  }

  private static void leftJoinStreamWithStream(StreamsBuilder streamsBuilder) {
    KStream<String, String> alphabetAbbreviation =
        streamsBuilder.stream(
            ALPHABETS_ABBREVATIONS, Consumed.with(Serdes.String(), Serdes.String()));
    alphabetAbbreviation.print(
        Printed.<String, String>toSysOut().withLabel(ALPHABETS_ABBREVATIONS));

    KStream<String, String> alphabetStream =
        streamsBuilder.stream(ALPHABETS, Consumed.with(Serdes.String(), Serdes.String()));
    alphabetStream.print(Printed.<String, String>toSysOut().withLabel(ALPHABETS));

    ValueJoiner<String, String, Alphabet> valueJoiner = Alphabet::new;

    JoinWindows fiveSecondJoinWindows =
        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(5));

    StreamJoined<String, String, String> joinedParams =
        StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String());

    KStream<String, Alphabet> joinedStream =
        alphabetAbbreviation.leftJoin(
            alphabetStream, valueJoiner, fiveSecondJoinWindows, joinedParams);
    joinedStream.print(
        Printed.<String, Alphabet>toSysOut()
            .withLabel("alphabet_alphabet_abbreviations-kstream-left-join"));
  }

  private static void joinStreamWithStream(StreamsBuilder streamsBuilder) {
    KStream<String, String> alphabetAbbreviation =
        streamsBuilder.stream(
            ALPHABETS_ABBREVATIONS, Consumed.with(Serdes.String(), Serdes.String()));
    alphabetAbbreviation.print(
        Printed.<String, String>toSysOut().withLabel(ALPHABETS_ABBREVATIONS));

    KStream<String, String> alphabetStream =
        streamsBuilder.stream(ALPHABETS, Consumed.with(Serdes.String(), Serdes.String()));
    alphabetStream.print(Printed.<String, String>toSysOut().withLabel(ALPHABETS));

    ValueJoiner<String, String, Alphabet> valueJoiner = Alphabet::new;

    JoinWindows fiveSecondJoinWindows =
        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(5));

    StreamJoined<String, String, String> joinedParams =
        StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String());

    KStream<String, Alphabet> joinedStream =
        alphabetAbbreviation.join(alphabetStream, valueJoiner, fiveSecondJoinWindows, joinedParams);
    joinedStream.print(
        Printed.<String, Alphabet>toSysOut().withLabel("alphabet_alphabet_abbreviations-kstream"));
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

  private static void joinStreamWithGlobalKTable(StreamsBuilder streamsBuilder) {

    KStream<String, String> alphabetAbbreviation =
        streamsBuilder.stream(
            ALPHABETS_ABBREVATIONS, Consumed.with(Serdes.String(), Serdes.String()));
    alphabetAbbreviation.print(
        Printed.<String, String>toSysOut().withLabel(ALPHABETS_ABBREVATIONS));

    GlobalKTable<String, String> alphabetKTable =
        streamsBuilder.globalTable(
            ALPHABETS,
            Consumed.with(Serdes.String(), Serdes.String()),
            Materialized.as("alphabets-store"));
    // alphabetKTable.toStream().print(Printed.<String, String>toSysOut().withLabel(ALPHABETS));

    ValueJoiner<String, String, Alphabet> valueJoiner = Alphabet::new;
    KeyValueMapper<String, String, String> keyValueMapper = (leftKey, rightKey) -> leftKey;

    KStream<String, Alphabet> joinedKStream =
        alphabetAbbreviation.join(alphabetKTable, keyValueMapper, valueJoiner);
    joinedKStream.print(
        Printed.<String, Alphabet>toSysOut().withLabel("alphabet-with-abbrevation"));
  }

  private static void joinKtableWithKTable(StreamsBuilder streamsBuilder) {

    KTable<String, String> alphabetAbbreviation =
        streamsBuilder.table(
            ALPHABETS_ABBREVATIONS,
            Consumed.with(Serdes.String(), Serdes.String()),
            Materialized.as("alphabet_abbreviation-store"));
    alphabetAbbreviation
        .toStream()
        .print(Printed.<String, String>toSysOut().withLabel(ALPHABETS_ABBREVATIONS));

    KTable<String, String> alphabetKTable =
        streamsBuilder.table(
            ALPHABETS,
            Consumed.with(Serdes.String(), Serdes.String()),
            Materialized.as("alphabets-store"));
    alphabetKTable.toStream().print(Printed.<String, String>toSysOut().withLabel(ALPHABETS));

    ValueJoiner<String, String, Alphabet> valueJoiner = Alphabet::new;

    KTable<String, Alphabet> joinedKStream = alphabetAbbreviation.join(alphabetKTable, valueJoiner);
    joinedKStream
        .toStream()
        .print(Printed.<String, Alphabet>toSysOut().withLabel("alphabet-with-abbrevation"));
  }
}
