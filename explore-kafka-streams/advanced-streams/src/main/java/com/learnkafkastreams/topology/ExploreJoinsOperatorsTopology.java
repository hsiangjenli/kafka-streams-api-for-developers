package com.learnkafkastreams.topology;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

@Slf4j
public class ExploreJoinsOperatorsTopology {

  public static String ALPHABETS = "alphabets"; // A => First letter in the english alphabet
  public static String ALPHABETS_ABBREVATIONS = "alphabets_abbreviations"; // A=> Apple

  public static Topology build() {
    StreamsBuilder streamsBuilder = new StreamsBuilder();

    return streamsBuilder.build();
  }
}
