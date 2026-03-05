package com.learnkafkastreams.topology;

import java.time.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

@Slf4j
public class ExploreWindowTopology {

  public static final String WINDOW_WORDS = "windows-words";

  public static Topology build() {
    StreamsBuilder streamsBuilder = new StreamsBuilder();
    KStream<String, String> wordsStream =
        streamsBuilder.stream(WINDOW_WORDS, Consumed.with(Serdes.String(), Serdes.String()));
    // thumblingWindows(wordsStream);
    // hoppingWindows(wordsStream);
    slidingWindows(wordsStream);

    return streamsBuilder.build();
  }

  private static void thumblingWindows(KStream<String, String> wordsStream) {
    Duration windowSize = Duration.ofSeconds(5);
    TimeWindows timeWindows = TimeWindows.ofSizeWithNoGrace(windowSize);

    KTable<Windowed<String>, Long> windowsTable =
        wordsStream
            .groupByKey()
            .windowedBy(timeWindows)
            .count()
            .suppress(
                Suppressed.untilWindowCloses(
                    Suppressed.BufferConfig.unbounded().shutDownWhenFull()));
    windowsTable
        .toStream()
        .peek(
            (key, value) -> {
              log.info("thumbling windows : key : {}, value : {}", key, value);
              printLocalDateTimes(key, value);
            })
        .print(Printed.<Windowed<String>, Long>toSysOut().withLabel(WINDOW_WORDS));
  }

  private static void hoppingWindows(KStream<String, String> wordsStream) {

    Duration windowSize = Duration.ofSeconds(5);
    Duration advancedBySize = Duration.ofSeconds(3);
    TimeWindows timeWindows = TimeWindows.ofSizeWithNoGrace(windowSize).advanceBy(advancedBySize);

    KTable<Windowed<String>, Long> windowsTable =
        wordsStream
            .groupByKey()
            .windowedBy(timeWindows)
            .count()
            .suppress(
                Suppressed.untilWindowCloses(
                    Suppressed.BufferConfig.unbounded().shutDownWhenFull()));
    windowsTable
        .toStream()
        .peek(
            (key, value) -> {
              log.info("hopping windows : key : {}, value : {}", key, value);
              printLocalDateTimes(key, value);
            })
        .print(Printed.<Windowed<String>, Long>toSysOut().withLabel(WINDOW_WORDS));
  }

  private static void slidingWindows(KStream<String, String> wordsStream) {

    Duration windowSize = Duration.ofSeconds(5);
    SlidingWindows slidingWindows = SlidingWindows.ofTimeDifferenceWithNoGrace(windowSize);

    KTable<Windowed<String>, Long> windowsTable =
        wordsStream
            .groupByKey()
            .windowedBy(slidingWindows)
            .count()
            .suppress(
                Suppressed.untilWindowCloses(
                    Suppressed.BufferConfig.unbounded().shutDownWhenFull()));
    windowsTable
        .toStream()
        .peek(
            (key, value) -> {
              log.info("sliding windows : key : {}, value : {}", key, value);
              printLocalDateTimes(key, value);
            })
        .print(Printed.<Windowed<String>, Long>toSysOut().withLabel(WINDOW_WORDS));
  }

  private static void printLocalDateTimes(Windowed<String> key, Long value) {
    var startTime = key.window().startTime();
    var endTime = key.window().endTime();

    LocalDateTime startLDT =
        LocalDateTime.ofInstant(startTime, ZoneId.of(ZoneId.SHORT_IDS.get("CST")));
    LocalDateTime endLDT = LocalDateTime.ofInstant(endTime, ZoneId.of(ZoneId.SHORT_IDS.get("CST")));
    log.info("startLDT : {} , endLDT : {}, Count : {}", startLDT, endLDT, value);
  }
}
