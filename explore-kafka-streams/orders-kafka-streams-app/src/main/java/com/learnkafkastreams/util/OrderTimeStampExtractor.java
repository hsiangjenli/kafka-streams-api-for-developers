package com.learnkafkastreams.util;

import com.learnkafkastreams.domain.Order;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

@Slf4j
public class OrderTimeStampExtractor implements TimestampExtractor {

  @Override
  public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
    Order order = (Order) record.value();

    if (order != null && order.orderedDateTime() != null) {
      LocalDateTime timeStamp = order.orderedDateTime();
      log.info("timestamp in extractor : {}", timeStamp);
      return convertToInstantFromCST(timeStamp);
    }
    return partitionTime;
  }

  private long convertToInstantFromCST(LocalDateTime timeStamp) {

    return timeStamp.toInstant(ZoneOffset.ofHours(-6)).toEpochMilli();
  }

  private long convertToInstantFromUTC(LocalDateTime timeStamp) {

    return timeStamp.toInstant(ZoneOffset.UTC).toEpochMilli();
  }
}
