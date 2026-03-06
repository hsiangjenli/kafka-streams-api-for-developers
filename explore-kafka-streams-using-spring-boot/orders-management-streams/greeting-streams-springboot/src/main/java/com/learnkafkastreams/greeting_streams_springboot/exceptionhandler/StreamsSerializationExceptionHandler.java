package com.learnkafkastreams.greeting_streams_springboot.exceptionhandler;

import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;

@Slf4j
public class StreamsSerializationExceptionHandler implements ProductionExceptionHandler {
  @Override
  public ProductionExceptionHandlerResponse handle(ProducerRecord<byte[], byte[]> record,
      Exception exception) {
    log.error("Serialization Exception is : {}, and the kafka record is : {}",
        exception.getMessage(), record, exception);
    return ProductionExceptionHandlerResponse.CONTINUE;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    // TODO Auto-generated method stub

  }
}
