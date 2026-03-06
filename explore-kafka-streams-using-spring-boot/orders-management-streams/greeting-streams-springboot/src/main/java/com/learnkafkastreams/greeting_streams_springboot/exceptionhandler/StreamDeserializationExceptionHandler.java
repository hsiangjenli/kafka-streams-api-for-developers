package com.learnkafkastreams.greeting_streams_springboot.exceptionhandler;

import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;

@Slf4j
public class StreamDeserializationExceptionHandler implements DeserializationExceptionHandler {

  int errorCounter = 0;

  @Override
  public DeserializationHandlerResponse handle(ProcessorContext context,
      ConsumerRecord<byte[], byte[]> record, Exception exception) {
    log.error("Exception is : {}, and the kafka record is {}", exception.getMessage(), record,
        exception);
    log.info("Error counter : {}", errorCounter);

    if (errorCounter < 2) {
      errorCounter = errorCounter + 1;
      return DeserializationHandlerResponse.CONTINUE;
    }

    return DeserializationHandlerResponse.FAIL;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    // TODO Auto-generated method stub

  }
}
