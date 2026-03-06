package com.learnkafkastreams.greeting_streams_springboot.exceptionhandler;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

@Slf4j
public class StreamsProcessorCustomErrorHandler implements StreamsUncaughtExceptionHandler {
  @Override
  public StreamThreadExceptionResponse handle(Throwable exception) {
    log.error("Exception in the Application : {}", exception.getMessage(), exception);

    if (exception instanceof StreamsException) {
      Throwable cause = exception.getCause();
      if (cause.getMessage().equals("Transient Error")) {
        return StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
      }
    }
    return StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
  }
}
