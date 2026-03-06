package com.learnkafkastreams.greeting_streams_springboot.config;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.streams.RecoveringDeserializationExceptionHandler;
import org.springframework.stereotype.Component;
import com.learnkafkastreams.greeting_streams_springboot.topology.GreetingStreamsTopology;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.kafka.support.serializer.JsonSerde;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.learnkafkastreams.greeting_streams_springboot.domain.Greeting;
import com.learnkafkastreams.greeting_streams_springboot.exceptionhandler.StreamsProcessorCustomErrorHandler;

@Slf4j
@Component
public class GreetingStreamsConfiguration {

  @Autowired
  KafkaProperties kafkaProperties;

  @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
  public KafkaStreamsConfiguration kStreamsConfiguration(SslBundles sslBundles) {

    var kafkaStreamProperties = kafkaProperties.buildStreamsProperties(sslBundles);

    kafkaStreamProperties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
        RecoveringDeserializationExceptionHandler.class);
    kafkaStreamProperties.put(
        RecoveringDeserializationExceptionHandler.KSTREAM_DESERIALIZATION_RECOVERER, recoverer());

    return new KafkaStreamsConfiguration(kafkaStreamProperties);
  }

  @Bean
  public StreamsBuilderFactoryBeanConfigurer streamsBuilderFactoryBeanConfigurer() {
    return factoryBeanConfigurer -> {
      factoryBeanConfigurer
          .setStreamsUncaughtExceptionHandler(new StreamsProcessorCustomErrorHandler());
    };
  }

  private ConsumerRecordRecoverer recoverer() {
    return (consumerRecord, e) -> {

      log.error("Exception is : {}, Failed Record : {}", consumerRecord, e.getMessage());

    };
  }

  @Bean
  public NewTopic greetingTopic() {
    return TopicBuilder.name(GreetingStreamsTopology.GREETING).partitions(2).replicas(1).build();
  }

  @Bean
  public NewTopic greetingOutputTopic() {
    return TopicBuilder.name(GreetingStreamsTopology.GREETING_OUTPUT).partitions(2).replicas(1)
        .build();
  }

  @Bean
  public Serde<Greeting> greetingSerde(ObjectMapper objectMapper) {
    return new JsonSerde<>(Greeting.class, objectMapper);
  }

  @Bean
  public ObjectMapper objectMapper() {
    return new ObjectMapper().registerModule(new JavaTimeModule())
        .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
  }

}
