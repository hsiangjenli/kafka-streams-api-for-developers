package com.learnkafkastreams.greeting_streams_springboot.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.stereotype.Component;
import com.learnkafkastreams.greeting_streams_springboot.topology.GreetingStreamsTopology;
import org.apache.kafka.common.serialization.Serde;
import org.springframework.kafka.support.serializer.JsonSerde;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.learnkafkastreams.greeting_streams_springboot.domain.Greeting;

@Component
public class GreetingStreamsConfiguration {

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
