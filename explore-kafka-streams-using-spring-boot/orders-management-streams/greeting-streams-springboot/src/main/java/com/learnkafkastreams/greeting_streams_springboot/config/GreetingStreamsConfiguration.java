package com.learnkafkastreams.greeting_streams_springboot.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.stereotype.Component;
import com.learnkafkastreams.greeting_streams_springboot.topology.GreetingStreamsTopology;

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

}
