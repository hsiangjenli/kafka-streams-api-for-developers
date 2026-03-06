package com.learnkafkastreams.greeting_streams_springboot;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication(scanBasePackages = "com.learnkafkastreams")
@EnableKafkaStreams
public class GreetingStreamsSpringbootApplication {

	public static void main(String[] args) {
		SpringApplication.run(GreetingStreamsSpringbootApplication.class, args);
	}

}
