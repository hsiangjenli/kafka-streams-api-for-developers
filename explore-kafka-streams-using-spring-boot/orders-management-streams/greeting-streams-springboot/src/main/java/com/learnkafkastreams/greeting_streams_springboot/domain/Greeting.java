package com.learnkafkastreams.greeting_streams_springboot.domain;

import java.time.LocalDateTime;

public record Greeting(String message, LocalDateTime timeStamp) {
}
