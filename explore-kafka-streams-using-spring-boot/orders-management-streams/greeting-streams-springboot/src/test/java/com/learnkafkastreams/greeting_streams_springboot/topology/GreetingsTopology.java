package com.learnkafkastreams.greeting_streams_springboot.topology;

import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import com.learnkafkastreams.greeting_streams_springboot.domain.Greeting;

public class GreetingsTopology {

  TopologyTestDriver topologyTestDriver = null;

  TestInputTopic<String, Greeting> inputTopic = null;

  TestOutputTopic<String, Greeting> outputTopic = null;

}
