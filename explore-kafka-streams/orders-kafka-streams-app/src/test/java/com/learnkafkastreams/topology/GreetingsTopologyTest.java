package com.learnkafkastreams.topology;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import java.time.LocalDateTime;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import com.learnkafkastreams.domain.Greeting;
import com.learnkafkastreams.serdes.SerdesFactory;

public class GreetingsTopologyTest {

  TopologyTestDriver topologyTestDriver = null;

  TestInputTopic<String, Greeting> inputTopic = null;

  TestOutputTopic<String, Greeting> outputTopic = null;

  @BeforeEach
  void setUp() {
    topologyTestDriver = new TopologyTestDriver(GreetingsTopology.buildTopology());
    inputTopic = topologyTestDriver.createInputTopic(GreetingsTopology.GREETINGS,
        Serdes.String().serializer(), SerdesFactory.greetingSerdesGenerics().serializer());
    outputTopic = topologyTestDriver.createOutputTopic(GreetingsTopology.GREETINGS_UPPERCASE,
        Serdes.String().deserializer(), SerdesFactory.greetingSerdesGenerics().deserializer());
  }

  @AfterEach
  void tearDown() {
    topologyTestDriver.close();
  }

  @Test
  void buildTopology() {

    // Given
    inputTopic.pipeInput("GM", new Greeting("Good Morning", LocalDateTime.now()));

    // When
    // Pass

    // Then
    var count = outputTopic.getQueueSize();
    assertEquals(1, count);

    var outputKeyValue = outputTopic.readKeyValue();
    assertEquals("GM", outputKeyValue.key);
    assertEquals("GOOD MORNING", outputKeyValue.value.getMessage());
    assertNotNull(outputKeyValue.value.getTimeStamp());



  }

}
