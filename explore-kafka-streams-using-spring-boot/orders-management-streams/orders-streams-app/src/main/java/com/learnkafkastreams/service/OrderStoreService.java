package com.learnkafkastreams.service;

import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

@Service
public class OrderStoreService {

  @Autowired StreamsBuilderFactoryBean streamsBuilderFactoryBean;

  public ReadOnlyKeyValueStore<String, Long> orderCountStore(String storeName) {
    return streamsBuilderFactoryBean
        .getKafkaStreams()
        .store(
            StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore()));
  }
}
