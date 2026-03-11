package com.learnkafkastreams.service;

import com.learnkafkastreams.domain.TotalRevenue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
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

  public ReadOnlyKeyValueStore<String, TotalRevenue> orderRevenueStore(String storeName) {
    return streamsBuilderFactoryBean
        .getKafkaStreams()
        .store(
            StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore()));
  }

  public ReadOnlyWindowStore<String, Long> orderWindowsCountStore(String storeName) {
    return streamsBuilderFactoryBean
        .getKafkaStreams()
        .store(StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.windowStore()));
  }

  public ReadOnlyWindowStore<String, TotalRevenue> orderWindowsRevenueStore(String storeName) {
    return streamsBuilderFactoryBean
        .getKafkaStreams()
        .store(StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.windowStore()));
  }
}
