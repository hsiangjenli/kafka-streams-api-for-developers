package com.learnkafkastreams.service;

import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;
import com.learnkafkastreams.domain.TotalRevenue;

@Service
public class OrderStoreService {

  @Autowired
  StreamsBuilderFactoryBean streamsBuilderFactoryBean;

  public ReadOnlyKeyValueStore<String, Long> orderCountStore(String storeName) {
    return streamsBuilderFactoryBean.getKafkaStreams().store(
        StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore()));
  }

  public ReadOnlyKeyValueStore<String, TotalRevenue> orderRevenueStore(String storeName) {
    return streamsBuilderFactoryBean.getKafkaStreams().store(
        StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore()));
  }

  public ReadOnlyWindowStore<String, Long> orderWindowsCountStore(String storeName) {
    return streamsBuilderFactoryBean.getKafkaStreams()
        .store(StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.windowStore()));
  }
}
