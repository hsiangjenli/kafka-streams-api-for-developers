package com.learnkafkastreams.producer;

import com.learnkafkastreams.domain.HostInfoDTO;
import com.learnkafkastreams.domain.HostInfoDTOWithKey;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

@Service
public class MetaDataService {

  private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

  public MetaDataService(StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
    this.streamsBuilderFactoryBean = streamsBuilderFactoryBean;
  }

  public List<HostInfoDTO> getStreamsMetaData() {
    return streamsBuilderFactoryBean.getKafkaStreams().metadataForAllStreamsClients().stream()
        .map(
            streamMetadata -> {
              var hostInfo = streamMetadata.hostInfo();
              return new HostInfoDTO(hostInfo.host(), hostInfo.port());
            })
        .collect(Collectors.toList());
  }

  public HostInfoDTOWithKey getStreamsMetaData(String storeName, String locationId) {
    var metaDataForKey =
        streamsBuilderFactoryBean
            .getKafkaStreams()
            .queryMetadataForKey(storeName, locationId, Serdes.String().serializer());
    if (metaDataForKey != null) {
      var activeHost = metaDataForKey.activeHost();
      return new HostInfoDTOWithKey(activeHost.host(), activeHost.port(), locationId);
    }
    return null;
  }
}
