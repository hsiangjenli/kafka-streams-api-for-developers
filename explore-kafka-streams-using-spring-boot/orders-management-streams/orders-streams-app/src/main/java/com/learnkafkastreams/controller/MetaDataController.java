package com.learnkafkastreams.controller;

import com.learnkafkastreams.domain.HostInfoDTO;
import com.learnkafkastreams.producer.MetaDataService;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1/metadata")
public class MetaDataController {

  @Autowired private MetaDataService metaDataService;

  @GetMapping("/all")
  public List<HostInfoDTO> getStreamsMetaData() {
    return metaDataService.getStreamsMetaData();
  }
}
