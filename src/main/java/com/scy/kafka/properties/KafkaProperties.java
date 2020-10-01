package com.scy.kafka.properties;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * KafkaProperties
 *
 * @author shichunyang
 * Created by shichunyang on 2020/10/1.
 */
@Getter
@Setter
@ToString
public class KafkaProperties {

    public static final String PREFIX = "kafka-config";

    private String servers;

    private KafkaProducerProperties producer;

    private KafkaConsumerProperties consumer;
}
