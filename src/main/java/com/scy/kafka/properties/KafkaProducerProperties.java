package com.scy.kafka.properties;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.List;

/**
 * KafkaProducerProperties
 *
 * @author shichunyang
 * Created by shichunyang on 2020/10/1.
 */
@Getter
@Setter
@ToString
public class KafkaProducerProperties {

    private List<TopicProperties> topics;
}
