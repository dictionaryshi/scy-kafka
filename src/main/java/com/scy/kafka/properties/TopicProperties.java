package com.scy.kafka.properties;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * TopicProperties
 *
 * @author shichunyang
 * Created by shichunyang on 2020/10/1.
 */
@Getter
@Setter
@ToString
public class TopicProperties {

    private String name;

    private String topic;

    private String groupId;
}
