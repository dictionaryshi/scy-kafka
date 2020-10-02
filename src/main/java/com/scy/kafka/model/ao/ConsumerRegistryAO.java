package com.scy.kafka.model.ao;

import com.scy.kafka.properties.TopicProperties;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;

/**
 * ConsumerRegistryAO
 *
 * @author shichunyang
 * Created by shichunyang on 2020/10/2.
 */
@Getter
@Setter
@ToString
public class ConsumerRegistryAO {

    private String servers;

    private TopicProperties topicProperties;

    private BeanDefinitionRegistry registry;

    private String consumerFactoryBeanName;

    private String concurrentMessageListenerContainerBeanName;
}
