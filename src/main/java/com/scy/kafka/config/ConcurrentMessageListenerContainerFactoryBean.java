package com.scy.kafka.config;

import com.scy.kafka.listener.AbstractAcknowledgingMessageListener;
import com.scy.kafka.model.ao.ConsumerRegistryAO;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

/**
 * ConcurrentMessageListenerContainerFactoryBean
 *
 * @author shichunyang
 * Created by shichunyang on 2020/10/2.
 */
@Getter
@Setter
@ToString
public class ConcurrentMessageListenerContainerFactoryBean implements FactoryBean<ConcurrentMessageListenerContainer<String, String>> {

    private ConcurrentKafkaListenerContainerFactory<String, String> concurrentKafkaListenerContainerFactory;

    private ConsumerRegistryAO consumerRegistry;

    private AbstractAcknowledgingMessageListener acknowledgingMessageListener;

    @Override
    public ConcurrentMessageListenerContainer<String, String> getObject() throws Exception {
        ConcurrentMessageListenerContainer<String, String> concurrentKafkaListenerContainerFactoryContainer = concurrentKafkaListenerContainerFactory.createContainer(consumerRegistry.getTopicProperties().getTopic());
        concurrentKafkaListenerContainerFactoryContainer.getContainerProperties().setGroupId(consumerRegistry.getTopicProperties().getGroupId());
        concurrentKafkaListenerContainerFactoryContainer.getContainerProperties().setMessageListener(acknowledgingMessageListener);
        return concurrentKafkaListenerContainerFactoryContainer;
    }

    @Override
    public Class<?> getObjectType() {
        return ConcurrentMessageListenerContainer.class;
    }
}
