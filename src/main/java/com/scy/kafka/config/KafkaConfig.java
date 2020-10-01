package com.scy.kafka.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

/**
 * KafkaConfig
 *
 * @author shichunyang
 * Created by shichunyang on 2020/10/1.
 */
@ConditionalOnProperty(value = "kafkaConfig.enabled", havingValue = "true", matchIfMissing = true)
public class KafkaConfig {

    @Bean
    public KafkaProducerBeanDefinitionRegistryPostProcessor kafkaProducerBeanDefinitionRegistryPostProcessor() {
        return new KafkaProducerBeanDefinitionRegistryPostProcessor();
    }

    @Bean
    public KafkaConsumerBeanDefinitionRegistryPostProcessor kafkaConsumerBeanDefinitionRegistryPostProcessor() {
        return new KafkaConsumerBeanDefinitionRegistryPostProcessor();
    }
}
