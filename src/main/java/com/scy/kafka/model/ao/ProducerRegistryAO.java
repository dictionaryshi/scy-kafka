package com.scy.kafka.model.ao;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;

/**
 * ProducerRegistryAO
 *
 * @author shichunyang
 * Created by shichunyang on 2020/10/1.
 */
@Getter
@Setter
@ToString
public class ProducerRegistryAO {

    private String servers;

    private String topic;

    private BeanDefinitionRegistry registry;

    private String producerFactoryBeanName;

    private String kafkaTemplateBeanName;

    private String producerBeanName;
}
