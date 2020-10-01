package com.scy.kafka.config;

import com.scy.core.CollectionUtil;
import com.scy.core.ObjectUtil;
import com.scy.core.StringUtil;
import com.scy.core.enums.ResponseCodeEnum;
import com.scy.core.exception.BusinessException;
import com.scy.core.format.MessageUtil;
import com.scy.core.spring.ApplicationContextUtil;
import com.scy.kafka.constant.KafkaConstant;
import com.scy.kafka.model.ao.ProducerRegistryAO;
import com.scy.kafka.properties.KafkaProperties;
import com.scy.kafka.properties.TopicProperties;
import com.scy.kafka.util.KafkaProducer;
import com.scy.kafka.util.KafkaUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.lang.NonNull;

import java.util.List;

/**
 * KafkaProducerBeanDefinitionRegistryPostProcessor
 *
 * @author shichunyang
 * Created by shichunyang on 2020/10/1.
 */
@Slf4j
public class KafkaProducerBeanDefinitionRegistryPostProcessor implements BeanDefinitionRegistryPostProcessor {

    @Override
    public void postProcessBeanDefinitionRegistry(@NonNull BeanDefinitionRegistry registry) throws BeansException {
        KafkaProperties kafkaProperties = Binder.get(ApplicationContextUtil.getApplicationContext().getEnvironment()).bind(KafkaProperties.PREFIX, Bindable.of(KafkaProperties.class)).orElse(null);
        if (ObjectUtil.isNull(kafkaProperties)) {
            return;
        }

        if (ObjectUtil.isNull(kafkaProperties.getProducer())) {
            return;
        }

        if (CollectionUtil.isEmpty(kafkaProperties.getProducer().getTopics())) {
            return;
        }

        checkParam(kafkaProperties.getProducer().getTopics());

        log.info(MessageUtil.format("kafka producer config", "topics", kafkaProperties.getProducer().getTopics()));

        kafkaProperties.getProducer().getTopics().stream().map(topicProperty -> {
            ProducerRegistryAO producerRegistryAO = new ProducerRegistryAO();
            producerRegistryAO.setProducerFactoryBeanName(topicProperty.getName() + KafkaConstant.PRODUCER_FACTORY);
            producerRegistryAO.setKafkaTemplateBeanName(topicProperty.getName() + KafkaConstant.KAFKA_TEMPLATE);
            producerRegistryAO.setProducerBeanName(topicProperty.getName() + KafkaConstant.PRODUCER);

            producerRegistryAO.setServers(kafkaProperties.getServers());
            producerRegistryAO.setTopic(topicProperty.getTopic());
            producerRegistryAO.setRegistry(registry);
            return producerRegistryAO;
        }).forEach(this::registerProducer);
    }

    private void registerProducer(ProducerRegistryAO producerRegistryAO) {
        registerProducerFactory(producerRegistryAO);

        registerKafkaTemplate(producerRegistryAO);

        registerProducerClient(producerRegistryAO);
    }

    private void registerProducerClient(ProducerRegistryAO producerRegistryAO) {
        BeanDefinitionBuilder beanDefinitionBuilder = BeanDefinitionBuilder.genericBeanDefinition(KafkaProducer.class);
        beanDefinitionBuilder.addConstructorArgValue(producerRegistryAO.getTopic());
        beanDefinitionBuilder.addConstructorArgReference(producerRegistryAO.getKafkaTemplateBeanName());
        producerRegistryAO.getRegistry().registerBeanDefinition(producerRegistryAO.getProducerBeanName(), beanDefinitionBuilder.getBeanDefinition());
    }

    private void registerKafkaTemplate(ProducerRegistryAO producerRegistryAO) {
        BeanDefinitionBuilder beanDefinitionBuilder = BeanDefinitionBuilder.genericBeanDefinition(KafkaTemplate.class);
        beanDefinitionBuilder.addConstructorArgReference(producerRegistryAO.getProducerFactoryBeanName());
        producerRegistryAO.getRegistry().registerBeanDefinition(producerRegistryAO.getKafkaTemplateBeanName(), beanDefinitionBuilder.getBeanDefinition());
    }

    private void registerProducerFactory(ProducerRegistryAO producerRegistryAO) {
        BeanDefinitionBuilder beanDefinitionBuilder = BeanDefinitionBuilder.genericBeanDefinition(DefaultKafkaProducerFactory.class, () -> new DefaultKafkaProducerFactory<String, String>(KafkaUtil.getProducerConfigs(producerRegistryAO.getServers())));
        producerRegistryAO.getRegistry().registerBeanDefinition(producerRegistryAO.getProducerFactoryBeanName(), beanDefinitionBuilder.getBeanDefinition());
    }

    private void checkParam(List<TopicProperties> topics) {
        topics.forEach(topicProperty -> {
            if (StringUtil.isEmpty(topicProperty.getName())) {
                throw new BusinessException(ResponseCodeEnum.SYSTEM_EXCEPTION.getCode(), "kafka producer config 缺少name");
            }

            if (StringUtil.isEmpty(topicProperty.getTopic())) {
                throw new BusinessException(ResponseCodeEnum.SYSTEM_EXCEPTION.getCode(), "kafka producer config 缺少topic");
            }
        });
    }

    @Override
    public void postProcessBeanFactory(@NonNull ConfigurableListableBeanFactory beanFactory) throws BeansException {
    }
}
