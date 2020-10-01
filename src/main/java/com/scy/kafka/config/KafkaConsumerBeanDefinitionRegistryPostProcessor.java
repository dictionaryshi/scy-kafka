package com.scy.kafka.config;

import com.scy.core.CollectionUtil;
import com.scy.core.ObjectUtil;
import com.scy.core.StringUtil;
import com.scy.core.enums.ResponseCodeEnum;
import com.scy.core.exception.BusinessException;
import com.scy.core.format.MessageUtil;
import com.scy.core.spring.ApplicationContextUtil;
import com.scy.kafka.properties.KafkaProperties;
import com.scy.kafka.properties.TopicProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.lang.NonNull;

import java.util.List;

/**
 * KafkaConsumerBeanDefinitionRegistryPostProcessor
 *
 * @author shichunyang
 * Created by shichunyang on 2020/10/1.
 */
@Slf4j
public class KafkaConsumerBeanDefinitionRegistryPostProcessor implements BeanDefinitionRegistryPostProcessor {

    @Override
    public void postProcessBeanDefinitionRegistry(@NonNull BeanDefinitionRegistry registry) throws BeansException {
        KafkaProperties kafkaProperties = Binder.get(ApplicationContextUtil.getApplicationContext().getEnvironment()).bind(KafkaProperties.PREFIX, Bindable.of(KafkaProperties.class)).orElse(null);
        if (ObjectUtil.isNull(kafkaProperties)) {
            return;
        }

        if (ObjectUtil.isNull(kafkaProperties.getConsumer())) {
            return;
        }

        if (CollectionUtil.isEmpty(kafkaProperties.getConsumer().getTopics())) {
            return;
        }

        checkParam(kafkaProperties.getConsumer().getTopics());

        log.info(MessageUtil.format("kafka consumer config", "topics", kafkaProperties.getConsumer().getTopics()));
    }

    private void checkParam(List<TopicProperties> topics) {
        topics.forEach(topicProperty -> {
            if (StringUtil.isEmpty(topicProperty.getName())) {
                throw new BusinessException(ResponseCodeEnum.SYSTEM_EXCEPTION.getCode(), "kafka consumer config 缺少name");
            }

            if (StringUtil.isEmpty(topicProperty.getTopic())) {
                throw new BusinessException(ResponseCodeEnum.SYSTEM_EXCEPTION.getCode(), "kafka consumer config 缺少topic");
            }

            if (StringUtil.isEmpty(topicProperty.getGroupId())) {
                throw new BusinessException(ResponseCodeEnum.SYSTEM_EXCEPTION.getCode(), "kafka consumer config 缺少groupId");
            }
        });
    }

    @Override
    public void postProcessBeanFactory(@NonNull ConfigurableListableBeanFactory beanFactory) throws BeansException {
    }
}
