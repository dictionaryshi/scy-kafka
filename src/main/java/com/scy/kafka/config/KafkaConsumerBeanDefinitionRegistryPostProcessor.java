package com.scy.kafka.config;

import com.scy.core.CollectionUtil;
import com.scy.core.ObjectUtil;
import com.scy.core.StringUtil;
import com.scy.core.enums.ResponseCodeEnum;
import com.scy.core.exception.BusinessException;
import com.scy.core.format.MessageUtil;
import com.scy.core.reflect.ClassUtil;
import com.scy.core.spring.ApplicationContextUtil;
import com.scy.kafka.constant.KafkaConstant;
import com.scy.kafka.listener.AbstractAcknowledgingMessageListener;
import com.scy.kafka.model.ao.ConsumerRegistryAO;
import com.scy.kafka.properties.KafkaProperties;
import com.scy.kafka.properties.TopicProperties;
import com.scy.kafka.util.KafkaUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.lang.NonNull;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.List;
import java.util.Map;

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

        kafkaProperties.getConsumer().getTopics().stream().map(topicProperties -> {
            ConsumerRegistryAO consumerRegistryAO = new ConsumerRegistryAO();
            consumerRegistryAO.setServers(kafkaProperties.getServers());
            consumerRegistryAO.setTopicProperties(topicProperties);
            consumerRegistryAO.setRegistry(registry);
            consumerRegistryAO.setConsumerFactoryBeanName(topicProperties.getName() + KafkaConstant.CONSUMER_FACTORY);
            consumerRegistryAO.setConcurrentMessageListenerContainerBeanName(topicProperties.getName() + KafkaConstant.CONCURRENT_MESSAGE_LISTENER_CONTAINER);
            consumerRegistryAO.setErrorHandlerBeanName(topicProperties.getName() + KafkaConstant.ERROR_HANDLER);
            return consumerRegistryAO;
        }).forEach(this::registerConsumer);

        log.info(MessageUtil.format("kafka consumer config", "topics", kafkaProperties.getConsumer().getTopics()));
    }

    private void registerConsumer(ConsumerRegistryAO consumerRegistryAO) {
        registerConsumerFactory(consumerRegistryAO);

        registerErrorHandler(consumerRegistryAO);

        registerConcurrentMessageListenerContainer(consumerRegistryAO);
    }

    private void registerErrorHandler(ConsumerRegistryAO consumerRegistryAO) {
        BeanDefinitionBuilder beanDefinitionBuilder = BeanDefinitionBuilder.genericBeanDefinition(DefaultErrorHandler.class, () -> {

            // 初始间隔为1秒
            long initialInterval = 1_000L;
            // 最大间隔为30秒
            long maxInterval = 30_000L;
            // 乘数为2, 实现按2的次幂增长
            double multiplier = 2.0;
            // 最大重试次
//            int maxAttempts = Integer.MAX_VALUE;

            ExponentialBackOff exponentialBackOff = new ExponentialBackOff(initialInterval, multiplier);
            exponentialBackOff.setMaxInterval(maxInterval);
//            exponentialBackOff.setMaxElapsedTime(maxInterval * maxAttempts);

            // 设置最大重试次数为100次
            return new DefaultErrorHandler(exponentialBackOff);
        });
        consumerRegistryAO.getRegistry().registerBeanDefinition(consumerRegistryAO.getErrorHandlerBeanName(), beanDefinitionBuilder.getBeanDefinition());
    }

    private void registerConcurrentMessageListenerContainer(ConsumerRegistryAO consumerRegistryAO) {
        BeanDefinitionBuilder beanDefinitionBuilder = BeanDefinitionBuilder.genericBeanDefinition(ConcurrentMessageListenerContainer.class);
        beanDefinitionBuilder.addConstructorArgReference(consumerRegistryAO.getConsumerFactoryBeanName());

        ContainerProperties containerProperties = new ContainerProperties(consumerRegistryAO.getTopicProperties().getTopic());
        Class<?> listenerClass = ClassUtil.resolveClassName(consumerRegistryAO.getTopicProperties().getListenerClassName(), ClassUtil.getDefaultClassLoader());
        if (!ClassUtil.isAssignable(AbstractAcknowledgingMessageListener.class, listenerClass)) {
            throw new BusinessException(ResponseCodeEnum.SYSTEM_EXCEPTION.getCode(), MessageUtil.format("listener class类型不正确", "listener", consumerRegistryAO.getTopicProperties().getListenerClassName()));
        }
        Map<String, ?> beanMap = ApplicationContextUtil.getBeansOfType(listenerClass);
        if (CollectionUtil.isEmpty(beanMap)) {
            throw new BusinessException(ResponseCodeEnum.SYSTEM_EXCEPTION.getCode(), MessageUtil.format("listener 对象不存在", "listener", consumerRegistryAO.getTopicProperties().getListenerClassName()));
        }
        containerProperties.setMessageListener(beanMap.values().stream().findAny().orElse(null));

        containerProperties.setGroupId(consumerRegistryAO.getTopicProperties().getGroupId());
        containerProperties.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        beanDefinitionBuilder.addConstructorArgValue(containerProperties);

        // 设置ErrorHandler
        beanDefinitionBuilder.addPropertyReference("commonErrorHandler", consumerRegistryAO.getErrorHandlerBeanName());

        consumerRegistryAO.getRegistry().registerBeanDefinition(consumerRegistryAO.getConcurrentMessageListenerContainerBeanName(), beanDefinitionBuilder.getBeanDefinition());
    }

    private void registerConsumerFactory(ConsumerRegistryAO consumerRegistryAO) {
        BeanDefinitionBuilder beanDefinitionBuilder = BeanDefinitionBuilder.genericBeanDefinition(DefaultKafkaConsumerFactory.class, () -> {
            Map<String, Object> consumerConfigs = KafkaUtil.getConsumerConfigs(consumerRegistryAO.getServers(), consumerRegistryAO.getTopicProperties().getTopic(), consumerRegistryAO.getTopicProperties().getGroupId());
            return new DefaultKafkaConsumerFactory<String, String>(consumerConfigs);
        });
        consumerRegistryAO.getRegistry().registerBeanDefinition(consumerRegistryAO.getConsumerFactoryBeanName(), beanDefinitionBuilder.getBeanDefinition());
    }

    private void checkParam(List<TopicProperties> topics) {
        topics.forEach(topicProperty -> {
            if (StringUtil.isEmpty(topicProperty.getName())) {
                throw new BusinessException(ResponseCodeEnum.SYSTEM_EXCEPTION.getCode(), "kafka consumer config 缺少name");
            }

            if (StringUtil.isEmpty(topicProperty.getListenerClassName())) {
                throw new BusinessException(ResponseCodeEnum.SYSTEM_EXCEPTION.getCode(), "kafka consumer config 缺少listenerClassName");
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
