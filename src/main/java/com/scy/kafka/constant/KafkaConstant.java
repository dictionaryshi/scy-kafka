package com.scy.kafka.constant;

/**
 * KafkaConstant
 *
 * @author shichunyang
 * Created by shichunyang on 2020/10/1.
 */
public class KafkaConstant {

    private KafkaConstant() {
    }

    public static final String PRODUCER_FACTORY = "ProducerFactory";

    public static final String KAFKA_TEMPLATE = "KafkaTemplate";

    public static final String PRODUCER = "Producer";

    public static final String CONSUMER_FACTORY = "ConsumerFactory";

    public static final String CONCURRENT_KAFKA_LISTENER_CONTAINER_FACTORY = "ConcurrentKafkaListenerContainerFactory";

    public static final String CONCURRENT_MESSAGE_LISTENER_CONTAINER = "ConcurrentMessageListenerContainer";
}
