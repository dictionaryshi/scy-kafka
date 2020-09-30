package com.scy.kafka.util;

import com.scy.core.CollectionUtil;
import com.scy.core.UUIDUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;

/**
 * KafkaUtil
 *
 * @author shichunyang
 * Created by shichunyang on 2020/9/30.
 */
public class KafkaUtil {

    private KafkaUtil() {
    }

    public static Map<String, Object> getProducerConfigs(String servers) {
        Map<String, Object> props = CollectionUtil.newHashMap();

        // 用于建立到Kafka集群的初始连接的主机/端口对列表。(host1:port1,host2:port2)
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);

        // 等待服务器所有副本的确认信息
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        // 重试次数
        props.put(ProducerConfig.RETRIES_CONFIG, 1);

        // 及时发送
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);

        // 保证消息有序
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);

        // key序列化
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // value序列化
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // 请求最大数据量
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 2 * 1024 * 1024);

        // 设置发送端的消息幂等
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, Boolean.TRUE);

        return props;
    }

    public static Map<String, Object> getConsumerConfigs(String servers, String topic, String groupId) {
        Map<String, Object> props = CollectionUtil.newHashMap();

        // 用于建立到Kafka集群的初始连接的主机/端口对列表。(host1:port1,host2:port2)
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);

        // 设置offset手动提交
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.FALSE);

        // 心跳超时时间
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 15_000);

        // 消费失败超时时间
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300_000);

        // key反序列化
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // value反序列化
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // 自动将偏移量重置为最新偏移量
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        // 一次poll记录的数量
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);

        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, topic + "_" + groupId + "_" + UUIDUtil.uuid());

        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 3 * 1024 * 1024);

        return props;
    }
}
