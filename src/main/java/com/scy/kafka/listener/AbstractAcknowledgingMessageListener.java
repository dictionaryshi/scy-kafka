package com.scy.kafka.listener;

import com.scy.core.StringUtil;
import com.scy.core.format.MessageUtil;
import com.scy.core.trace.TraceUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;

/**
 * AbstractAcknowledgingMessageListener
 *
 * @author shichunyang
 * Created by shichunyang on 2020/10/2.
 */
@Slf4j
public abstract class AbstractAcknowledgingMessageListener implements AcknowledgingMessageListener<String, String> {

    @Override
    public void onMessage(ConsumerRecord<String, String> consumerRecord, Acknowledgment acknowledgment) {
        TraceUtil.setTraceId(null);

        String topic = consumerRecord.topic();
        String key = consumerRecord.key();
        String value = consumerRecord.value();
        int partition = consumerRecord.partition();
        long offset = consumerRecord.offset();
        try {
            long startTime = System.currentTimeMillis();

            listen(consumerRecord);

            log.info(MessageUtil.format("kafka listen",
                    "topic", topic, "key", key, "value", value, "partition", partition, "offset", offset, StringUtil.COST, System.currentTimeMillis() - startTime));

            acknowledgment.acknowledge();
        } catch (Throwable throwable) {
            log.error(MessageUtil.format("kafka listen error", throwable,
                    "topic", topic, "key", key, "value", value, "partition", partition, "offset", offset));
            throw throwable;
        } finally {
            TraceUtil.clearTrace();
        }
    }

    /**
     * 监听消息
     *
     * @param consumerRecord 消息记录
     */
    public abstract void listen(ConsumerRecord<String, String> consumerRecord);
}
