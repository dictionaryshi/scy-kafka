package com.scy.kafka.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;

/**
 * AbstractAcknowledgingMessageListener
 *
 * @author shichunyang
 * Created by shichunyang on 2020/10/2.
 */
public abstract class AbstractAcknowledgingMessageListener implements AcknowledgingMessageListener<String, String> {

    @Override
    public void onMessage(ConsumerRecord<String, String> consumerRecord, Acknowledgment acknowledgment) {
        listen(consumerRecord);
        acknowledgment.acknowledge();
    }

    /**
     * 监听消息
     *
     * @param consumerRecord 消息记录
     */
    public abstract void listen(ConsumerRecord<String, String> consumerRecord);
}
