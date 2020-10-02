package com.scy.kafka.util;

import com.scy.core.format.MessageUtil;
import com.scy.core.trace.TraceUtil;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.lang.NonNull;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * KafkaProducer
 *
 * @author shichunyang
 * Created by shichunyang on 2020/10/1.
 */
@Slf4j
@AllArgsConstructor
public class KafkaProducer {

    private final String topic;

    private final KafkaTemplate<String, String> kafkaTemplate;

    public void send(String key, String value) throws ExecutionException {
        String traceId = TraceUtil.getTraceId();

        ListenableFuture<SendResult<String, String>> listenableFuture = kafkaTemplate.send(topic, key, value);
        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {

            @Override
            public void onFailure(@NonNull Throwable throwable) {
                TraceUtil.putMdc(TraceUtil.TRACE_ID, traceId);
                log.error(MessageUtil.format("kafka send error", throwable, "topic", topic, "key", key, "value", value));
                TraceUtil.clearMdc();
            }

            @Override
            public void onSuccess(SendResult<String, String> result) {
                TraceUtil.putMdc(TraceUtil.TRACE_ID, traceId);
                log.info(MessageUtil.format("kafka send success", "topic", topic, "key", key, "value", value));
                TraceUtil.clearMdc();
            }
        });
        try {
            listenableFuture.get(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.warn(MessageUtil.format("kafka send interrupted", "topic", topic, "key", key, "value", value));
        } catch (TimeoutException e) {
            log.warn(MessageUtil.format("kafka send timeout", "topic", topic, "key", key, "value", value));
        }
    }
}
