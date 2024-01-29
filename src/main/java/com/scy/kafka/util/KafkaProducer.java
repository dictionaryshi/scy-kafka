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

    public Boolean send(String key, String value) {
        String traceId = TraceUtil.getTraceId();

        return kafkaTemplate.executeInTransaction(operations -> {
            ListenableFuture<SendResult<String, String>> listenableFuture = operations.send(topic, key, value);
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

            listenableFuture.completable().join();

            return Boolean.TRUE;
        });
    }
}
