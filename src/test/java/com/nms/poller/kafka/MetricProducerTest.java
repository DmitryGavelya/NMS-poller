package com.nms.poller.kafka;

import com.nms.poller.TestProps;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MetricProducerTest {

    private static final Instant NOW = Instant.parse("2026-10-27T10:00:00Z");

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, Object> template = mock(KafkaTemplate.class);
    private final MetricProducer producer = new MetricProducer(template, TestProps.props());

    @Test
    @SuppressWarnings("unchecked")
    void routesEventsToTopicOfTheirType() {
        var bool = BooleanMetricEvent.success(1L, "door_open", true, NOW);
        var dbl = DoubleMetricEvent.success(1L, "cpu_load", 1.5, NOW);
        var str = StringMetricEvent.success(1L, "firmware", "1.0", NOW);
        var timeout = DeviceTimeoutEvent.of(1L, NOW);
        when(template.send(anyString(), anyString(), any()))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        for (PollEvent e : new PollEvent[]{bool, dbl, str, timeout}) {
            StepVerifier.create(producer.send(e)).expectNext(true).verifyComplete();
        }

        verify(template).send("t.bool", "1", bool);
        verify(template).send("t.numeric", "1", dbl);
        verify(template).send("t.string", "1", str);
        verify(template).send("t.timeout", "1", timeout);
    }

    @Test
    void returnsFalseWhenSendFails() {
        var event = DoubleMetricEvent.success(1L, "cpu_load", 1.5, NOW);
        when(template.send("t.numeric", "1", event))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker down")));

        StepVerifier.create(producer.send(event)).expectNext(false).verifyComplete();
    }
}
