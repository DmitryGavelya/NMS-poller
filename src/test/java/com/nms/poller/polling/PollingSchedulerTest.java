package com.nms.poller.polling;

import com.nms.poller.config.PollerProperties;
import com.nms.poller.kafka.MetricEvent;
import com.nms.poller.kafka.MetricProducer;
import com.nms.poller.registry.Device;
import com.nms.poller.registry.DeviceRegistry;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PollingSchedulerTest {

    private static final Instant NOW = Instant.parse("2026-10-27T10:00:00Z");

    private final DeviceRegistry registry = mock(DeviceRegistry.class);
    private final DevicePoller poller = mock(DevicePoller.class);
    private final MetricProducer producer = mock(MetricProducer.class);
    private final PollingScheduler scheduler = new PollingScheduler(registry, poller, producer, new PollerProperties(
            new PollerProperties.Backend("http://backend", Duration.ofMinutes(1), Duration.ofSeconds(1)),
            new PollerProperties.Emulator("http://emulator", Duration.ofSeconds(1), Duration.ofSeconds(1), 0,
                    Duration.ofMillis(10)),
            new PollerProperties.Poll(Duration.ofSeconds(30), 4),
            new PollerProperties.Kafka("nms.metrics", 1)));

    @Test
    void publishesEventsOfAllDevices() {
        Device router = new Device("router-1", null, null, null);
        Device lte = new Device("lte-1", null, null, null);
        MetricEvent cpu = MetricEvent.success("router-1", "cpu_load", 40.0, NOW);
        MetricEvent mem = MetricEvent.success("router-1", "memory_load", 50.0, NOW);
        MetricEvent lteDown = MetricEvent.timeout("lte-1", NOW);
        when(poller.poll(router)).thenReturn(Mono.just(List.of(cpu, mem)));
        when(poller.poll(lte)).thenReturn(Mono.just(List.of(lteDown)));
        when(producer.send(cpu)).thenReturn(Mono.just(true));
        when(producer.send(mem)).thenReturn(Mono.just(false));
        when(producer.send(lteDown)).thenReturn(Mono.just(true));

        PollingScheduler.PollCycleStats stats = scheduler.runCycle(List.of(router, lte));

        assertThat(stats).isEqualTo(new PollingScheduler.PollCycleStats(2, 1));
        verify(producer).send(lteDown);
    }

    @Test
    void skipsCycleWhenRegistryIsEmpty() {
        when(registry.devices()).thenReturn(List.of());

        scheduler.pollAll();

        verify(poller, never()).poll(any());
    }
}
