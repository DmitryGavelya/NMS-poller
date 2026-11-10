package com.nms.poller.polling;

import com.nms.poller.TestProps;
import com.nms.poller.kafka.BooleanMetricEvent;
import com.nms.poller.kafka.DeviceTimeoutEvent;
import com.nms.poller.kafka.DoubleMetricEvent;
import com.nms.poller.kafka.MetricProducer;
import com.nms.poller.registry.Device;
import com.nms.poller.registry.DeviceRegistry;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    private final PollQueue queue = new PollQueue(TestProps.props(Map.of()));
    private final PollingScheduler scheduler = new PollingScheduler(registry, queue, poller, producer,
            TestProps.props(Map.of()), Clock.fixed(NOW, ZoneOffset.UTC));

    private final Device router = new Device(1L, null, null, null);
    private final Device lte = new Device(2L, null, null, null);

    @Test
    void publishesEventsOfAllRequestsAndCountsFailures() {
        var cpu = DoubleMetricEvent.success(1L, "cpu_load", 40.0, NOW);
        var door = BooleanMetricEvent.success(1L, "door_open", true, NOW);
        var lteDown = DeviceTimeoutEvent.of(2L, NOW);
        var routerReq = new PollRequest(router, Set.of("cpu_load", "door_open"));
        var lteReq = new PollRequest(lte, Set.of("cpu_load"));
        when(poller.poll(routerReq)).thenReturn(Mono.just(new PollResult(routerReq, List.of(cpu, door), true)));
        when(poller.poll(lteReq)).thenReturn(Mono.just(new PollResult(lteReq, List.of(lteDown), false)));
        when(producer.send(cpu)).thenReturn(Mono.just(true));
        when(producer.send(door)).thenReturn(Mono.just(false));
        when(producer.send(lteDown)).thenReturn(Mono.just(true));

        var stats = scheduler.runCycle(List.of(routerReq, lteReq));

        assertThat(stats).isEqualTo(new PollingScheduler.PollCycleStats(2, 1));
        verify(producer).send(lteDown);
    }

    @Test
    void discoveryRegistersMetricsInQueueAndNextTickPollsOnlyDueOnes() {
        when(registry.devices()).thenReturn(List.of(router));
        var discovery = new PollRequest(router, Set.of());
        var cpu = DoubleMetricEvent.success(1L, "cpu_load", 40.0, NOW);
        when(poller.poll(discovery)).thenReturn(Mono.just(new PollResult(discovery, List.of(cpu), true)));
        when(producer.send(cpu)).thenReturn(Mono.just(true));

        scheduler.tick();

        assertThat(queue.size()).isEqualTo(1);
        assertThat(queue.pollDue(NOW)).isEmpty();
        assertThat(queue.pollDue(NOW.plusSeconds(30))).containsExactly(new PollRequest(router, Set.of("cpu_load")));
    }

    @Test
    void failedDiscoveryDoesNotRegisterMetrics() {
        when(registry.devices()).thenReturn(List.of(router));
        var discovery = new PollRequest(router, Set.of());
        var timeout = DeviceTimeoutEvent.of(1L, NOW);
        when(poller.poll(discovery)).thenReturn(Mono.just(new PollResult(discovery, List.of(timeout), false)));
        when(producer.send(timeout)).thenReturn(Mono.just(true));

        scheduler.tick();

        assertThat(queue.pollDue(NOW.plusSeconds(30))).containsExactly(discovery);
    }

    @Test
    void doesNothingWhenNoDevices() {
        when(registry.devices()).thenReturn(List.of());

        scheduler.tick();

        verify(poller, never()).poll(any());
    }
}
