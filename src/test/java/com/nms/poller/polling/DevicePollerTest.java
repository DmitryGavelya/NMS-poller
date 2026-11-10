package com.nms.poller.polling;

import com.nms.poller.emulator.BooleanSample;
import com.nms.poller.emulator.DoubleSample;
import com.nms.poller.emulator.EmulatorClient;
import com.nms.poller.emulator.StringSample;
import com.nms.poller.kafka.BooleanMetricEvent;
import com.nms.poller.kafka.DeviceTimeoutEvent;
import com.nms.poller.kafka.DoubleMetricEvent;
import com.nms.poller.kafka.StringMetricEvent;
import com.nms.poller.registry.Device;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DevicePollerTest {

    private static final Instant NOW = Instant.parse("2026-10-27T10:00:00Z");

    private final EmulatorClient emulatorClient = mock(EmulatorClient.class);
    private final DevicePoller poller = new DevicePoller(emulatorClient, Clock.fixed(NOW, ZoneOffset.UTC));
    private final Device device = new Device(1L, "Router 1", "ACCESS_ROUTER", true);
    private final PollRequest discovery = new PollRequest(device, Set.of());

    @Test
    void mapsEachSampleTypeToItsEventType() {
        Instant sampleTime = Instant.parse("2026-10-27T09:59:59Z");
        when(emulatorClient.fetchMetrics(1L, Set.of())).thenReturn(Mono.just(List.of(
                new DoubleSample("cpu_load", 55.0, sampleTime),
                new BooleanSample("door_open", true, null),
                new StringSample("firmware", "1.2.3", sampleTime))));

        StepVerifier.create(poller.poll(discovery))
                .assertNext(result -> {
                    assertThat(result.success()).isTrue();
                    assertThat(result.events()).containsExactly(
                            DoubleMetricEvent.success(1L, "cpu_load", 55.0, sampleTime),
                            BooleanMetricEvent.success(1L, "door_open", true, NOW),
                            StringMetricEvent.success(1L, "firmware", "1.2.3", sampleTime));
                    assertThat(result.metricNames()).containsExactlyInAnyOrder("cpu_load", "door_open", "firmware");
                })
                .verifyComplete();
    }

    @Test
    void skipsInvalidSamples() {
        when(emulatorClient.fetchMetrics(1L, Set.of())).thenReturn(Mono.just(List.of(
                new DoubleSample(null, 1.0, null),
                new DoubleSample(" ", 1.0, null),
                new DoubleSample("temperature", null, null),
                new BooleanSample("door_open", null, null),
                new StringSample("firmware", null, null),
                new DoubleSample("cpu_load", 1.0, null))));

        StepVerifier.create(poller.poll(discovery))
                .assertNext(result -> assertThat(result.events())
                        .containsExactly(DoubleMetricEvent.success(1L, "cpu_load", 1.0, NOW)))
                .verifyComplete();
    }

    @Test
    void requestsOnlyTheDueMetrics() {
        Set<String> due = Set.of("door_open");
        when(emulatorClient.fetchMetrics(1L, due))
                .thenReturn(Mono.just(List.of(new BooleanSample("door_open", false, NOW))));

        StepVerifier.create(poller.poll(new PollRequest(device, due)))
                .assertNext(result -> assertThat(result.events())
                        .containsExactly(BooleanMetricEvent.success(1L, "door_open", false, NOW)))
                .verifyComplete();
    }

    @Test
    void emitsTimeoutEventWhenDeviceDoesNotRespond() {
        when(emulatorClient.fetchMetrics(1L, Set.of())).thenReturn(Mono.error(new TimeoutException()));

        StepVerifier.create(poller.poll(discovery))
                .assertNext(result -> {
                    assertThat(result.success()).isFalse();
                    assertThat(result.events()).containsExactly(DeviceTimeoutEvent.of(1L, NOW));
                    assertThat(result.metricNames()).isEmpty();
                })
                .verifyComplete();
    }
}
