package com.nms.poller.polling;

import com.nms.poller.emulator.EmulatorClient;
import com.nms.poller.emulator.MetricSample;
import com.nms.poller.kafka.MetricEvent;
import com.nms.poller.registry.Device;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DevicePollerTest {

    private static final Instant NOW = Instant.parse("2026-10-27T10:00:00Z");

    private final EmulatorClient emulatorClient = mock(EmulatorClient.class);
    private final DevicePoller poller = new DevicePoller(emulatorClient, Clock.fixed(NOW, ZoneOffset.UTC));
    private final Device device = new Device("router-1", "Router 1", "ACCESS_ROUTER", true);

    @Test
    void mapsSamplesToSuccessEvents() {
        Instant sampleTime = Instant.parse("2026-10-27T09:59:59Z");
        when(emulatorClient.fetchMetrics("router-1")).thenReturn(Mono.just(List.of(
                new MetricSample("cpu_load", 55.0, sampleTime),
                new MetricSample("uptime", 1234.0, null),
                new MetricSample(null, 1.0, sampleTime),
                new MetricSample("temperature", null, sampleTime))));

        StepVerifier.create(poller.poll(device))
                .assertNext(events -> assertThat(events).containsExactly(
                        MetricEvent.success("router-1", "cpu_load", 55.0, sampleTime),
                        MetricEvent.success("router-1", "uptime", 1234.0, NOW)))
                .verifyComplete();
    }

    @Test
    void emitsTimeoutEventWhenDeviceDoesNotRespond() {
        when(emulatorClient.fetchMetrics("router-1")).thenReturn(Mono.error(new TimeoutException()));

        StepVerifier.create(poller.poll(device))
                .assertNext(events -> assertThat(events).containsExactly(MetricEvent.timeout("router-1", NOW)))
                .verifyComplete();
    }
}
