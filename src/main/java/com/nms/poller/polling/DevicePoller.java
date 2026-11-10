package com.nms.poller.polling;

import com.nms.poller.emulator.BooleanSample;
import com.nms.poller.emulator.DoubleSample;
import com.nms.poller.emulator.EmulatorClient;
import com.nms.poller.emulator.MetricSample;
import com.nms.poller.emulator.StringSample;
import com.nms.poller.kafka.BooleanMetricEvent;
import com.nms.poller.kafka.DeviceTimeoutEvent;
import com.nms.poller.kafka.DoubleMetricEvent;
import com.nms.poller.kafka.PollEvent;
import com.nms.poller.kafka.StringMetricEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Clock;
import java.time.Instant;
import java.util.List;

@Component
public class DevicePoller {

    private static final Logger log = LoggerFactory.getLogger(DevicePoller.class);

    private final EmulatorClient emulatorClient;
    private final Clock clock;

    public DevicePoller(EmulatorClient emulatorClient, Clock clock) {
        this.emulatorClient = emulatorClient;
        this.clock = clock;
    }

    public Mono<PollResult> poll(PollRequest request) {
        return Mono.defer(() -> {
            Instant polledAt = clock.instant();
            String deviceId = request.device().id();
            return emulatorClient.fetchMetrics(deviceId, request.metrics())
                    .map(samples -> new PollResult(request, toEvents(deviceId, samples, polledAt), true))
                    .onErrorResume(e -> {
                        log.warn("Device {} did not respond: {}", deviceId, e.toString());
                        return Mono.just(new PollResult(request,
                                List.of(DeviceTimeoutEvent.of(deviceId, polledAt)), false));
                    });
        });
    }

    private static List<PollEvent> toEvents(String deviceId, List<MetricSample> samples, Instant polledAt) {
        return samples.stream()
                .filter(s -> {
                    boolean valid = s.metric() != null && !s.metric().isBlank() && s.value() != null;
                    if (!valid) {
                        log.warn("Skipping invalid sample from device {}: {}", deviceId, s);
                    }
                    return valid;
                })
                .<PollEvent>map(s -> toEvent(deviceId, s, s.timestamp() != null ? s.timestamp() : polledAt))
                .toList();
    }

    private static PollEvent toEvent(String deviceId, MetricSample sample, Instant timestamp) {
        return switch (sample) {
            case BooleanSample s -> BooleanMetricEvent.success(deviceId, s.metric(), s.value(), timestamp);
            case DoubleSample s -> DoubleMetricEvent.success(deviceId, s.metric(), s.value(), timestamp);
            case StringSample s -> StringMetricEvent.success(deviceId, s.metric(), s.value(), timestamp);
        };
    }
}
