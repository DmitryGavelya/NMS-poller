package com.nms.poller.polling;

import com.nms.poller.emulator.EmulatorClient;
import com.nms.poller.emulator.MetricSample;
import com.nms.poller.kafka.MetricEvent;
import com.nms.poller.registry.Device;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Clock;
import java.time.Instant;
import java.util.List;

/**
 * Опрос одного устройства и преобразование ответа в события метрик.
 * Любая ошибка опроса превращается в событие {@code TIMEOUT} — poller никогда не «теряет»
 * опрос молча, иначе backend не сможет обнаружить недоступность устройства.
 */
@Component
public class DevicePoller {

    private static final Logger log = LoggerFactory.getLogger(DevicePoller.class);

    private final EmulatorClient emulatorClient;
    private final Clock clock;

    public DevicePoller(EmulatorClient emulatorClient, Clock clock) {
        this.emulatorClient = emulatorClient;
        this.clock = clock;
    }

    public Mono<List<MetricEvent>> poll(Device device) {
        return Mono.defer(() -> {
            Instant polledAt = clock.instant();
            return emulatorClient.fetchMetrics(device.id())
                    .map(samples -> toEvents(device, samples, polledAt))
                    .onErrorResume(e -> {
                        log.warn("Device {} did not respond: {}", device.id(), e.toString());
                        return Mono.just(List.of(MetricEvent.timeout(device.id(), polledAt)));
                    });
        });
    }

    private static List<MetricEvent> toEvents(Device device, List<MetricSample> samples, Instant polledAt) {
        return samples.stream()
                .filter(s -> {
                    boolean valid = s.metric() != null && !s.metric().isBlank() && s.value() != null;
                    if (!valid) {
                        log.warn("Skipping invalid sample from device {}: {}", device.id(), s);
                    }
                    return valid;
                })
                .map(s -> MetricEvent.success(device.id(), s.metric(), s.value(),
                        s.timestamp() != null ? s.timestamp() : polledAt))
                .toList();
    }
}
